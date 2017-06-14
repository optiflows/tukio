import asyncio
import inspect
import logging
import functools
from copy import copy
from enum import Enum
from uuid import uuid4
from datetime import datetime, timezone

from tukio.dag import DAG
from tukio.utils import FutureState, Listen, SkipTask
from tukio.broker import get_broker, workflow_exec_topics
from tukio.event import Event, EventSource
from tukio.task import (
    TaskTemplate, TaskRegistry, UnknownTaskName, TukioTask, TukioTaskError
)


log = logging.getLogger(__name__)


class WorkflowError(Exception):
    pass


class WorkflowRootTaskError(WorkflowError):

    def __init__(self, value):
        super().__init__()
        self._value = value

    def __str__(self):
        return 'expected one root task, found {}'.format(self._value)


class TemplateGraphError(WorkflowError):

    def __init__(self, key):
        super().__init__()
        self._key = key

    def __str__(self):
        return 'graph error on task id: {}'.format(self._key)


class RescueError(WorkflowError):

    def __init__(self, uid, reason):
        super().__init__()
        self._uid = uid
        self._reason = reason

    def __str__(self):
        return (
            'cannot rescue workflow id {} from its last execution report. '
            'reason: {}'.format(self._uid, self._reason)
        )


class WorkflowExecState(Enum):

    begin = 'workflow-begin'
    end = 'workflow-end'
    error = 'workflow-error'
    progress = 'workflow-progress'
    suspend = 'workflow-suspend'
    resume = 'workflow-resume'

    @classmethod
    def values(cls):
        return [member.value for member in cls]


class OverrunPolicy(Enum):

    """
    The overrun policy defines what to do if the previous execution of a
    workflow didn't finish yet and a new event must be dispatched by the
    workflow engine.
    This class also defines policy handlers that can create new instances
    of workflow execution objects according to the current overrun policy and
    to the list of running workflow instances (with the same template).
    """

    # Skip until all running instances are finished
    skip = 'skip'
    # Start a new workflow instance whatever the running instances
    start_new = 'start-new'
    # Skip until all running instances had been unlocked
    skip_until_unlock = 'skip-until-unlock'
    # Abort all running instances before creating a new once
    abort_running = 'abort-running'

    @classmethod
    def get_default_policy(cls):
        """
        Returns the default overrun policy.
        """
        return cls('skip-until-unlock')

    @classmethod
    def get(cls, policy=None):
        if policy is None:
            return cls.get_default_policy()
        if isinstance(policy, cls):
            return policy
        return cls(policy)


class OverrunPolicyHandler:

    """
    This class defines overrun policy handlers to create new instances
    of workflow execution objects according to the current overrun policy and
    to the list of running workflow instances (with the same template).
    """

    __slots__ = ('_loop', 'template', 'policy')

    def __init__(self, template, loop=None):
        self._loop = loop
        # Store the workflow template for further use in policy handlers.
        self.template = template
        self.policy = template.policy

    def new_workflow(self, running=None):
        method = getattr(self, '_' + self.policy.name)
        return method(running)

    def _check_wflow(self, wflow):
        if wflow.template.uid != self.template.uid:
            err = 'expected template ID {}'', got {}'
            raise ValueError(err.format(self.template.uid, wflow.template.uid))

    def _new_wflow(self):
        return Workflow(self.template, loop=self._loop)

    def _skip(self, running):
        """
        Run a new instance of workflow only if there's no instance already
        running with the same template ID.
        """
        if running:
            for wflow in running:
                self._check_wflow(wflow)
            return None
        else:
            return self._new_wflow()

    def _start_new(self, _):
        """
        Always run a new instance of workflow.
        """
        return self._new_wflow()

    def _skip_until_unlock(self, running):
        """
        Run a new instance of workflow only if all the instances already
        running have been unlocked. Refer to the `Workflow` docstring for more
        details about locked/unlocked workflows.
        """
        if running:
            for wflow in running:
                self._check_wflow(wflow)
                if wflow.lock.locked():
                    break
            else:
                return self._new_wflow()
            # There's at least 1 locked workflow instance
            return None
        else:
            return self._new_wflow()

    def _abort_running(self, running):
        """
        Abort all running instances of the workflow before creating a new one.
        """
        if running:
            for wflow in running:
                self._check_wflow(wflow)
                wflow.cancel()
        return self._new_wflow()


class WorkflowTemplate:

    """
    A workflow template is a DAG (Directed Acyclic Graph) made up of task
    template objects (`TaskTemplate`). This class is not a workflow execution
    engine.
    It provides an API to easily build and update a consistent workflow.
    """

    __slots__ = ('uid', 'topics', 'policy', 'dag')

    def __init__(self, uid=None, policy=None, topics=None):
        self.uid = uid or str(uuid4())
        self.topics = topics
        self.policy = OverrunPolicy.get(policy)
        self.dag = DAG()

    @property
    def tasks(self):
        return list(self.dag.graph.keys())

    @property
    def listen(self):
        return Listen.get(self.topics)

    def add(self, task_tmpl):
        """
        Adds a new task template to the workflow. The task will remain orphan
        until it is linked to upstream/downstream tasks.
        This method must be passed a `TaskTemplate()` instance.
        """
        if not isinstance(task_tmpl, TaskTemplate):
            raise TypeError("expected a 'TaskTemplate' instance")
        self.dag.add_node(task_tmpl)

    def delete(self, task_tmpl):
        """
        Remove a task template from the workflow and delete the links to
        upstream/downstream tasks.
        """
        self.dag.delete_node(task_tmpl)

    def root(self):
        """
        Returns the root task. If no root task or several root tasks were found
        raises `WorkflowValidationError`.
        """
        root_task = self.dag.root_nodes()
        nb = len(root_task)
        if nb == 1:
            return root_task[0]
        raise WorkflowRootTaskError(nb)

    def link(self, up_task_tmpl, down_task_tmpl):
        """
        Create a directed link from an upstream to a downstream task.
        """
        self.dag.add_edge(up_task_tmpl, down_task_tmpl)

    def unlink(self, task_tmpl1, task_tmpl2):
        """
        Remove the link between two tasks.
        """
        try:
            self.dag.delete_edge(task_tmpl1, task_tmpl2)
        except KeyError:
            self.dag.delete_edge(task_tmpl2, task_tmpl1)

    @classmethod
    def from_dict(cls, wf_dict):
        """
        Build a new workflow description from the given dictionary.
        The dictionary takes the form of:
            {
                "id": <workflow-uid>,
                "topics": [<a-topic>, <another-topic>],
                "policy": <policy>,
                "tasks": [
                    {"id": <task-uid>, "name": <name>, "config": <cfg-dict>},
                    ...
                ],
                "graph": {
                    <t1-uid>: [t2-uid, <t3-uid>],
                    <t2-uid>: [],
                    ...
                }
            }

        See below the conditions applied to trigger a workflow according to the
        value of 'topics':
            {"topics": None}
            try to trigger a workflow each time data is received by the engine
            ** default behavior **

            {"topics": []}
            never try to trigger a workflow when data is received by the engine

            {"topics": ["blob", "foo"]}
            try to trigger a workflow when data is received by the engine in
            topics "blob" and "foo" only
        """
        wf_tmpl = cls(
            uid=wf_dict.get('id'),
            policy=wf_dict.get('policy'),
            topics=wf_dict.get('topics')
        )

        # Tasks
        task_ids = dict()
        for task_dict in wf_dict.get('tasks', []):
            task_tmpl = TaskTemplate.from_dict(task_dict)
            wf_tmpl.add(task_tmpl)
            task_ids[task_tmpl.uid] = task_tmpl

        # Graph
        try:
            for up_id, down_ids_set in wf_dict.get('graph', {}).items():
                up_tmpl = task_ids[up_id]
                for down_id in down_ids_set:
                    down_tmpl = task_ids[down_id]
                    wf_tmpl.link(up_tmpl, down_tmpl)
        except KeyError as exc:
            raise TemplateGraphError(exc.args[0]) from exc

        return wf_tmpl

    def as_dict(self):
        """
        Builds and returns a dictionary that represents the current workflow
        template object.
        """
        wf_dict = {
            'id': self.uid,
            'policy': self.policy.value,
            'topics': self.topics,
            'tasks': [],
            'graph': {}
        }
        for task_tmpl in self.tasks:
            wf_dict['tasks'].append(task_tmpl.as_dict())
        for up_tmpl, down_tmpls in self.dag.graph.items():
            entry = {up_tmpl.uid: list(map(lambda x: x.uid, down_tmpls))}
            wf_dict['graph'].update(entry)
        return wf_dict

    def copy(self):
        """
        Returns a copy of the current instance of workflow template.
        """
        wf_tmpl = WorkflowTemplate(uid=self.uid)
        wf_tmpl.dag = self.dag.copy()
        return wf_tmpl

    def validate(self):
        """
        Validate the current workflow template. At that point, we already know
        the underlying DAG is valid. This method ensures there's a single root
        task and all task names are registered tasks.
        If not valid, this method should raise either `WorkflowRootTaskError`
        or `UnknownTaskName` exceptions.
        """
        root_nodes = len(self.dag.root_nodes())
        if root_nodes != 1:
            raise WorkflowRootTaskError(root_nodes)
        for task in self.tasks:
            TaskRegistry.get(task.name)
        return True

    def __str__(self):
        """
        Human readable string representation of a workflow template.
        """
        return '<WorkflowTemplate uid={}>'.format(self.uid)


def _current_workflow(func):
    """
    A decorator to maintain the dictionary of currently running workflows.
    Inspired from the implementation of `asyncio.Task`.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        self.__class__._current_workflows[self._loop] = self
        try:
            return func(self, *args, **kwargs)
        finally:
            self.__class__._current_workflows.pop(self._loop)
    return wrapper


def _get_workflow_from_task(task):
    """
    Looks for an instance of `Workflow` linked to the task or a method of
    `Workflow` among the done callbacks of the asyncio task. Returns None if
    not found.
    If the task was triggered from within a workflow it MUST have a `workflow`
    attribute that points to it and at least one done callback that is a method
    of `Workflow`.
    """
    if isinstance(task, TukioTask):
        workflow = task.workflow
    if workflow:
        return workflow

    for cb in task._callbacks:
        # inspect.getcallargs() gives access to the implicit 'self' arg of
        # the bound method but it is marked as deprecated since
        # Python 3.5.1 and the new `inspect.Signature` object does NOT do
        # the job :((
        if inspect.ismethod(cb):
            inst = cb.__self__
        elif isinstance(cb, functools.partial):
            try:
                inst = cb.func.__self__
            except AttributeError:
                continue
        else:
            continue
        if isinstance(inst, Workflow):
            return inst
    return None


class Workflow(asyncio.Future):

    """
    This class handles the execution of a workflow. Tasks are created along the
    way of workflow execution.
    """

    # Inspired from the implementation of `asyncio.Task`
    _current_workflows = {}

    __slots__ = (
        'uid', '_template', '_start', '_end', 'tasks', '_tasks_by_id',
        '_updated_next_tasks', '_done_tasks', '_internal_exc', '_must_cancel',
        'lock', '_broker', '_source', '_committed',
    )

    @classmethod
    def current_workflow(cls, loop=None):
        """
        Returns the currently running workflow in an event loop or None.
        By default the current workflow for the current event loop is returned.
        None is returned when called not in the context of a Workflow.
        """
        loop = loop or asyncio.get_event_loop()
        task = asyncio.Task.current_task(loop)
        workflow = None
        if task:
            workflow = _get_workflow_from_task(task)
        if not workflow:
            workflow = cls._current_workflows.get(loop)
        return workflow

    def __init__(self, wf_tmpl, *, loop=None, broker=None):
        super().__init__(loop=loop)
        self.uid = str(uuid4())
        self._template = wf_tmpl
        # Start and end datetime (UTC) of the execution of the workflow
        self._start, self._end = None, None
        # Set of tasks executed at some point. Items of that set are
        # instances of `asyncio.Task`
        self.tasks = set()
        self._tasks_by_id = dict()
        # This dict references all tasks that updated the set of their
        # downstream tasks at runtime. Keys are `asyncio.Task` objects and
        # values are sets of task template IDs.
        self._updated_next_tasks = dict()
        self._done_tasks = set()
        self._internal_exc = None
        self._must_cancel = False
        self.lock = asyncio.Lock()
        # Create the workflow in the 'locked' state when its overrun policy is
        # 'skip-until-unlock'.
        if self.policy is OverrunPolicy.skip_until_unlock:
            self.lock._locked = True
        # Work with an event broker
        self._broker = broker or get_broker(self._loop)
        self._source = EventSource(
            workflow_template_id=self._template.uid,
            workflow_exec_id=self.uid
        )
        # A 'committed' workflow is a pending workflow not suspended
        self._committed = asyncio.Event()

    @property
    def template(self):
        return self._template

    @property
    def policy(self):
        return self._template.policy

    @property
    def committed(self):
        return self._committed.is_set()

    @_current_workflow
    def _unlock(self, _):
        """
        The concept of 'locked workflow' only applies when the overrun policy
        is set to 'skip-until-unlock'. Once the workflow is unlocked, a new
        execution from the same template ID can be triggered.
        """
        if self.lock.locked():
            self.lock.release()

    def unlock_when_task_done(self, task=None):
        """
        Adds a done callback to the passed or current task so as to unlock the
        workflow when the task gets done.
        """
        task = task or asyncio.Task.current_task()
        task.add_done_callback(self._unlock)

    def _register_to_broker(self, task_tmpl, task):
        """
        Registers the `data_received` callback of the task (if defined) into
        the event broker to handle new data received during task execution.
        """
        listen = task_tmpl.listen
        # Task is configured to receive no data during execution
        if listen is Listen.nothing:
            return

        # A tukio task always has a 'data_received' method
        try:
            callback = task.data_received
        except AttributeError as exc:
            task.set_exception(exc)
            raise

        # Register the callback in the event broker
        if listen is Listen.everything:
            self._broker.register(callback)
        else:
            for topic in task_tmpl.topics:
                self._broker.register(callback, topic=topic)

        # Unregister this callback as soon as the task will be done.
        done_cb = functools.partial(self._unregister_from_broker, callback,
                                    topics=task_tmpl.topics)
        task.add_done_callback(done_cb)

    @_current_workflow
    def _unregister_from_broker(self, callback, _, topics=None):
        """
        A very simple wrapper around `Broker.unregister()` to ignore the future
        object passed as argument by asyncio to all done callbacks.
        """
        if topics is None:
            itertopics = [None]
        else:
            itertopics = topics
        for topic in itertopics:
            try:
                self._broker.unregister(callback, topic=topic)
            except KeyError as exc:
                log.error('failed to unregister callback: %s', exc)
                self._internal_exc = exc
        if self._internal_exc:
            self._cancel_all_tasks()

    @_current_workflow
    def run(self, event):
        """
        Execute the workflow following the description passed at init.
        """
        # A workflow can be ran only once
        if self.tasks:
            raise RuntimeError('a workflow can be run only once!')

        # Run the root task
        self._committed.set()
        try:
            root_tmpl = self._template.root()
        except WorkflowRootTaskError as exc:
            self._internal_exc = exc
            self._try_mark_done()
            task = None
        else:
            # Automatically wrap input data into an event object
            self._dispatch_exec_event(WorkflowExecState.begin, copy(event))
            task = self._new_task(root_tmpl, event)
            self._start = datetime.now(timezone.utc)
            # The workflow may fail to start at once
            if not task:
                self._try_mark_done()
        return task

    def _new_task(self, task_tmpl, event):
        """
        Each new task must be created successfully, else the whole workflow
        shall stop running (wrong workflow config or bug).
        It is assumed all tasks are tukio tasks (aka `TukioTask` objects).
        """
        try:
            task = task_tmpl.new_task(event, loop=self._loop)
            # Register the `data_received` callback (if required) as soon as
            # the execution of the task is scheduled.
            self._register_to_broker(task_tmpl, task)
        except (UnknownTaskName, TypeError, ValueError, AttributeError) as exc:
            log.error('failed to create task from template: %s', task_tmpl)
            self._internal_exc = exc
            self._cancel_all_tasks()
            return None
        task._workflow = self
        log.debug('new task created for %s', task_tmpl)

        def next_tasks(future):
            asyncio.ensure_future(self._run_next_tasks(future))

        task.add_done_callback(next_tasks)
        self.tasks.add(task)
        # Create the exec dict of the task
        self._tasks_by_id[task_tmpl.uid] = task
        return task

    def _join_task(self, next_task, event):
        """
        Pass an event to a downstream task that has already been started (by
        another parent). In such a situation, it is known to be a join task.
        """
        # Push event into next_task's event queue
        asyncio.ensure_future(next_task.data_received(event))

    def _get_next_task_templates(self, task_tmpl, task):
        """
        Retrieves the downstream tasks of `task_tmpl` and filters it with the
        template IDs that (may) have been provided at runtime by `task`.
        """
        succ_tmpls = self._template.dag.successors(task_tmpl)
        try:
            tmpl_ids = self._updated_next_tasks[task]
        except KeyError:
            return succ_tmpls
        filtered_tmpls = []
        for tid in tmpl_ids:
            for tmpl in succ_tmpls:
                if tmpl.uid == tid:
                    filtered_tmpls.append(tmpl)
                    break
            else:
                # This is a misconfiguration from the task. Ignore it to
                # leave the opportunity to execute the other next tasks.
                log.error('ID %s not in downstream tasks of %s', tid, task)
        log.debug('%s filtered next tasks to: %s', task, filtered_tmpls)
        return filtered_tmpls

    def _dispatch_exec_event(self, etype, data=None):
        """
        A shorthand to dispatch information about the workflow execution along
        the way.
        """
        if isinstance(data, Event):
            data = data.data
        self._broker.dispatch(
            {'type': etype.value, 'content': data},
            topics=workflow_exec_topics(self.uid),
            source=self._source,
        )

    @_current_workflow
    async def _run_next_tasks(self, task):
        """
        A callback to be added to each task in order to select and schedule
        asynchronously downstream tasks once the parent task is done.
        """
        self._done_tasks.add(task)
        if self._must_cancel:
            self._try_mark_done()
            return

        # Don't execute downstream tasks if the task's result is an exception
        # (may include task cancellation) but don't stop executing the other
        # branches of the workflow.
        try:
            result = task.result()
        except SkipTask:
            # If the task has been skipped, we just forward the previous task
            # inputs to the next one.
            result = task.inputs
            log.warning('Task %s has been skipped', task.template)
        except asyncio.CancelledError:
            log.warning('Task %s has been cancelled', task.template)
            self._try_mark_done()
            return
        except TukioTaskError as exc:
            log.warning(
                'Task %s ended on error: %s', task.template, exc.task_outputs
            )
            self._try_mark_done()
            return
        except Exception as exc:
            log.warning('Task %s ended on exception', task.template)
            log.exception(exc)
            self._try_mark_done()
            return

        # Ensure the workflow is not suspended, else wait for a resume.
        try:
            await self._committed.wait()
        except asyncio.CancelledError:
            # If cancelled, the workflow will return with the statement below.
            log.debug('Suspended workflow cancelled')
            return

        source = EventSource(
            workflow_template_id=self.template.uid,
            workflow_exec_id=self.uid,
            task_template_id=task.template.uid,
            task_exec_id=task.uid
        )
        # Go through each child task
        for tmpl in self._get_next_task_templates(task.template, task):
            # Wrap result from parent task into an event object
            event = Event(result, source=source)
            next_task = self._tasks_by_id.get(tmpl.uid)
            if next_task:
                # Ignore done tasks
                if next_task.done():
                    continue
                # Downstream task already running, join it!
                self._join_task(next_task, event)
            # Create new task
            else:
                next_task = self._new_task(tmpl, event)
            if not next_task:
                break

        self._try_mark_done()

    def _try_mark_done(self):
        """
        If nothing left to execute, the workflow must be marked as done. The
        result is set to either the exec graph (represented by a dict) or to an
        exception raised at task creation.
        """
        if not self._committed.is_set():
            # Won't mark a workflow 'done' if it's suspended
            return

        # Note: the result of the workflow may already have been set by another
        # done callback from another task executed in the same iteration of the
        # event loop.
        if self._all_tasks_done() and not self.done():
            exec_event = WorkflowExecState.end
            if self._internal_exc:
                self.set_exception(self._internal_exc)
                exec_event = WorkflowExecState.error
                data = self._internal_exc
            elif self._must_cancel:
                super().cancel()
                data = {'cancel': True}
            else:
                self.set_result(self.tasks)
                data = None
            self._end = datetime.now(timezone.utc)
            self._dispatch_exec_event(exec_event, data=data)

    def _all_tasks_done(self):
        """
        Returns True if all tasks are done, else returns False.
        Here, a task is considered as 'done' only if it is marked as done and
        its `_run_next_tasks()` done callback has been called.
        """
        if self.tasks == self._done_tasks:
            return True
        else:
            return False

    def _cancel_all_tasks(self):
        """
        Cancels all pending tasks and returns the number of tasks cancelled.
        """
        self._must_cancel = True
        cancelled = 0
        pending = self.tasks - self._done_tasks
        for task in pending:
            is_cancelled = task.cancel()
            if is_cancelled:
                cancelled += 1
                try:
                    asyncio.ensure_future(task.holder.teardown())
                except AttributeError:
                    pass
        return cancelled

    def set_next_tasks(self, task_tmpl_ids):
        """
        By default the workflow runs all downstream tasks once the current task
        is done. This method allows to select the tasks that will be actually
        ran and disables other tasks (unless they're already running).

        This method is intended to be called at runtime by the task itself.
        `task_tmpl_ids` must be a list (can be empty) of task template IDs.
        """
        task = asyncio.Task.current_task(self._loop)
        if task not in self.tasks:
            raise RuntimeError('task {} not executed by {}'.format(task, self))
        self._updated_next_tasks[task] = task_tmpl_ids

    def suspend(self):
        """
        Suspend the execution of this workflow prevents any new task from
        starting.
        """
        if not self._committed.is_set():
            return
        self._committed.clear()

        # Suspend a task means the task will be cancelled with the state
        # 'suspended', which can be used later to resume the workflow
        for task in (self.tasks - self._done_tasks):
            task.suspend()

        self._dispatch_exec_event(WorkflowExecState.suspend)
        log.info('workflow %s has been suspended', self)

    def resume(self):
        """
        Resume the execution of this workflow allows new tasks to start.
        """
        if self._committed.is_set():
            log.error('Cannot resume a workflow that has not been suspended')
            return
        self._committed.set()

        # Next tasks are done waiting for '_committed' asyncio event
        # The 'suspended' ones needs to be re-executed
        for task in self._done_tasks:
            if FutureState.get(task) is not FutureState.suspended:
                continue
            event = Event(task.inputs, source=task.event_source)
            self._new_task(task.template, event)

        self._dispatch_exec_event(WorkflowExecState.resume)
        log.info('workflow %s has been resumed', self)

    def cancel(self):
        """
        Cancel the workflow by cancelling all pending tasks (aka all tasks not
        marked as done). We must wait for all tasks to be actually done before
        marking the workflow as cancelled (hence done).
        """
        if not self._committed.is_set():
            self._committed.set()
            self._try_mark_done()

        cancelled = self._cancel_all_tasks()
        if cancelled == 0:
            # Tasks where already all done.If not,
            # the last task will end the workflow already set as _must_cancel
            super().cancel()
        return True

    def __str__(self):
        """
        Readable string representation of a workflow execution object.
        """
        string = '<Workflow template.uid={}, uid={}, start={}, end={}>'
        return string.format(self._template.uid, self.uid,
                             self._start, self._end)

    def report(self):
        """
        Creates and returns a complete execution report, including workflow and
        tasks templates and execution details.
        """
        report = self._template.as_dict()
        report['exec'] = {
            'id': self.uid,
            'start': self._start,
            'end': self._end,
            'state': FutureState.get(self).value
        }
        # Update task descriptions to add info about their execution.
        for task_dict in report['tasks']:
            try:
                task = self._tasks_by_id[task_dict['id']]
            except KeyError:
                task_dict['exec'] = None
                continue
            task_dict['exec'] = task.as_dict()
            # If the task is linked to a task holder, try to use its own report
            if hasattr(task.holder, 'report'):
                task_dict['exec']['reporting'] = task.holder.report()
        return report

    def fast_forward(self, report):
        """
        Restore a workflow and its tasks states, parsing an execution report.
        """
        self.uid = report['exec']['id']
        self._start = report['exec']['start']
        self._end = report['exec'].get('end')
        self._source = EventSource(
            workflow_template_id=self._template.uid,
            workflow_exec_id=self.uid
        )
        log.info('fast forward workflow %s', self)

        # Build a list of task in the graph that need to be executed
        resume = list()

        def browse(entry, parent=None):
            """
            Dive into the dag to:
                - ensure executed task contexts are restored.
                - find tasks that need to be executed.
            """
            t_template = next(
                t for t in report['tasks'] if t['id'] == entry.uid
            )
            t_report = t_template.get('exec')
            if not t_report:
                if not parent:
                    raise RescueError(self.uid, 'root task never been started')
                # No execution report found, the task needs to be executed
                resume.append((entry, None, parent))
                return

            t_state = FutureState(t_report['state'])
            if not t_state.done():
                # Pending, suspended or cancelled tasks need to be executed
                resume.append((entry, t_template, parent))
                return

            if t_state.done():
                # We won't manually create an asyncio task since it won't be
                # run (the task is 'done'). Yet, we need a memory print of its
                # execution (mainly for reporting purposes).
                t_shadow = type('TukioTaskShadow', (object,), {
                    'done': lambda: True,
                    'cancelled': lambda: False,
                    '_exception': None,
                    'as_dict': lambda: t_report
                })

                # Add this task to the tracking sets
                self.tasks.add(t_shadow)
                self._done_tasks.add(t_shadow)
                self._tasks_by_id[entry.uid] = t_shadow

                # Recursive browing
                for t_next in self._template.dag.successors(entry):
                    browse(t_next, t_template)
        browse(self._template.root())

        # Resume/start tasks that need to be executed
        for task_template, task_report, parent_report in resume:
            if parent_report:
                inputs = parent_report['exec']['outputs']
                source = EventSource(
                    workflow_template_id=self.template.uid,
                    workflow_exec_id=self.uid,
                    task_template_id=parent_report['id'],
                    task_exec_id=parent_report['exec']['id']
                )
                event = Event(inputs, source=source)
            else:
                # No parent report = root task
                event = Event(task_report['exec']['inputs'])
            self._new_task(task_template, event)

        self._dispatch_exec_event(WorkflowExecState.begin)


def new_workflow(wf_tmpl, running=None, loop=None):
    """
    Returns a new workflow execution object if a new instance can be run.
    It depends on the template's overrun policy and the list of running
    workflow instances (given by `running`).
    """
    policy_handler = OverrunPolicyHandler(wf_tmpl, loop=loop)
    return policy_handler.new_workflow(running)
