import asyncio
from datetime import datetime
from enum import Enum
import functools
import inspect
import logging
from uuid import uuid4

from tukio.dag import DAG
from tukio.task import TaskTemplate, TaskRegistry, JoinTask
from tukio.utils import future_state, topics_to_listen, Listen
from tukio.broker import get_broker


log = logging.getLogger(__name__)


class WorkflowError(Exception):
    pass


class WorkflowRootTaskError(WorkflowError):
    def __init__(self, value):
        self._value = value

    def __str__(self):
        return 'expected one root task, found {}'.format(self._value)


class WorkflowNotFoundError(WorkflowError):
    def __str__(self):
        return 'no workfow bound to the task, cannot unlock it!'


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

    def __init__(self, template, loop=None):
        self._loop = loop
        # Store the workflow template for further use in policy handlers.
        self.template = template
        self.policy = template.policy

    def new_workflow(self, running=None):
        method = getattr(self, '_' + self.policy.name)
        return method(running)

    def _check_wflow(self, wflow):
        if wflow.template_id != self.template.uid:
            err = 'expected template ID {}'', got {}'
            raise ValueError(err.format(self.template.uid, wflow.template_id))

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

    def __init__(self, title, uid=None, tags=None, version=None, policy=None,
                 draft=False, topics=None):
        self.title = title
        self.tags = tags or []
        self.uid = uid or str(uuid4())
        self.version = int(version) if version is not None else 1
        self.draft = draft
        self.topics = topics
        self.policy = OverrunPolicy.get(policy)
        self.dag = DAG()

    @property
    def tasks(self):
        return list(self.dag.graph.keys())

    @property
    def listen(self):
        return topics_to_listen(self.topics)

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
                "title": <workflow-title>,
                "id": <workflow-uid>,
                "version": <version>,
                "tags": [<a-tag>, <another-tag>],
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

        Only 'title' is mandatory to create a workflow template.
        See below the conditions applied to trigger a workflow according to the
        value of 'topics':
            {"topics": None}
            try to trigger a workflow each time data is received by the engine

            {"topics": []}
            never try to trigger a workflow when data is received by the engine

            {"topics": ["blob", "foo"]}
            try to trigger a workflow when data is received by the engine in
            topics "blob" and "foo" only
        """
        # 'title' is the only mandatory key
        wf_tmpl = cls(
            wf_dict['title'],
            uid=wf_dict.get('id'),
            tags=wf_dict.get('tags'),
            version=wf_dict.get('version'),
            draft=wf_dict.get('draft', False),
            policy=wf_dict.get('policy'),
            topics=wf_dict.get('topics')
        )
        task_ids = dict()
        for task_dict in wf_dict.get('tasks', []):
            task_tmpl = TaskTemplate.from_dict(task_dict)
            wf_tmpl.add(task_tmpl)
            task_ids[task_tmpl.uid] = task_tmpl
        for up_id, down_ids_set in wf_dict.get('graph', {}).items():
            up_tmpl = task_ids[up_id]
            for down_id in down_ids_set:
                down_tmpl = task_ids[down_id]
                wf_tmpl.link(up_tmpl, down_tmpl)
        return wf_tmpl

    def as_dict(self):
        """
        Builds and returns a dictionary that represents the current workflow
        template object.
        """
        wf_dict = {
            "title": self.title,
            "id": self.uid,
            "tags": self.tags,
            "version": int(self.version),
            "draft": self.draft,
            "policy": self.policy.value,
            "topics": self.topics,
            "tasks": [],
            "graph": {}
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
        wf_tmpl = WorkflowTemplate(self.title, uid=self.uid,
                                   tags=self.tags, version=self.version)
        wf_tmpl.dag = self.dag.copy()
        return wf_tmpl

    def validate(self):
        """
        Validate the current workflow template. At that point, we already know
        the underlying DAG is valid. This methods ensures there's a single
        root task and all task names are registered tasks.
        If not valid, this method should raise either `WorkflowRootTaskError`
        or `UnknownTaskName` exceptions.
        """
        self.root()
        for task in self.tasks:
            TaskRegistry.get(task.name)
        return True

    def __str__(self):
        """
        Human readable string representation of a workflow template.
        """
        wfstr = "<WorkflowTemplate title={}, uid={}, version={}>"
        return wfstr.format(self.title, self.uid, self.version)


class Workflow(asyncio.Future):

    """
    This class handles the execution of a workflow. Tasks are created along the
    way of workflow execution.
    """

    def __init__(self, wf_tmpl, *, loop=None, broker=None):
        super().__init__(loop=loop)
        self.uid = str(uuid4())
        self._template = wf_tmpl
        # Start and end datetime (UTC) of the execution of the workflow
        self._start, self._end = None, None
        # Dict of tasks executed at some point. The values of that dict are
        # instances of `asyncio.Task`, and keys are instances of `TaskTemplate`
        self.tasks = set()
        self._tasks_by_id = dict()
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

    @property
    def template_id(self):
        return self._template.uid

    @property
    def policy(self):
        return self._template.policy

    def _unlock(self, _):
        """
        A done callback to unlock the workflow.
        The concept of 'locked workflow' only applies when the overrun policy
        is set to 'skip-until-unlock'. Once a task has unlocked the workflow,
        new execution with the same template can be triggered.
        """
        if self.lock.locked():
            self.lock.release()

    def _register_to_broker(self, task_tmpl, task):
        """
        Registers the `data_received` callback of the task (if defined) into
        the event broker to handle new data received during task execution.
        """
        listen = task_tmpl.listen
        # Task is configured to receive no data during execution
        if listen is Listen.nothing:
            return

        # Since the task is configured to receive data during execution it MUST
        # be possible to register a callback.
        try:
            callback = task.holder.data_received
        except AttributeError as exc:
            log.error("no callback to register in broker (%s)", exc)
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
            except Exception as exc:
                log.error('failed to unregister callback: %s', exc)
                self._internal_exc = exc
        if self._internal_exc:
            self._cancel_all_tasks()

    def run(self, *args, **kwargs):
        """
        Execute the workflow following the description passed at init.
        """
        # A workflow can be ran only once
        if self.tasks:
            return
        # Run the root task
        root_tmpl = self._template.root()
        task = self._next_task(root_tmpl, (args, kwargs))
        self._start = datetime.utcnow()
        # The workflow may fail to start at once
        if not task:
            self._try_mark_done()

    def _call_join_task(self, task_tmpl, task, inputs):
        """
        Plan an update on a join task informing one of it's parents is done.
        """
        log.debug('new join task call for {}'.format(task_tmpl))

        args, kwargs = inputs
        holder = task.holder
        if not holder:
            raise Exception("No holder on task {}".format(task))

        return holder.data_received(*args, from_parent=True, **kwargs)

    def _create_task(self, task_tmpl, inputs):
            task = task_tmpl.new_task(inputs=inputs, loop=self._loop)

            # Register the `data_received` callback (if required) as soon as
            # the execution of the task is scheduled.
            self._register_to_broker(task_tmpl, task)
            log.debug('new task created for %s', task_tmpl)
            done_cb = functools.partial(self._run_next_tasks, task_tmpl)
            task.add_done_callback(done_cb)
            self.tasks.add(task)

            # Create the exec dict of the task
            exec_dict = {'start': datetime.utcnow()}
            try:
                exec_dict['id'] = task.uid
            except AttributeError:
                exec_dict['id'] = None
            self._tasks_by_id[task_tmpl.uid] = (task, exec_dict)
            return task

    def _next_task(self, task_tmpl, inputs):
        """
        Each new task must be created successfully, else the whole workflow
        shall stop running (wrong workflow config or bug).
        """
        task = None
        try:
            klass, _ = TaskRegistry.get(task_tmpl.name)
            if task_tmpl.uid in self._tasks_by_id:
                task, exec_dict = self._tasks_by_id[task_tmpl.uid]
                call = self._call_join_task(task_tmpl, task, inputs)
            else:
                task = self._create_task(task_tmpl, inputs)
        except Exception as exc:
            log.error('failed to create or call task %s: raised %s', task_tmpl, exc)
            self._internal_exc = exc
            self._cancel_all_tasks()
            return None
        return task

    def _run_next_tasks(self, task_tmpl, future):
        """
        A callback to be added to each task in order to select and schedule
        asynchronously downstream tasks once the parent task is done.
        """
        self._done_tasks.add(future)
        if self._must_cancel:
            self._try_mark_done()
            return
        # Don't execute downstream tasks if the task's result is an exception
        # (may include task cancellation) but don't stop executing the other
        # branches of the workflow.
        try:
            result = future.result()
        except Exception as exc:
            log.warning('task %s ended on exception', task_tmpl)
            log.exception(exc)
        else:
            succ_tmpls = self._template.dag.successors(task_tmpl)
            for succ_tmpl in succ_tmpls:
                inputs = ((result,), {})
                succ_task = self._next_task(succ_tmpl, inputs)
                if not succ_task:
                    break
        finally:
            self._try_mark_done()

    def _try_mark_done(self):
        """
        If nothing left to execute, the workflow must be marked as done. The
        result is set to either the exec graph (represented by a dict) or to an
        exception raised at task creation.
        """
        # Note: the result of the workflow may already have been set by another
        # done callback from another task executed in the same iteration of the
        # event loop.
        if self._all_tasks_done() and not self.done():
            if self._internal_exc:
                self.set_exception(self._internal_exc)
            elif self._must_cancel:
                super().cancel()
            else:
                self.set_result(self.tasks)
            self._end = datetime.utcnow()

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
        return cancelled

    def cancel(self):
        """
        Cancel the workflow by cancelling all pending tasks (aka all tasks not
        marked as done). We must wait for all tasks to be actually done before
        marking the workflow as cancelled (hence done).
        """
        cancelled = self._cancel_all_tasks()
        if cancelled == 0:
            super().cancel()
        return True

    def __str__(self):
        """
        Readable string representation of a workflow execution object.
        """
        string = ("<Workflow template_id={}, template_title={}, uid={}, "
                  "start={}, end={}>")
        tmpl_id, title = self.template_id, self._template.title
        return string.format(tmpl_id, title, self.uid, self._start, self._end)

    def report(self):
        """
        Creates and returns a complete execution report, including workflow and
        tasks templates and execution details.
        """
        wf_exec = {"id": self.uid, "start": self._start, "end": self._end}
        wf_exec['state'] = future_state(self).value
        report = self._template.as_dict()
        report['exec'] = wf_exec
        # Update task descriptions to add info about their execution.
        tasks_list = report['tasks']
        for task_dict in tasks_list:
            try:
                task, exec_dict = self._tasks_by_id[task_dict['id']]
            except KeyError:
                task_dict.update({'exec': None})
                continue
            exec_dict['state'] = future_state(task).value
            # If the task is linked to a task holder, try to use its own report
            try:
                task_report = task.holder.report()
            except AttributeError:
                pass
            else:
                if task_report is not None:
                    exec_dict.update(task_report)
            task_dict.update({'exec': exec_dict})
        return report


def new_workflow(wf_tmpl, running=None, loop=None):
    """
    Returns a new workflow execution object if a new instance can be run.
    It depends on the template's overrun policy and the list of running
    workflow instances (given by `running`).
    """
    policy_handler = OverrunPolicyHandler(wf_tmpl, loop=loop)
    return policy_handler.new_workflow(running)


def unlock_workflow_when_done():
    """
    Adds a done callback to the current task so as to unlock the workflow that
    handles its execution when the task gets done.
    If no workflow linked to the task can be found, raise an exception.
    It assumes all tasks scheduled from within a workflow object have at least
    one done callback which is a bound method from the workflow object.

    Note: bound methods from distinct workflow ojects in the task's
    "call when done" list are not supported. It must never happen!
    """
    task = asyncio.Task.current_task()
    for cb in task._callbacks:
        # inspect.getcallargs() gives acces to the implicit 'self' arg of the
        # bound method but it is marked as deprecated since Python 3.5.1
        # and the new `inspect.Signature` object does do the job :((
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
            task.add_done_callback(inst._unlock)
            break
    else:
        raise WorkflowNotFoundError
