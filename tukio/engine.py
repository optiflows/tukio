"""
Tukio Workflow Engine
"""
import asyncio
import logging
import weakref

from tukio.workflow import OverrunPolicy, new_workflow
from tukio.broker import get_broker
from tukio.task import tukio_factory
from tukio.utils import Listen


log = logging.getLogger(__name__)


class LoadWorkflowError(Exception):

    def __init__(self, new_tmpl, current_tmpl, message=None):
        super().__init__()
        self.new = new_tmpl
        self.current = current_tmpl
        self.message = message

    def __str__(self):
        err = 'cannot load {}, it is not newer than {}'
        if self.message is not None:
            err = err + '. ' + self.message
        return err.format(self.new, self.current)


class _WorkflowSelector:

    """
    This class stores all the workflow templates loaded in the workflow engine
    and the association template ID/topics. Thanks to this association, it can
    provide a list of 'trigger-able' workflow templates from a given topic or
    return the right template from a given template ID.
    This object is used from within the workflow engine and is not meant to be
    used by others modules.
    """

    def __init__(self):
        self._selector = {Listen.everything: set()}
        self._templates = dict()

    def load(self, template):
        """
        Loads a new workflow template in the selector. If a previous version
        of this template was loaded, unload it first.
        """
        # Cleanup the current template ID/topic associations before loading the
        # new template.
        try:
            current = self._templates[template.uid]
        except KeyError:
            pass
        else:
            # A workflow template is uniquely defined by the tuple
            # (template ID, version) but only a single version of template ID
            # can be loaded.
            # TODO: support multiple versions of the same template?
            if current.version > template.version:
                raise LoadWorkflowError(template, current)
            else:
                self.unload(current.uid)
        self._templates[template.uid] = template

        # Add new template ID/topic associations
        listen = template.listen
        if listen is Listen.everything:
            self._selector[Listen.everything].add(template)
        elif listen is Listen.topics:
            for topic in template.topics:
                try:
                    self._selector[topic].add(template)
                except KeyError:
                    self._selector[topic] = {template}

    def unload(self, tmpl_id):
        """
        Unloads a workflow template from the selector. Raises a `KeyError`
        exception if the given template ID is not loaded.
        """
        template = self._templates.pop(tmpl_id)

        listen = template.listen
        if listen is Listen.everything:
            self._selector[Listen.everything].remove(template)
        elif listen is Listen.topics:
            for topic in template.topics:
                self._selector[topic].remove(template)
                if not self._selector[topic]:
                    del self._selector[topic]
        return template

    def clear(self):
        """
        Removes all workflow templates loaded in the selector. As a consequence
        a call to `get()` right after this operation will always return an
        empty list.
        """
        self._templates.clear()
        self._selector.clear()
        self._selector[Listen.everything] = set()

    def get(self, topic=None):
        """
        Returns the list of workflow templates that can be triggered by new
        data received in the given topic.
        Remember that topic=None means all workflow templates that can be
        triggered whatever the topic (including no topic).
        """
        # Always include the set of global workflow templates (trigger-able in
        # any case)
        templates = self._selector[Listen.everything]
        if topic is not None:
            try:
                templates = templates | self._selector[topic]
            except KeyError:
                pass
        return list(templates)


class DraftError(Exception):
    pass


class Engine(asyncio.Future):

    """
    The Tukio workflow engine. Basically, it can load or unload workflow
    templates and trigger new executions of workflows upon receiving new data.
    The `run()` method allows to select and trigger a particular workflow.
    Workflow executions can be cancelled as per their execution ID (`cancel()`)
    or all at once (`cancel_all()`).
    It is an awaitable object (inherits from `asyncio.Future`) which will be
    marked as done after its `stop()` method has been called and all the
    running workflows are done. Afterwards no new workflow can be triggered.
    """

    def __init__(self, *, loop=None):
        super().__init__(loop=loop)
        # use the custom asyncio task factory
        self._loop.set_task_factory(tukio_factory)
        self._selector = _WorkflowSelector()
        self._running = dict()
        self._broker = get_broker(self._loop)
        self._lock = asyncio.Lock()
        self._running_by_id = weakref.WeakValueDictionary()
        self._must_stop = False

    @property
    def templates(self):
        """
        Returns the dict of loaded workflow templates.
        """
        return self._selector._templates

    def _add_wflow(self, wflow):
        """
        Adds a new entry into the dict of running workflows and updates the
        weak value dict to index it by its execution ID.
        """
        try:
            self._running[wflow.template_id].append(wflow)
        except KeyError:
            self._running[wflow.template_id] = [wflow]
        self._running_by_id[wflow.uid] = wflow
        log.debug('new workflow started %s', wflow)

    def _remove_wflow(self, wflow):
        """
        Removes a worflow instance from the dict of running workflows.
        """
        self._running[wflow.template_id].remove(wflow)
        # Cleanup the running dict if no more running instance of that template
        if len(self._running[wflow.template_id]) == 0:
            del self._running[wflow.template_id]
        del self._running_by_id[wflow.uid]
        log.debug('workflow removed from the running list: {}'.format(wflow))
        if self._must_stop and not self._running and not self.done():
            self.set_result(None)
            log.debug('no more workflow running, engine stopped')

    def stop(self, force=False):
        """
        Cancels all workflows and prevent new instances from being run.
        """
        self._must_stop = True
        if not self._running and not self.done():
            self.set_result(None)
        elif force:
            self.cancel_all()
        return self

    def _run_in_task(self, callback, *args, **kwargs):
        """
        Wrap a regular function into a coroutine and run it in a task.
        This is intended to wrap time consuming functions into a task so as to
        prevent slow operations from blocking the whole loop.
        """
        async def coro():
            return callback(*args, **kwargs)
        return asyncio.ensure_future(coro(), loop=self._loop)

    def _load(self, template):
        """
        Loads a workflow template into the engine. Each workflow may be
        triggered as soon as it is loaded.
        Duplicates or invalid descriptions raise an exception.
        This operation does not affect workflow executions in progress.
        """
        if template.draft:
            raise DraftError('template must not be a draft')

        template.validate()
        self._selector.load(template)
        log.debug("new workflow template loaded: %s", template)

    async def load(self, template):
        """
        A coroutine that loads a new workflow template while preventing other
        coroutines from updating the dict of loaded templates in the mean time.
        """
        with await self._lock:
            await self._run_in_task(self._load, template)

    async def reload(self, templates):
        """
        Replaces the current list of loaded workflow templates by a new one.
        This operation does not affect workflow executions in progress.
        """
        with await self._lock:
            self._selector.clear()
            for tmpl in templates:
                await self._run_in_task(self._load, tmpl)

    async def unload(self, template_id):
        """
        Unloads a workflow template from the engine. Returns True if the
        template was found and actually unloaded, else, returns False.
        """
        with await self._lock:
            template = self._selector.unload(template_id)
        return template

    async def data_received(self, data, topic=None):
        """
        This method should be called to pass an event to the workflow engine
        which in turn will disptach this event to the right running workflows
        and may trigger new workflow executions.
        """
        log.debug("data received '%s' in topic '%s'", data, topic)
        self._broker.dispatch(data, topic)
        # Don't start new workflow instances if `stop()` was called.
        if self._must_stop:
            return
        with await self._lock:
            templates = self._selector.get(topic)
            # Try to trigger new workflows from the current dict of workflow
            # templates at all times!
            wflows = []
            for tmpl in templates:
                wflow = await self._run_in_task(self._try_run, tmpl, data)
                if wflow:
                    wflows.append(wflow)
        return wflows

    def _try_run(self, template, data):
        """
        Try to run a new instance of workflow defined by `tmpl_id` according to
        the instances already running and the overrun policy.
        """
        running = self._running.get(template.uid)
        # Always apply the policy of the current workflow template (workflow
        # instances may run with an old version of the template)
        wflow = new_workflow(template, running=running, loop=self._loop)
        if wflow:
            self._add_wflow(wflow)
            wflow.add_done_callback(self._remove_wflow)
            if template.policy == OverrunPolicy.abort_running and running:
                def cb():
                    wflow.run(data)
                asyncio.ensure_future(self._wait_abort(running, cb))
            else:
                wflow.run(data)
        else:
            log.debug("skip new workflow from %s (overrun policy)", template)
        return wflow

    async def _wait_abort(self, running, callback):
        """
        Wait for the end of a list of aborted (cancelled) workflows before
        starting a new one when the policy is 'abort-running'.
        """
        await asyncio.wait(running)
        callback()

    def run(self, tmpl_id, inputs):
        """
        Starts a new execution of the workflow template identified by `tmpl_id`
        regardless of the overrun policy and already running workflows.
        """
        if self._must_stop:
            log.debug("The engine is stopping, cannot run a new workflow from"
                      "template id %s", tmpl_id)
            return None
        wf_tmpl = self.templates[tmpl_id]
        wflow = new_workflow(wf_tmpl, loop=self._loop)
        self._add_wflow(wflow)
        wflow.add_done_callback(self._remove_wflow)
        wflow.run(inputs)
        return wflow

    def cancel(self, exec_id):
        """
        Cancels an execution of workflow identified by its execution ID.
        The cancelled workflow instance (a future object) is returned.
        If the workflow could not be found, returns None.
        """
        wflow = self._running_by_id.get(exec_id)
        if wflow:
            wflow.cancel()
            log.debug('cancelled workflow {}'.format(wflow))
        return wflow

    def cancel_all(self):
        """
        Cancels all the running workflows.
        """
        cancelled = 0
        for wf_list in self._running.values():
            for wflow in wf_list:
                is_cancelled = wflow.cancel()
                if is_cancelled:
                    cancelled += 1
        log.debug('cancelled {} workflows'.format(cancelled))
        return cancelled
