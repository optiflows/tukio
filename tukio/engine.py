"""
Tukio Workflow Engine
"""
import asyncio
import logging

from tukio.workflow import OverrunPolicy, new_workflow, Workflow
from tukio.broker import get_broker
from tukio.task import tukio_factory
from tukio.utils import Listen
from tukio.event import Event


log = logging.getLogger(__name__)


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
        Loads a new workflow template in the selector. If the template is
        already loaded, unloads it first.
        """
        # Cleanup the current template ID/topic associations before loading the
        # new template.
        try:
            current = self._templates[template.uid]
        except KeyError:
            pass
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

    def select(self, topic=None):
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

    def get(self, tmpl_id):
        """
        Returns the workflow template with the given template ID, None if
        no template is found.
        """
        return self._templates.get(tmpl_id)


class Engine(asyncio.Future):

    """
    The Tukio workflow engine. Basically, it can load or unload workflow
    templates and trigger new executions of workflows upon receiving new data.
    The `run()` method allows to select and trigger a particular workflow.
    It is an awaitable object (inherits from `asyncio.Future`) which will be
    marked as done after its `stop()` method has been called and all the
    running workflows are done. Afterwards no new workflow can be triggered.
    """

    def __init__(self, *, selector=None, loop=None):
        super().__init__(loop=loop)
        # use the custom asyncio task factory
        self._loop.set_task_factory(tukio_factory)
        self._selector = selector or _WorkflowSelector()
        self._instances = []
        self._broker = get_broker(self._loop)
        self._lock = asyncio.Lock()
        self._must_stop = False

    @property
    def selector(self):
        """
        Returns the `_WorkflowSelector` instance for use outside of the engine
        """
        return self._selector

    @property
    def instances(self):
        """
        Returns the list or running instances
        """
        return self._instances

    def _add_wflow(self, wflow):
        """
        Adds a new entry into the list of running instances.
        """
        self._instances.append(wflow)
        log.debug('new workflow started %s', wflow)

    def _remove_wflow(self, wflow):
        """
        Removes a worflow instance from the list of running instances.
        """
        self._instances.remove(wflow)
        log.debug('workflow removed from the running list: %s', wflow)

        try:
            wflow.result()
        except asyncio.CancelledError:
            log.info('Workflow %s has been cancelled', wflow.uid[:8])
        except Exception as exc:
            log.warning('Workflow %s ended on exception', wflow.uid[:8])
            log.exception(exc)

        if self._must_stop and not self._instances and not self.done():
            self.set_result(None)
            log.debug('no more workflow running, engine stopped')

    def stop(self, force=False):
        """
        Prevents new workflow instances from being run and optionally cancels
        all running workflows (when force=True).
        """
        self._must_stop = True
        if not self._instances and not self.done():
            self.set_result(None)
        elif force:
            for wflow in self._instances:
                wflow.cancel()
                log.debug('cancelled workflow %s', wflow)
        return self

    def _load(self, template):
        """
        Loads a workflow template into the engine. Each workflow may be
        triggered as soon as it is loaded.
        Duplicates or invalid descriptions raise an exception.
        This operation does not affect workflow executions in progress.
        """
        template.validate()
        self._selector.load(template)
        log.debug("new workflow template loaded: %s", template)

    async def load(self, template):
        """
        A coroutine that loads a new workflow template while preventing other
        coroutines from updating the dict of loaded templates in the mean time.
        """
        with await self._lock:
            self._load(template)

    async def reload(self, templates):
        """
        Replaces the current list of loaded workflow templates by a new one.
        This operation does not affect workflow executions in progress.
        """
        with await self._lock:
            self._selector.clear()
            for tmpl in templates:
                self._load(tmpl)

    async def unload(self, template_id):
        """
        Unloads a workflow template from the engine. Returns the template
        object if the template was found and actually unloaded, else raises
        a `KeyError` exception.
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
        log.debug("data received: %s (topic=%s)", data, topic)
        event = Event(data, topic=topic)
        # Disptatch data to 'listening' tasks at all cases
        self._broker.dispatch(event, topic)
        # Don't start new workflow instances if `stop()` was called.
        if self._must_stop:
            log.debug("The engine is stopping, cannot trigger new workflows")
            return
        with await self._lock:
            if asyncio.iscoroutinefunction(self._selector.select):
                templates = await self._selector.select(topic)
            else:
                templates = self._selector.select(topic)
            # Try to trigger new workflows from the current dict of workflow
            # templates at all times!
            wflows = []
            for tmpl in templates:
                wflow = self._try_run(tmpl, event)
                if wflow:
                    wflows.append(wflow)
        return wflows

    async def trigger(self, template_id, data):
        """
        Trigger a new execution of the workflow template identified by
        `template_id`. Use this method instead of a reserved topic +
        `data_received` to trigger a specific workflow.
        """
        with await self._lock:
            # Ignore unknown (aka not loaded) workflow templates
            if asyncio.iscoroutinefunction(self._selector.get):
                template = await self._selector.get(template_id)
            else:
                template = self._selector.get(template_id)
            if not template:
                return None
            return self._try_run(template, Event(data))

    def _do_run(self, wflow, event):
        """
        A workflow instance must be in the list of running instances until
        it completes.
        """
        self._add_wflow(wflow)
        wflow.add_done_callback(self._remove_wflow)
        wflow.run(event)

    def _try_run(self, template, event):
        """
        Try to run a new instance of workflow according to the instances
        already running and the overrun policy.
        """
        # Don't start new workflow instances if `stop()` was called.
        if self._must_stop:
            log.debug("The engine is stopping, cannot trigger new workflows")
            return
        # Always apply the policy of the current workflow template (workflow
        # instances may run with another version of the template)
        wflow = new_workflow(template, running=self._instances, loop=self._loop)
        if wflow:
            if template.policy == OverrunPolicy.ABORT_RUNNING:
                others = [
                    i for i in self._instances
                    if i.template.uid == template.uid
                ]
                if others:
                    def cb(f):
                        self._do_run(wflow, event)
                    others_done = asyncio.ensure_future(asyncio.wait(others))
                    others_done.add_done_callback(cb)
                    return wflow
            self._do_run(wflow, event)
        else:
            log.debug("skip new workflow from %s (overrun policy)", template)
        return wflow

    async def run_once(self, template, data):
        """
        Starts a new execution of the workflow template regardless of the
        overrun policy and already running workflows.
        Note: it does NOT load the workflow template in the engine.
        """
        if self._must_stop:
            log.debug("The engine is stopping, cannot run a new workflow from"
                      "template id %s", template.uid)
            return None
        with await self._lock:
            wflow = Workflow(template, loop=self._loop)
            self._do_run(wflow, Event(data))
        return wflow

    async def rescue(self, template, report):
        """
        Attaches a dangling workflow instance using its last known report.
        """
        if self._must_stop:
            log.debug("The engine is stopping, cannot rescue the workflow from"
                      "its report (execution id %s)", report['exec']['id'])
            return None

        with await self._lock:
            wflow = Workflow(template, loop=self._loop)
            self._add_wflow(wflow)
            wflow.add_done_callback(self._remove_wflow)
            wflow.fast_forward(report)
        return wflow
