import asyncio
import logging
import inspect
from copy import copy
from enum import Enum
from uuid import uuid4
from datetime import datetime, timezone

from tukio.event import EventSource, Event
from tukio.utils import FutureState, SkipTask
from tukio.broker import get_broker, workflow_exec_topics

from .task import TaskRegistry


log = logging.getLogger(__name__)


class TaskExecState(Enum):

    BEGIN = 'task-begin'
    END = 'task-end'
    ERROR = 'task-error'
    SKIP = 'task-skip'
    PROGRESS = 'task-progress'
    TIMEOUT = 'task-timeout'

    @classmethod
    def values(cls):
        return [member.value for member in cls]

    @classmethod
    def from_exception(cls, exc):
        if isinstance(exc, SkipTask):
            return cls.SKIP
        else:
            return cls.ERROR


class TukioTaskError(Exception):

    def __init__(self, output):
        self.task_outputs = output


class TukioTask(asyncio.Task):

    """
    A simple subclass of `asyncio.Task()` to add an execution ID and optionally
    bind a task holder class.
    """

    __slots__ = (
        'holder', 'uid', '_broker', '_in_progress', '_template', '_workflow',
        '_source', '_start', '_end', '_inputs', '_outputs', '_queue',
        '_commited', '_timed_out',
    )

    def __init__(self, coro, *, loop=None):
        super().__init__(coro, loop=loop)
        self.holder = inspect.getcoroutinelocals(coro).get('self')
        try:
            self.uid = self.holder.uid
        except AttributeError:
            self.uid = str(uuid4())
        self._broker = get_broker(self._loop)
        self._in_progress = False
        self._template = None
        self._workflow = None
        self._source = EventSource()
        self._start = None
        self._end = None
        self._inputs = None
        self._outputs = None
        self._queue = asyncio.Queue(loop=self._loop)
        if self.holder:
            self.holder.queue = self._queue
        # A 'committed' task is a pending task not suspended
        self._committed = asyncio.Event()
        self._committed.set()
        self._timed_out = False

    @property
    def inputs(self):
        return self._inputs

    @inputs.setter
    def inputs(self, data):
        # Freeze input data (dict or event)
        self._inputs = copy(data)

    @property
    def outputs(self):
        return self._outputs

    @property
    def template(self):
        return self._template

    @property
    def workflow(self):
        return self._workflow

    @property
    def event_source(self):
        return self._source

    @property
    def queue(self):
        return self._queue

    @property
    def committed(self):
        return self._committed.is_set()

    @property
    def timed_out(self):
        return self._timed_out

    def timeout(self):
        """
        Timeout the task, cancelling its execution.
        """
        self.cancel()
        self._timed_out = True

    def suspend(self):
        """
        Suspend a task means, for now, to cancel the task but flag it as
        'suspended'. It shall be executed from scratch upon workflow resume.

        TODO: improve this method to avoid cancelling the task but to actually
        suspend transactionnal jobs inside the task.
        """
        self.cancel()
        self._committed.clear()

    def as_dict(self):
        """
        Returns the execution informations of this task.
        """
        return {
            'id': self.uid,
            'start': self._start,
            'end': self._end,
            'state': FutureState.get(self).value,
            'inputs': self._inputs,
            'outputs': self._outputs
        }

    async def data_received(self, event):
        """
        A handler that puts events into the task's own receiving queue. This
        handler shall be registered in the data broker so that any tukio task
        can receive and process events during execution.
        """
        await self._queue.put(event)

    def in_progress(self):
        """
        Returns True if the task execution started, else returns False.
        """
        return self._in_progress

    def result(self):
        """
        Wrapper around `Future.result()` to automatically dispatch a
        `TaskExecState.END` or `TaskExecState.ERROR` event.
        """
        try:
            result = super().result()
        except asyncio.CancelledError:
            # The task was cancelled, simply raise it.
            if self._timed_out is False:
                raise
            # The task timed out, fetch the outputs and raise a TimeoutError.
            if self.holder:
                self._outputs = self.holder.teardown()
            # If teardown did not return anything, return inputs.
            if self._outputs is None:
                self._outputs = self._inputs
            self._end = datetime.now(timezone.utc)
            self._broker.dispatch(
                {'type': TaskExecState.TIMEOUT.value, 'content': self._outputs},
                topics=workflow_exec_topics(self._source._workflow_exec_id),
                source=self._source,
            )
            raise asyncio.TimeoutError
        except Exception as exc:
            self._end = datetime.now(timezone.utc)
            etype = TaskExecState.from_exception(exc)
            if isinstance(exc, TukioTaskError):
                content = exc.task_outputs
                self._outputs = content
            else:
                content = {'exception': exc}
            self._broker.dispatch(
                {'type': etype.value, 'content': content},
                topics=workflow_exec_topics(self._source._workflow_exec_id),
                source=self._source,
            )
            raise
        else:
            # Freeze output data (dict or event)
            self._outputs = copy(result)
            self._end = datetime.now(timezone.utc)
            self._broker.dispatch(
                {'type': TaskExecState.END.value, 'content': self._outputs},
                topics=workflow_exec_topics(self._source._workflow_exec_id),
                source=self._source,
            )
            return result

    def dispatch_progress(self, data, event_type=None):
        """
        Dispatch task progress events in the 'EXEC_TOPIC' from
        this tukio task.
        """
        if event_type is None:
            event_type = TaskExecState.PROGRESS.value
        event_data = {'type': event_type, 'content': data}
        self._broker.dispatch(
            event_data,
            topics=workflow_exec_topics(self._source._workflow_exec_id),
            source=self._source,
        )

    def setup_workflow(self, workflow, template):
        self._workflow = workflow
        self._template = template
        self._start = datetime.now(timezone.utc)
        source = {'task_exec_id': self.uid}
        if self._template:
            source['task_template_id'] = self._template.uid
        if self._workflow:
            source['workflow_template_id'] = self._workflow.template.uid
            source['workflow_exec_id'] = self._workflow.uid
        self._source = EventSource(**source)
        self._in_progress = True
        data = {
            'type': TaskExecState.BEGIN.value,
            'content': self._inputs
        }
        self._broker.dispatch(
            data,
            topics=workflow_exec_topics(self._source._workflow_exec_id),
            source=self._source,
        )


def tukio_factory(loop, coro):
    """
    A task factory for asyncio that creates `TukioTask()` instances for all
    coroutines registered as Tukio tasks and default `asyncio.Task()` instances
    for all others.
    """
    try:
        # Trigger exception if not valid
        TaskRegistry.codes()[coro.cr_code]
    except (KeyError, AttributeError):
        # When the coroutine is not a registered Tukio task or when `coro` is a
        # simple generator (e.g. upon calling `asyncio.wait()`)
        task = asyncio.Task(coro, loop=loop)
    else:
        task = TukioTask(coro, loop=loop)
    return task
