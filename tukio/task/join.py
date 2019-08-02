import asyncio
import logging
from copy import copy
from enum import Enum
from datetime import datetime, timezone

from .holder import TaskHolder
from .task import register


log = logging.getLogger(__name__)


class JoinStatus(Enum):

    RUNNING = 'running'
    TIMEOUT = 'timeout'
    DONE = 'done'


@register('join', 'execute')
class JoinTask(TaskHolder):

    """
    A join task is an almost-standard task that allows awaiting for multiple
    parents and gathering their results using `data_received()`.
    The `wait_for` config parameter is mandatory and is a list of task IDs.
    """

    __slots__ = ('_data_stash', '_wait_for', '_report', '_task', 'data')

    def __init__(self, config):
        super().__init__(config)
        self._data_stash = []
        # Must be either a number of tasks or a list of ids
        # Copy because we alter the list/int during `_step`
        self._wait_for = copy(self.config['wait_for'])

        # Reporting
        self._task = None
        self.data = None
        self._report = {'tasks': [], 'status': JoinStatus.RUNNING.value}

    def report(self):
        return self._report

    def add_task_report(self, task_id):
        task_report = {'id': task_id, 'end_time': datetime.now(timezone.utc)}
        self._report['tasks'].append(task_report)
        self._task.dispatch_progress(self._report)

    def _step(self, event):
        """
        Take a new event and consume it with the `wait_for` configuration.
        """
        task_id = event.source._task_template_id
        self.add_task_report(task_id)
        # Check if wait for a number of tasks
        if isinstance(self._wait_for, int):
            self._data_stash.append(event.data.copy())
            self._wait_for -= 1
            return self._wait_for == 0
        # Else, wait for list of ids
        elif isinstance(self._wait_for, list) and task_id in self._wait_for:
            self._data_stash.append(event.data.copy())
            self._wait_for.remove(task_id)
            return len(self._wait_for) == 0

    async def _wait_for_tasks(self):
        while True:
            new_event = await self.queue.get()
            if self._step(new_event) is True:
                return

    async def execute(self, event):
        log.info('Join task waiting for tasks (%s)', self._wait_for)
        self._task = asyncio.current_task()
        # Trigger first step for this event
        self._step(event)
        self.data = event.data
        await self._wait_for_tasks()
        log.debug('All awaited parents joined')
        self._report['status'] = JoinStatus.DONE.value
        self._task.dispatch_progress(self._report)
        # Data contains the outputs of the first task to join
        # Variable `data_stash` contains a list of all parent tasks outputs
        self.data['data_stash'] = self._data_stash
        return self.data

    def teardown(self):
        self._report['status'] = JoinStatus.TIMEOUT.value
        self._task.dispatch_progress(self._report)
        self.data['data_stash'] = self._data_stash
        return self.data
