import asyncio
import logging
from copy import copy
from datetime import datetime, timezone

from .holder import TaskHolder
from .task import register


log = logging.getLogger(__name__)


@register('join', 'execute')
class JoinTask(TaskHolder):

    """
    A join task is an almost-standard task that allows awaiting for multiple
    parents and gathering their results using `data_received()`.
    The `wait_for` config parameter is mandatory and is a list of task IDs.
    """

    __slots__ = ('_data_stash', '_wait_for', '_timeout', '_report')

    def __init__(self, config):
        super().__init__(config)
        self._data_stash = []
        # Must be either a number of tasks or a list of ids
        # Copy because we alter the list/int during `_step`
        self._wait_for = copy(self.config['wait_for'])
        # Default timeout to avoid infinite wait
        self._timeout = self.config.get('timeout')

        # Reporting
        self._report = {'tasks': [], 'status': 'running'}

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
        log.info(
            'Join task waiting for tasks (%s) (timeout: %s)',
            self._wait_for, self._timeout
        )
        self._task = asyncio.Task.current_task()
        # Trigger first step for this event
        self._step(event)
        data = event.data
        try:
            await asyncio.wait_for(self._wait_for_tasks(), self._timeout)
        except asyncio.TimeoutError:
            log.warning("Join timed out, still waiting for %s", self._wait_for)
            self._report['status'] = 'timeout'
            self._task.dispatch_progress(self._report)
        else:
            log.debug('All awaited parents joined')
            self._report['status'] = 'done'
            self._task.dispatch_progress(self._report)
        # Data contains the outputs of the first task to join
        # Variable `data_stash` contains a list of all parent tasks outputs
        data['data_stash'] = self._data_stash
        return data
