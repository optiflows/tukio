import asyncio
from unittest import TestCase
from tukio.task import register, TaskHolder, tukio_factory
from tukio.utils import FutureState
from tukio.workflow import Workflow, WorkflowTemplate


@register('basic', 'execute')
class BasicTask(TaskHolder):

    async def execute(self, event):
        pass


@register('crash', 'execute')
class CrashTask(TaskHolder):

    def __init__(self, config):
        super().__init__(config)
        # Crash here if the key is missing
        config['init_ok']

    async def execute(self, event):
        # Or crash here
        raise Exception


@register('cancel', 'execute')
class CancelTask(TaskHolder):

    async def execute(self, event):
        Workflow.current_workflow().cancel()
        await asyncio.sleep(1.0)


@register('sleep', 'execute')
class SleepTask(TaskHolder):

    async def execute(self, event):
        await asyncio.sleep(1.0)


TEMPLATES = {
    'ok': {
        'title': 'ok',
        'tasks': [
            {'id': '1', 'name': 'basic'},
            {'id': '2', 'name': 'basic'},
            {'id': '3', 'name': 'basic'},
            {'id': '4', 'name': 'basic'},
        ],
        'graph': {
            '1': ['2', '3'],
            '2': ['4'],
            '3': [],
            '4': [],
        }
    },
    'crash_test': {
        'title': 'crash_test',
        'tasks': [
            {'id': 'crash', 'name': 'crash'},
            {'id': 'wont_run', 'name': 'basic'},
            {'id': '1', 'name': 'basic'},
            {'id': '2', 'name': 'basic'},
        ],
        'graph': {
            '1': ['crash', '2'],
            'crash': ['wont_run'],
            'wont_run': [],
            '2': [],
        }
    },
    'workflow_cancel': {
        'title': 'workflow_cancel',
        'tasks': [
            {'id': 'cancel', 'name': 'cancel'},
            {'id': '2', 'name': 'basic'},
            {'id': '3', 'name': 'basic'},
            {'id': '4', 'name': 'basic'},
        ],
        'graph': {
            'cancel': ['2', '3'],
            '2': ['4'],
            '3': [],
            '4': [],
        }
    },
    'workflow_timeout': {
        'title': 'timeout',
        'timeout': 0.1,
        'tasks': [{'id': '1', 'name': 'sleep'}],
        'graph': {'1': []}
    },
    'task_timeout': {
        'title': 'timeout',
        'tasks': [{'id': '1', 'name': 'sleep', 'timeout': 0.1}],
        'graph': {'1': []}
    },
}


class TestWorkflow(TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.loop.set_task_factory(tukio_factory)
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def test_workflow_schema(self):
        tmpl = TEMPLATES['ok']
        template = WorkflowTemplate.from_dict(tmpl)
        self.assertEqual(template.schema, 1)
        tmpl['schema'] = 5
        template = WorkflowTemplate.from_dict(tmpl)
        self.assertEqual(template.schema, 5)

    def test_basic_workflow(self):
        async def test():
            tmpl = TEMPLATES['ok']
            wflow = Workflow(WorkflowTemplate.from_dict(tmpl))
            wflow.run({'initial': 'data'})
            await wflow
            # These tasks have finished
            for tid in tmpl['graph'].keys():
                task = wflow._tasks_by_id.get(tid)
                self.assertTrue(task.done())
                self.assertEqual(FutureState.get(task), FutureState.finished)
            # The workflow finished properly
            self.assertTrue(wflow.done())
            self.assertEqual(FutureState.get(wflow), FutureState.finished)
        self.loop.run_until_complete(test())

    def test_workflow_crash(self):
        async def test():
            tmpl = TEMPLATES['crash_test']
            # Test crash at task __init__
            wflow = Workflow(WorkflowTemplate.from_dict(tmpl))
            wflow.run({'initial': 'data'})
            await wflow
            # These tasks have finished
            for tid in ('1', '2'):
                task = wflow._tasks_by_id.get(tid)
                self.assertTrue(task.done())
                self.assertEqual(FutureState.get(task), FutureState.finished)
            # These tasks were never started
            for tid in ('crash', 'wont_run'):
                task = wflow._tasks_by_id.get(tid)
                self.assertIs(task, None)
            # The workflow finished properly
            self.assertTrue(wflow.done())
            self.assertEqual(FutureState.get(wflow), FutureState.finished)

            # Test crash inside a task
            tmpl['tasks'][0]['config'] = {'init_ok': None}
            wflow = Workflow(WorkflowTemplate.from_dict(tmpl))
            wflow.run({'initial': 'data'})
            await wflow
            # These tasks have finished
            for tid in ('1', '2'):
                task = wflow._tasks_by_id.get(tid)
                self.assertTrue(task.done())
                self.assertEqual(FutureState.get(task), FutureState.finished)
            # This task crashed during execution
            task = wflow._tasks_by_id.get('crash')
            self.assertTrue(task.done())
            self.assertEqual(FutureState.get(task), FutureState.exception)
            # This task was never started
            task = wflow._tasks_by_id.get('wont_run')
            self.assertIs(task, None)
            # The workflow finished properly
            self.assertTrue(wflow.done())
            self.assertEqual(FutureState.get(wflow), FutureState.finished)
        self.loop.run_until_complete(test())

    def test_workflow_cancel(self):
        async def test():
            tmpl = TEMPLATES['workflow_cancel']
            wflow = Workflow(WorkflowTemplate.from_dict(tmpl))
            wflow.run({'initial': 'data'})
            # Workflow is cancelled
            with self.assertRaises(asyncio.CancelledError):
                await wflow
            self.assertEqual(FutureState.get(wflow), FutureState.cancelled)
            # This task was cancelled
            task = wflow._tasks_by_id.get('cancel')
            with self.assertRaises(asyncio.CancelledError):
                task.exception()
            self.assertEqual(FutureState.get(task), FutureState.cancelled)
            # These tasks were never started
            for tid in ('2', '3', '4'):
                task = wflow._tasks_by_id.get(tid)
                self.assertIs(task, None)
        self.loop.run_until_complete(test())

    def test_workflow_timeout(self):
        async def test():
            tmpl = TEMPLATES['workflow_timeout']
            wflow = Workflow(WorkflowTemplate.from_dict(tmpl))
            wflow.run({'initial': 'data'})
            # The workflow times out
            with self.assertRaises(asyncio.CancelledError):
                await wflow
            self.assertEqual(FutureState.get(wflow), FutureState.timeout)
            # The task has been cancelled
            task = wflow._tasks_by_id.get('1')
            with self.assertRaises(asyncio.CancelledError):
                task.exception()
            self.assertEqual(FutureState.get(task), FutureState.cancelled)
        self.loop.run_until_complete(test())

    def test_task_timeout(self):
        async def test():
            tmpl = TEMPLATES['task_timeout']
            wflow = Workflow(WorkflowTemplate.from_dict(tmpl))
            wflow.run({'initial': 'data'})
            # The workflow is OK
            await wflow
            self.assertEqual(FutureState.get(wflow), FutureState.finished)
            # The task has timed out
            task = wflow._tasks_by_id.get('1')
            with self.assertRaises(asyncio.CancelledError):
                task.exception()
            self.assertEqual(FutureState.get(task), FutureState.timeout)
        self.loop.run_until_complete(test())
