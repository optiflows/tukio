"""Test all task modules from tukio.task"""
import unittest
import asyncio
from uuid import uuid4

from tukio.task import (
    TaskHolder, register, tukio_factory, TukioTask, TaskRegistry,
    UnknownTaskName, new_task, TaskTemplate
)


MY_TASK_HOLDER_RES = 'Tukio task as task holder'
MY_CORO_TASK_RES = 'Tukio task as coroutine'


class MyTaskHolder(TaskHolder):

    """
    A valid task holder
    """

    def __init__(self, config):
        self.config = config
        self.uid = str(uuid4())

    async def do_it(self, data):
        return MY_TASK_HOLDER_RES


class BasicTaskHolder:

    """
    A valid task does not necessarily inherit from `TaskHolder`
    """

    def __init__(self, config):
        pass

    async def mycoro(self):
        pass


class BadInitTaskHolder1:

    """
    A class with `__init__` that does not match the expected signature:
    (self, config, *, **kwargs)
    """

    def __init__(self):
        pass

    async def dummy(self):
        pass


class MyDummyError(Exception):
    pass


class BadInitTaskHolder2:

    """
    A class with `__init__` that raises an exception
    """

    def __init__(self, config):
        raise MyDummyError

    async def dummy(self):
        pass


class BadInitTaskHolder3:

    """
    A class without coroutine to register
    """

    def __init__(self):
        pass

    def dummy(self):
        pass


class BadExecTaskHolder1:

    """
    A class with a coroutine that cannot be passed data is not valid
    """

    def __init__(self, config):
        pass

    async def dummy(self):
        pass


async def my_coro_task(data):
    """
    A valid coroutine
    """
    return MY_CORO_TASK_RES


async def bad_coro_task():
    """
    A coroutine with a bad signature (no argument)
    """


async def other_coro(data):
    """
    Another valid coroutine
    """
    return None


def my_func():
    """
    A dummy function (not a valid Tukio task)
    """
    return None


def my_gen():
    """
    A dummy generator (not a valid Tukio task)
    """
    yield 'dummy'


# utils


def _save_registry():
    return TaskRegistry._registry.copy(), TaskRegistry._codes.copy()


def _restore_registry(backup):
    registry, codes = backup
    TaskRegistry._registry = registry
    TaskRegistry._codes = codes


# tests


class TestTaskRegistry(unittest.TestCase):

    """
    Test task holder and coroutine registrations
    """

    def setUp(self):
        self._backup = _save_registry()

    def tearDown(self):
        _restore_registry(self._backup)

    def test_valid_registrations(self):
        """
        Check the current registry (provisioned by registered task holder and
        coroutine) holds the right references.
        """
        # A task holder is registered as a tuple made of (klass, coro_fn)
        register('my-task-holder', 'do_it')(MyTaskHolder)
        klass, coro_fn = TaskRegistry.get('my-task-holder')
        self.assertIs(klass, MyTaskHolder)
        self.assertIs(coro_fn, MyTaskHolder.do_it)
        self.assertEqual(MyTaskHolder.TASK_NAME, 'my-task-holder')

        # A coroutine is registered as a tuple made of (None, coro_fn)
        register('my-coro-task')(my_coro_task)
        klass, coro_fn = TaskRegistry.get('my-coro-task')
        self.assertIs(klass, None)
        self.assertIs(coro_fn, my_coro_task)

        # A coroutine can be registered sevral times with distinct names.
        # Note that an extra parameter passed to register will be ignored.
        register('other-coro-task', 'dummy')(my_coro_task)
        klass, coro_fn = TaskRegistry.get('other-coro-task')
        self.assertIs(klass, None)
        self.assertIs(coro_fn, my_coro_task)

    def test_registry_get_unknown(self):
        """
        Looking for an unknown Tukio task must raise a `KeyError` exception
        """
        with self.assertRaises(UnknownTaskName):
            TaskRegistry.get('dummy')

    def test_register_not_coroutine(self):
        """
        Trying to register a function or method that is not a coroutine must
        raise a `TypeError` exception.
        """
        err = 'not a coroutine function'
        registered = len(TaskRegistry.all())

        # A regular function cannot be registered
        with self.assertRaisesRegex(TypeError, err):
            register('dummy')(my_func)

        # A generator cannot be registered either
        with self.assertRaisesRegex(TypeError, err):
            register('dummy')(my_gen)
        with self.assertRaisesRegex(TypeError, err):
            register('dummy')(my_gen())

        # A simple method is not a valid task
        with self.assertRaisesRegex(TypeError, err):
            register('dummy-task', 'dummy')(BadInitTaskHolder3)

        # No new item must have been registered
        self.assertEqual(registered, len(TaskRegistry.all()))

    def test_register_same_name(self):
        """
        Trying to register a Tukio task with a name already used must raise a
        `ValueError` exception.
        """
        register('my-coro-task')(other_coro)
        with self.assertRaisesRegex(ValueError, 'already registered'):
            register('my-coro-task')(other_coro)

    def test_bad_register_class(self):
        """
        Trying to register a class with invalid/missing parameters passed to
        `register()` must raise a `ValueError` or `AttributeError` exception.
        """
        registered = len(TaskRegistry.all())

        # Register a class without the name of the method to register
        with self.assertRaisesRegex(TypeError, 'getattr()'):
            register('dummy-task')(MyTaskHolder)

        # Register a class with an invalid `coro_name` arg
        with self.assertRaisesRegex(TypeError, 'getattr()'):
            register('dummy-task', object())(MyTaskHolder)

        # Register a class with a wrong method name
        with self.assertRaisesRegex(AttributeError, 'yolo'):
            register('dummy-task', 'yolo')(MyTaskHolder)

        # No new item must have been registered
        self.assertEqual(registered, len(TaskRegistry.all()))

    def test_register_basic_class(self):
        """
        A class that does not inherit from `TaskHolder` is still a valid task
        holder if registered properly.
        """
        register('basic-task', 'mycoro')(BasicTaskHolder)
        klass, coro_fn = TaskRegistry.get('basic-task')
        self.assertIs(klass, BasicTaskHolder)
        self.assertIs(coro_fn, BasicTaskHolder.mycoro)


class TestNewTask(unittest.TestCase):

    """
    Test new tasks are created as expected from registered Tukio task names.
    """

    @classmethod
    def setUpClass(cls):
        # Save the initial state of `TaskRegistry`
        cls._backup = _save_registry()
        # Register all tasks used in the tests
        register('my-task-holder', 'do_it')(MyTaskHolder)
        register('my-coro-task')(my_coro_task)
        register('task-bad-inputs', 'dummy')(BadInitTaskHolder1)
        register('task-init-exc', 'dummy')(BadInitTaskHolder2)
        register('task-bad-coro', 'dummy')(BadExecTaskHolder1)

    @classmethod
    def tearDownClass(cls):
        # Await all dummy tasks created before test case complete to keep a
        # clean output.
        loop = asyncio.get_event_loop()
        tasks = asyncio.Task.all_tasks(loop=loop)
        loop.run_until_complete(asyncio.wait(tasks))
        # Restore the initial state of `TaskRegistry`
        _restore_registry(cls._backup)

    def test_new_task_ok(self):
        """
        Various cases which must lead to create asyncio tasks successfully.
        """
        # Create a task from a task holder
        task = new_task('my-task-holder')
        self.assertIsInstance(task, asyncio.Task)

        # Create a task from a simple coroutine
        task = new_task('my-coro-task')
        self.assertIsInstance(task, asyncio.Task)

    def test_new_task_unknown(self):
        """
        Cannot create a new task from an unknown name. It must raise a KeyError
        exception (just like `TaskRegistry.get`).
        """
        with self.assertRaises(UnknownTaskName):
            new_task('dummy')

    def test_new_task_bad_inputs(self):
        """
        Trying to create a new asyncio task with invalid inputs must raise
        a `TypeError` exception.
        """
        # 1 (mandatory) positional argument (None) passed to the coroutine
        # whereas the coroutine takes no argument
        with self.assertRaisesRegex(TypeError, 'positional argument'):
            new_task('task-bad-coro')

    def test_new_task_bad_holder(self):
        """
        Trying to create a new task from an invalid task holder may raise
        various exceptions. Ensure those exceptions are raised by `new_task`.
        """
        # Cannot create a task with `__init__` has an  invalid signature
        with self.assertRaisesRegex(TypeError, 'positional argument'):
            new_task('task-bad-inputs', config={'hello': 'world'})

        # Cannot create a task when `__init__` raises an exception
        with self.assertRaises(MyDummyError):
            new_task('task-init-exc')


class TestTaskFactory(unittest.TestCase):

    """
    Test task creation with and without the Tukio task factory set in the event
    loop
    """

    @classmethod
    def setUpClass(cls):
        cls._backup = _save_registry()
        register('dummy-holder', 'do_it')(MyTaskHolder)
        register('basic-holder', 'mycoro')(BasicTaskHolder)
        register('dummy-coro')(my_coro_task)
        cls.loop = asyncio.get_event_loop()
        cls.holder = MyTaskHolder({'hello': 'world'})
        cls.basic = BasicTaskHolder({'hello': 'world'})

    @classmethod
    def tearDownClass(cls):
        _restore_registry(cls._backup)

    def test_with_tukio_factory(self):
        """
        The tukio task factory must create `TukioTask` when the coroutine is a
        registered Tukio task, else it must create a regular `asyncio.Task`
        object.
        """
        self.loop.set_task_factory(tukio_factory)

        # Create and run a `TukioTask` from a registered task holder
        task = asyncio.ensure_future(self.holder.do_it('yopla'))
        self.assertIsInstance(task, TukioTask)
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, MY_TASK_HOLDER_RES)
        self.assertEqual(task._outputs, MY_TASK_HOLDER_RES)

        # Create and run a `TukioTask` from a registered coroutine
        task = asyncio.ensure_future(my_coro_task(None))
        self.assertIsInstance(task, TukioTask)
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, MY_CORO_TASK_RES)

        # Run a regular coroutine
        task = asyncio.ensure_future(other_coro(None))
        self.assertIsInstance(task, asyncio.Task)
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, None)

        # Run a generator (e.g. as returned by `asyncio.wait`)
        # The task factory is also called in this situation and is passed the
        # generator object.
        t1 = asyncio.ensure_future(my_coro_task(None))
        t2 = asyncio.ensure_future(other_coro(None))
        self.assertIsInstance(t1, TukioTask)
        self.assertIsInstance(t2, asyncio.Task)
        gen = asyncio.wait([t1, t2])
        # A task is created inside `asyncio.run_until_complete` anyway...
        task = asyncio.ensure_future(gen)
        self.assertIsInstance(task, asyncio.Task)
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, ({t1, t2}, set()))

        # Tukio task factory does not affect futures passed to `ensure_future`
        future = asyncio.ensure_future(asyncio.Future())
        self.assertIsInstance(future, asyncio.Future)

    def test_without_tukio_factory(self):
        """
        When the Tukio task factory is not set in the loop, all coroutines must
        be wrapped in a regular `asyncio.Task` object regardless of whether
        they are registered Tukio tasks or not.
        """
        # Reset to default task factory
        self.loop.set_task_factory(None)

        # Create and run a Tukio task implemented as a task holder
        task = asyncio.ensure_future(self.holder.do_it('bar'))
        self.assertIsInstance(task, asyncio.Task)
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, MY_TASK_HOLDER_RES)

        # Create and run a Tukio task implemented as a coroutine
        task = asyncio.ensure_future(my_coro_task(None))
        self.assertIsInstance(task, asyncio.Task)
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, MY_CORO_TASK_RES)

        # Create and run a regular `asyncio.Task`
        task = asyncio.ensure_future(other_coro(None))
        self.assertIsInstance(task, asyncio.Task)
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, None)

        # Run a generator (e.g. as returned by `asyncio.wait`)
        # The task factory is also called in this situation and is passed the
        # generator object.
        t1 = asyncio.ensure_future(my_coro_task(None))
        t2 = asyncio.ensure_future(other_coro(None))
        self.assertIsInstance(t1, asyncio.Task)
        self.assertIsInstance(t2, asyncio.Task)
        gen = asyncio.wait([t1, t2])
        # A task is created inside `asyncio.run_until_complete` anyway...
        task = asyncio.ensure_future(gen)
        self.assertIsInstance(task, asyncio.Task)
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, ({t1, t2}, set()))

    def test_tukio_task_attrs_from_coro(self):
        """
        Check `TukioTask` instances always have `.holder` and `.uid` attributes
        when `tukio_factory` is set.
        """
        self.loop.set_task_factory(tukio_factory)

        # Create a task from a simple coroutine
        task = asyncio.ensure_future(my_coro_task(None))
        self.assertTrue(hasattr(task, 'holder'))
        self.assertTrue(hasattr(task, 'uid'))
        self.assertIsNone(task.holder)
        self.assertIsInstance(task.uid, str)
        # uuid4() always returns a 36-chars long ID
        self.assertEqual(len(task.uid), 36)

        # Create a task from a task holder (inherited from `TaskHolder`)
        task = asyncio.ensure_future(self.holder.do_it('foo'))
        self.assertTrue(hasattr(task, 'holder'))
        self.assertTrue(hasattr(task, 'uid'))
        self.assertIs(task.holder, self.holder)
        self.assertIsInstance(task.uid, str)
        # uuid4() always returns a 36-chars long ID
        self.assertEqual(len(task.uid), 36)
        self.assertEqual(task.uid, task.holder.uid)

        # Create a task from a basic task holder
        task = asyncio.ensure_future(self.basic.mycoro())
        self.assertTrue(hasattr(task, 'holder'))
        self.assertTrue(hasattr(task, 'uid'))
        self.assertIs(task.holder, self.basic)
        self.assertIsInstance(task.uid, str)
        # uuid4() always returns a 36-chars long ID
        self.assertEqual(len(task.uid), 36)


class TestTaskTemplate(unittest.TestCase):

    """
    Task templates are simple objects that must basically load and dump.
    """

    @classmethod
    def setUpClass(cls):
        cls._backup = _save_registry()
        register('my-task-holder', 'do_it')(MyTaskHolder)
        register('basic-holder', 'mycoro')(BasicTaskHolder)
        register('dummy-coro')(my_coro_task)
        register('bad-coro-task')(bad_coro_task)
        cls.loop = asyncio.get_event_loop()
        cls.holder = MyTaskHolder({'hello': 'world'})
        cls.basic = BasicTaskHolder({'hello': 'world'})

    @classmethod
    def tearDownClass(cls):
        # Await all dummy tasks created before test case complete to keep a
        # clean output.
        loop = asyncio.get_event_loop()
        tasks = asyncio.Task.all_tasks(loop=loop)
        loop.run_until_complete(asyncio.wait(tasks))
        _restore_registry(cls._backup)

    def test_new_template(self):
        """
        Valid new task template operations
        """
        # The simplest case: only a name is required
        # Note: task name is not checked!
        name = 'dummy'
        task_tmpl = TaskTemplate(name)
        self.assertEqual(task_tmpl.name, name)
        self.assertEqual(task_tmpl.config, {})
        self.assertEqual(task_tmpl.topics, [])
        # Even if no ID is provided, it must be generated
        self.assertIsInstance(task_tmpl.uid, str)
        self.assertEqual(len(task_tmpl.uid), 36)

    def test_new_task(self):
        """
        Can create new asyncio tasks from the task template
        """
        self.loop.set_task_factory(tukio_factory)

        # Create a task from a registered task holder
        task_tmpl = TaskTemplate('my-task-holder')
        task = task_tmpl.new_task('dummy-data', loop=self.loop)
        self.assertIsInstance(task, TukioTask)
        self.assertEqual(task.inputs, 'dummy-data')

        # Also works with a registered coroutine
        task_tmpl = TaskTemplate('dummy-coro')
        task = task_tmpl.new_task(data='junk-data', loop=self.loop)
        self.assertIsInstance(task, TukioTask)
        self.assertEqual(task.inputs, 'junk-data')

    def test_new_task_unknown(self):
        """
        Trying to create a new task with an unknown name must raise a
        `UnknownTaskName` exception.
        """
        task_tmpl = TaskTemplate('dummy')
        with self.assertRaises(UnknownTaskName):
            task_tmpl.new_task('junk-data')

    def test_new_task_bad_args(self):
        """
        Trying to create a new task with invalid arguments must raise a
        `TypeError` exception.
        """
        # Missing argument
        task_tmpl = TaskTemplate('my-task-holder')
        with self.assertRaisesRegex(TypeError, 'positional argument'):
            task_tmpl.new_task()

        # Too many arguments
        task_tmpl = TaskTemplate('bad-coro-task')
        with self.assertRaisesRegex(TypeError, 'positional argument'):
            task_tmpl.new_task('junk')

    def test_build_from_dict(self):
        """
        Create a new task template from a dictionary
        """
        task_dict = {
            "id": "1234",
            "name": "dummy"
        }
        task_tmpl = TaskTemplate.from_dict(task_dict)
        self.assertIsInstance(task_tmpl, TaskTemplate)
        self.assertEqual(task_tmpl.uid, '1234')
        self.assertEqual(task_tmpl.name, 'dummy')
        self.assertEqual(task_tmpl.topics, [])
        self.assertEqual(task_tmpl.config, {})

        # Can also create a task template without ID defined in the dict
        task_dict = {
            "name": "dummy",
            "topics": ["yummy"],
            "config": {"hello": "world"},
            "foo": None,
            "bar": [1, 2, 3, 4]
        }
        task_tmpl = TaskTemplate.from_dict(task_dict)
        self.assertIsInstance(task_tmpl, TaskTemplate)
        # Even if no ID is provided, it must be generated
        self.assertIsInstance(task_tmpl.uid, str)
        self.assertEqual(len(task_tmpl.uid), 36)
        self.assertEqual(task_tmpl.name, 'dummy')
        self.assertEqual(task_tmpl.topics, ["yummy"])
        self.assertEqual(task_tmpl.config, {"hello": "world"})

        # Additional keys in the dict don't harm
        task_dict = {
            "name": "dummy",
            "foo": None,
            "bar": [1, 2, 3, 4]
        }
        task_tmpl = TaskTemplate.from_dict(task_dict)
        self.assertIsInstance(task_tmpl, TaskTemplate)
        # Even if no ID is provided, it must be generated
        self.assertIsInstance(task_tmpl.uid, str)
        self.assertEqual(len(task_tmpl.uid), 36)
        self.assertEqual(task_tmpl.name, 'dummy')
        self.assertEqual(task_tmpl.topics, [])
        self.assertEqual(task_tmpl.config, {})

    def test_build_from_dict_without_name(self):
        """
        The only required argument to create a task template is the task
        name.
        """
        task_dict = {
            "id": "1234",
        }
        with self.assertRaises(KeyError):
            TaskTemplate.from_dict(task_dict)

    def test_dump_as_dict(self):
        """
        A TaskTemplate instance can be dumped as a dictionary
        """
        task_tmpl = TaskTemplate('my-task-holder', config={'hello': 'world'})
        task_tmpl.topics = ['my-topic']

        expected_dict = {
            'name': 'my-task-holder',
            'timeout': None,
            'topics': ['my-topic'],
            'config': {'hello': 'world'}
        }

        task_dict = task_tmpl.as_dict()
        del task_dict['id']
        self.assertEqual(task_dict, expected_dict)

        # The dumped dict must be loadable by the `from_dict()` classmethod
        other_tmpl = TaskTemplate.from_dict(task_dict)
        self.assertIsInstance(other_tmpl, TaskTemplate)


if __name__ == '__main__':
    unittest.main()
