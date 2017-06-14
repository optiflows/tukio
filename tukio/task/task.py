"""
That module assumes that a Tukio task runs as an `asyncio.Task()` instance.

It provides a registry `TaskRegistry` to register classes that define
coroutines as well as standard standalone coroutines as Tukio tasks.

It also provides a function `new_task()` that shall be used to schedule the
execution of Tukio tasks in an asyncio event loop.
"""
import asyncio
import logging
import inspect


log = logging.getLogger(__name__)


class UnknownTaskName(Exception):
    pass


# Inspired from https://github.com/faif/python-patterns/blob/master/registry.py
class TaskRegistry:

    """
    This class is designed to be a registry of all coroutines and classes that
    implement a Tukio task. Coroutines and classes are registered by name.
    Each record of the registry is a tuple (class, coro_func). If the
    registered implementation is a simple coroutine, class=None.
    """

    _registry = dict()
    _codes = dict()

    @classmethod
    def register(cls, coro_or_cls, task_name, coro_name=None):
        # A task name can be registered only once!
        if task_name in cls._registry:
            klass, coro_fn = cls._registry[task_name]
            raise ValueError("task name '{}' already registered with "
                             "({}, {})".format(task_name, klass, coro_fn))

        if inspect.isclass(coro_or_cls):
            klass, coro_fn = coro_or_cls, getattr(coro_or_cls, coro_name)
        else:
            klass, coro_fn = None, coro_or_cls
        if not asyncio.iscoroutinefunction(coro_fn):
            raise TypeError("{} is not a coroutine function".format(coro_fn))
        cls._registry[task_name] = klass, coro_fn
        cls._codes[coro_fn.__code__] = task_name

    @classmethod
    def get(cls, task_name):
        try:
            return cls._registry[task_name]
        except KeyError as exc:
            raise UnknownTaskName from exc

    @classmethod
    def all(cls):
        return cls._registry

    @classmethod
    def codes(cls):
        return cls._codes


def register(task_name, coro_name=None):
    """
    A decorator to register a standard coroutine or a task holder class as a
    Tukio task implementation.
    This decorator adds the class attribute `TASK_NAME` to task holder classes.
    """
    def decorator(coro_or_cls):
        TaskRegistry.register(coro_or_cls, task_name, coro_name)
        if inspect.isclass(coro_or_cls):
            coro_or_cls.TASK_NAME = task_name
        return coro_or_cls
    return decorator


def new_task(task_name, *, data=None, config=None, loop=None):
    """
    Schedules the execution of the coroutine registered as `task_name` (either
    defined in a task holder class or not) in the loop and returns an instance
    of `asyncio.Task()` (or a subclass of it).
    """
    klass, coro_fn = TaskRegistry.get(task_name)
    if klass:
        task_holder = klass(config)
        coro = coro_fn(task_holder, data)
    else:
        coro = coro_fn(data)
    return asyncio.ensure_future(coro, loop=loop)
