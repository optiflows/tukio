"""
A task holder is a way to implement a Tukio task as a class and not only as a
simple standalone coroutine.
A task holder class must define a coroutine function that implements the Tukio
task.
To register a task holder class as a Tukio task, use the `@register()`
decorator:

    @register('my-holder', 'my_task_impl')
    class MyHolder:
        def __init__(self, config):
            # some code

        async def my_task_impl(self, event):
            # some code

Which turns into a more compact class when you inherit your own class from
`TaskHolder`:

    @register('my-holder', 'my_task_impl')
    class MyHolder(TaskHolder):
        async def my_task_impl(self, event):
            # some code

"""
from uuid import uuid4


class TaskHolder:

    """
    A base class that makes the implementation of task holders even easier.
    It is not mandatory to inherit your own task holder classes from this base
    class.
    The requirements of a task holder class are:
        1. the 1st argument passed to `__init__()` is the task's config.
        2. define a coroutine that implements the Tukio task
    If the task holder instance has a `uid` attribute it will be used by
    `TukioTask` as its own task ID (requires to use `tukio_factory` as the task
    factory).
    """

    __slots__ = ('config', 'uid', 'queue')

    TASK_NAME = None

    def __init__(self, config=None):
        self.config = config
        self.uid = str(uuid4())
        # The default task's receiving queue (an `asyncio.Queue` instance).
        # All events received during execution from topics the task subscribed
        # to will land into that queue.
        # This `queue` attribute is auto set by `TukioTask` at init.
        self.queue = None

    def report(self):
        """
        Returns an execution report (a dict). When defined, this method is used
        by the workflow execution engine to build the dict that represents an
        execution of a task.
        """

    async def teardown(self):
        """
        This method will be async called right after a Tukio task cancellation,
        and it is intended to perform any ultimate actions (like cleanups)
        before loosing the task reference.
        """
