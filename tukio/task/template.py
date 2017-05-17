import logging
from uuid import uuid4

from .task import new_task
from tukio.event import Event
from tukio.utils import Listen


log = logging.getLogger(__name__)


class TaskTemplate:

    """
    The complete description of a Tukio task is made of its registered name and
    its configuration (a dict).
    A task template can be linked to an execution object (inherited from
    `asyncio.Task`) and provide execution report.
    """

    __slots__ = ('name', 'config', 'topics', 'uid')

    def __init__(self, name, uid=None, config=None, topics=[]):
        self.name = name
        self.config = config or {}
        self.topics = topics
        self.uid = uid or str(uuid4())

    @property
    def listen(self):
        return Listen.get(self.topics)

    def new_task(self, data, loop=None):
        """
        Create a new asyncio task from the current task template.
        """
        task = new_task(self.name, data=data, config=self.config, loop=loop)
        task._template = self
        task.inputs = data.data if isinstance(data, Event) else data
        return task

    @classmethod
    def from_dict(cls, task_dict):
        """
        Create a new task description object from the given dictionary.
        The dictionary takes the form of:
            {
                "id": <task-template-id>,
                "name": <registered-task-name>,
                "config": <config-dict>,
                "topics": {[<>]|null}
            }

        The parameters 'topics' and 'config' are both optional.
        See below the behavior of a task at runtime according to the value of
        'topics':
            {"topics": None}
            the task will receive ALL data disptached by the broker
            ** default behavior **

            {"topics": []}
            the task will receive NO data from the broker

            {"topics": ["blob", "foo"]}
            the task will receive data dispatched by the broker in topics
            "blob" and "foo" only
        """
        uid = task_dict.get('id')
        name = task_dict['name']
        config = task_dict.get('config', {})
        topics = task_dict.get('topics', [])
        return cls(name, uid=uid, config=config, topics=topics)

    def as_dict(self):
        """
        Builds a dictionary that represents the task template object. If the
        task template is linked to a task execution object, the dictionary
        contains the execution (stored at key 'exec').
        """
        task_dict = {"name": self.name, "id": self.uid}
        task_dict.update({"config": self.config, "topics": self.topics})
        return task_dict

    def __str__(self):
        return "<TaskTemplate name={}, uid={}>".format(self.name, self.uid)
