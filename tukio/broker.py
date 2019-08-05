import asyncio
import logging
import itertools

from tukio.event import Event


log = logging.getLogger(__name__)


class _BrokerRegistry:

    """
    This class is designed to be a registry of all event brokers.
    """

    _registry = dict()

    @classmethod
    def get(cls, loop):
        """
        Get or create an event broker for the given event loop
        """
        try:
            broker = cls._registry[loop]
        except KeyError:
            broker = cls._registry[loop] = Broker(loop)
        return broker


EXEC_TOPIC = '__exec_topic__'


def workflow_exec_topics(uid):
    return (EXEC_TOPIC, '{}/{}'.format(EXEC_TOPIC, uid))


class Broker(object):

    """
    The workflow engine has a global event broker that gathers external events
    (e.g. from the network) and internal events (e.g. from tasks) and schedules
    the execution of registered handlers.
    A handler can be registered as global (receives all events) or per-topic
    (receives events only from its registered topics).
    If an event is disptached with a topic, all handlers registered with that
    topic will be executed as well as all global handlers. Else, only global
    handlers will be executed.

    Note: this class is not thread-safe. Its methods must be called from within
    the same thread as the thread running the event loop used by the workflow
    engine (including the broker)
    """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._global_handlers = set()
        self._topic_handlers = dict()

    def dispatch(self, data, topics=None, source=None):
        """
        Passes an event (aka the data) received to the right handlers.
        If topic is not None, the event is dispatched to handlers registered
        to that topic only + global handlers. Else it is dispatched to global
        handlers only.
        Even if a handler was registered multiple times in multiple topics, it
        will be executed only once per call to `dispatch()`.
        """
        # Always call registered global handlers
        handlers = self._global_handlers

        # Parse topics if iterable
        if isinstance(topics, str):
            topics = (topics,)
        if topics is not None:
            for topic in topics:
                try:
                    handlers = handlers | self._topic_handlers[topic]
                except KeyError:
                    pass

        # Schedule the execution of all registered handlers
        for handler in handlers:
            # Automatically wrap input data into an event object
            if isinstance(data, Event):
                event = Event(data, topic=data.topic, source=data.source)
            else:
                event = Event(data, topics, source)
            if asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(event), loop=self._loop)
            else:
                self._loop.call_soon(handler, event)

    def register(self, coro_or_cb, topic=None):
        """
        Registers a handler (i.e. a coroutine or a regular function/method) to
        be executed upon receiving new events.
        If topic is not None, the handler will receive all events dispatched in
        that topic only. Else it will receive all events dispatched by the
        broker (whatever the topic including None).
        A handler can be registered multiple times in different topics. But a
        handler can neither be registered as global when already per-topic nor
        registered as per-topic when already global.
        """
        if not callable(coro_or_cb):
            raise TypeError('{} is not a callable object'.format(coro_or_cb))

        # Register a global handler
        if topic is None:
            values = self._topic_handlers.values()
            topic_handlers = set(itertools.chain.from_iterable(values))
            if coro_or_cb in topic_handlers:
                raise ValueError('{} already registered with'
                                 ' topics'.format(coro_or_cb))
            self._global_handlers.add(coro_or_cb)
            log.debug('registered global handler: %s', coro_or_cb)
            return

        # Register a per-topic handler
        if coro_or_cb in self._global_handlers:
            raise ValueError('{} already registered as global'
                             ' handler'.format(coro_or_cb))
        try:
            self._topic_handlers[topic].add(coro_or_cb)
        except KeyError:
            self._topic_handlers[topic] = {coro_or_cb}
        log.debug('registered topic handler: %s', coro_or_cb)

    def unregister(self, coro_or_cb, topic=None):
        """
        Unregisters a per-topic or a global handler. If there's no handler left
        for that topic, just remove it from the registered topics.
        """
        if topic is None:
            self._global_handlers.remove(coro_or_cb)
        else:
            self._topic_handlers[topic].remove(coro_or_cb)
            if not self._topic_handlers[topic]:
                del self._topic_handlers[topic]


def get_broker(loop=None):
    """
    Returns the broker used in the event loop for the current context.
    There's only a single broker per event loop.
    """
    loop = loop or asyncio.get_event_loop()
    return _BrokerRegistry.get(loop)


class TopicManager:

    """
    A context manager to subscribe and unsubscribe to topics while executing
    a task.
    """

    __slots__ = ('_loop', '_topic')

    def __init__(self, topic, *, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._topic = topic

    @classmethod
    def subscribe(cls, topic=None):
        """
        A shorthand method to register the standard `TukioTask.data_received`
        coroutine with a new topic in the data broker.
        This is an easy way to get events from a new topic into the receiving
        queue.
        """
        # Override topic and loop with the values passed at init when used as a
        # regular instance method.
        loop = cls._loop if isinstance(cls, TopicManager) else None
        if isinstance(cls, TopicManager):
            topic = cls._topic
        coro = asyncio.Task.current_task(loop=loop).data_received
        get_broker(loop=loop).register(coro, topic=topic)

    def __enter__(self):
        self.subscribe(self._topic)

    @classmethod
    def unsubscribe(cls, topic=None):
        """
        A shorthand method to unregister the standard `TukioTask.data_received`
        coroutine associated with the given topic in the data broker.
        Once unsubscribed, no new event from that topic will be put into the
        receiving queue.
        """
        # Override topic and loop with the values passed at init when used as a
        # regular instance method.
        loop = cls._loop if isinstance(cls, TopicManager) else None
        if isinstance(cls, TopicManager):
            topic = cls._topic
        coro = asyncio.Task.current_task(loop=loop).data_received
        get_broker(loop=loop).unregister(coro, topic=topic)

    def __exit__(self, *exc):
        self.unsubscribe(self._topic)
