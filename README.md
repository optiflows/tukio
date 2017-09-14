# Tukio

[![Circle CI](https://img.shields.io/circleci/project/optiflows/tukio/master.svg)](https://circleci.com/gh/optiflows/tukio)
[![pypi version](http://img.shields.io/pypi/v/tukio.svg)](https://pypi.python.org/pypi/tukio)
[![python versions](https://img.shields.io/pypi/pyversions/tukio.svg)](https://pypi.python.org/pypi/tukio/)

Tukio is an event-driven workflow engine library built around [asyncio](https://docs.python.org/3/library/asyncio.html). It provides classes
to describe, run and follow-up workflows.

## Concepts

The library is built around a few core concepts: an **engine** that runs
**workflows** made of **tasks** that receive **events** dispatched by a
**broker** in **topics**.

#### Task and asyncio loop task factory
A task in tukio is basically an asyncio task. But we subclassed `asyncio.Task`
to add attributes and wrap methods to make it easier to manager tasks within a
workflow.
As a consequence you need to use tukio's own task factory. Note that the engine
does the job for you.

#### Workflow
A workflow is basically a collection of tasks executed sequentially starting
from the root task.

#### Engine
It triggers workflows upon receiving new data and can control the execution of
hundreds of concurrent workflows.

#### Broker, events and topics
Tukio implements a simple broker that can disptach events in topics. A topic
is similar to a communication channel. You need to register a handler to
process events received in a given topic.
All data received by the engine is wrapped into an event that gathers the data
received and, optionnally, the topic it was dispatched to and the source of the
event (if the event comes from another workflow/task).

#### Templates vs Instances
Tukio provides classes to define tasks and workflows. Such objects are called
task/workflow _templates_ whereas the execution of workflows/tasks is done in
_instances_. An instance of workflow uses the description of the workflow - the
workflow template - to run the right tasks. A task may use a configuration -
from the task template - to run properly.

## Workflow configuration

The description of a workflow is pretty straighforward:
```python
{
    "title": "workflow #1",
    "policy": "abort-running",
    "topics": ["abort"],
    "timeout": 30,
    "tasks": [
        {"id": "f1", "name": "task1"},
        {"id": "f2", "name": "task2"},
        {"id": "f3", "name": "task3", "timeout": 10},
        {"id": "f4", "name": "task1"},
        {"id": "f5", "name": "task2"},
        {"id": "f6", "name": "task1", "config": {"value": 1}}
    ],
    "graph": {
        "f1": ["f2"],
        "f2": ["f3", "f4"],
        "f3": ["f5"],
        "f4": ["f6"],
        "f5": [],
        "f6": []
    }
}
```
The fields `title`, `policy` and `topics` are optional. The `graph` field is a
simple adjacency list that ties tasks together into a DAG (Directed Acyclic
Graph). You must ensure there's only a single root task, otherwise the engine
will raise an exception.

Upon reaching the specified `timeout` (optional, infinite if `None`), the workflow will be cancelled. If it is a task, the task will be cancelled and Tukio will move on to the next tasks.

## How to use it?

#### Register coroutines as tasks

First you need to code and register your own tasks
```python
from tukio.task import register

@register('my-task1')
async def task1(event):
    print('hello from task1: {}'.format(event.data))
    return 'data from task1'


@register('my-task2')
async def task2(event):
    print('hello from task2: {}'.format(event.data))
    return 'data from task2'
```

#### Load your workflow description into the engine

Let's assume your 1st workflow is the following:
```python
import tukio
wf1 = {
    "title": "workflow #1",
    "tasks": [
        {"id": "f1", "name": "my-task1"},
        {"id": "f2", "name": "my-task2"},
        {"id": "f3", "name": "my-task2"},
        {"id": "f4", "name": "my-task1"},
        {"id": "f5", "name": "my-task2"},
        {"id": "f6", "name": "my-task1"}
    ],
    "graph": {
        "f1": ["f2"],
        "f2": ["f3", "f4"],
        "f3": ["f5"],
        "f4": ["f6"],
        "f5": [],
        "f6": []
    }
}

wf_tmpl = tukio.WorkflowTemplate.from_dict(wf1)
```

Now, load it into the workflow engine:
```python
import asyncio

loop = asyncio.get_event_loop()
engine = tukio.Engine(loop=loop)
loop.run_until_complete(engine.load(wf_tmpl))
```

It's now time to run your 1st workflow:
```python
wflows = loop.run_until_complete(engine.data_received('apple'))
# Wait for the end of the workflows triggered
loop.run_until_complete(asyncio.wait(wflows))
```

You've just run your 1st workflow with tukio and should get an output like this:
```
hello from task1: apple
hello from task2: data from task1
hello from task1: data from task2
hello from task2: data from task2
hello from task1: data from task1
hello from task2: data from task2
```

## Contributing

We always welcome great ideas. If you want to hack on the library, a [guide](CONTRIBUTING.md) is dedicated to it and describes the various steps involved.
