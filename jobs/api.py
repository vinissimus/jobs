from .utils import resolve_dotted_name

import asyncpg
import datetime
import json
import typing


async def publish(
    db: asyncpg.Connection,
    task: str,
    *,
    body: typing.Any = None,
    args: typing.List[typing.Any] = None,
    kwargs: typing.Dict[str, typing.Any] = None,
    scheduled_at: datetime.datetime = None,
    timeout: float = 60,
    priority: int = None,
    max_retries: int = 3,
):
    """Publish a message.

    Arguments:
    db -- db connection (could be a transanction)
    task -- name of the task (usually method to resolve the task from the worker)

    Keyword arguments:
    args -- arguments to pass to the resolved python task (default=None)
    kwargs -- kwargs to paass to the resolved python task (default=None)
    schedulet_at -- time when the task should run (consider using utc dates)
    timeout -- max timeout in seconds for the task default = 60s
    priority -- priority on the queue
    max_retries -- maximum retries allowed for the task
    body -- use it instead of (args and kwargs) to just put something
        on the task queue.
        Using *args and **kwargs both get serialized using json on
          something like {"args": [], "kwargs":{}}
        If you use this mechanics, then the task body could be whatever you want.
    """
    if not body:
        body = json.dumps({"args": args, "kwargs": kwargs})
    # todo not sure if we should serialize to json body
    result = await db.fetchrow(
        "select * from jobs.publish($1, $2, $3, $4, $5, $6)",
        task,
        body,
        scheduled_at,
        timeout,
        priority,
        max_retries,
    )
    return result


async def publish_bulk(db: asyncpg.Connection, jobs):
    """Publish a batch of jobs:

    Arguments:
    db -- asyncpg.Connection
    jobs -- typing.List of tuples with
        [
            (
                'taskname',
                body: jsonb,
                scheduled_at: datetime default None,
                timeout: seconds to timeout the task, default None
                priority: for the assigned task
                max_retries: max retries before mark the task as failed
            )
        ]
    Returns:
        the list of created tasks

    """
    return await db.fetch(
        "SELECT * FROM jobs.publish_bulk($1::jobs.bulk_job[])", jobs
    )


async def consume(db: asyncpg.Connection, n: int = 1):
    return await db.fetch("SELECT * FROM jobs.consume($1)", n)


async def consume_topic(db: asyncpg.Connection, topic: str, n: int = 1):
    return await db.fetch("SELECT * FROM jobs.consume($1, $2)", topic, n)


async def ack(db: asyncpg.Connection, task_id: str, result=None):
    return await db.fetchrow("SELECT * FROM jobs.ack($1, $2)", task_id, result)


async def nack(
    db: asyncpg.Connection,
    task_id: str,
    error: str = None,
    scheduled_at: datetime.datetime = None,
):
    return await db.execute(
        "SELECT * FROM jobs.nack($1, $2, $3)", task_id, error, scheduled_at
    )


async def get(db: asyncpg.Connection, task_id):
    return await db.fetchrow("SELECT * from jobs.all where job_id=$1", task_id)


async def run(db: asyncpg.Connection, task, sync=False):
    result = None
    try:
        func = resolve_dotted_name(task["task"])
        params = json.loads(task["body"] or "{}")
        args = params.get("args") or []
        kwargs = params.get("kwargs") or {}
        result = await func(*args, **kwargs)
        if sync:
            await ack(db, task["job_id"], json.dumps(result))
    except Exception as e:
        if sync:
            await nack(db, task["job_id"])
        else:
            raise e
    return result
