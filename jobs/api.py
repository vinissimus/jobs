from .utils import resolve_dotted_name
from traceback import format_exc

import asyncpg
import datetime
import json


async def publish(
    db: asyncpg.Connection,
    task: str,
    args=None,
    kwargs=None,
    scheduled_at=None,
    timeout: int = None,
    priority: int = None,
    max_retries: int = 3,
):
    arguments = json.dumps({"args": args, "kwargs": kwargs})
    result = await db.fetchrow(
        "select * from jobs.publish($1, $2, $3, $4, $5, $6)",
        task,
        arguments,
        scheduled_at,
        timeout,
        priority,
        max_retries,
    )
    return result


async def publish_bulk(db: asyncpg.Connection, jobs):
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
        params = json.loads(task["arguments"] or "{}")
        args = params.get("args") or []
        kwargs = params.get("kwargs") or {}
        result = await func(*args, **kwargs)
        if sync:
            await ack(db, task["job_id"], json.dumps(result))
    except Exception as e:
        print(e)
        res = await get(db, task["job_id"])
        print(dict(res))
        if sync:
            await nack(db, task["job_id"])
        else:
            raise e
    return result
