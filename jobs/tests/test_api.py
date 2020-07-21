from .utils import count

import asyncio
import asyncpg
import datetime
import jobs
import json
import pytest

pytestmark = pytest.mark.asyncio


async def test_migrations_are_working(db):
    mi = await db.fetchval("select migration from jobs.migrations")
    assert mi == 1


async def test_jobs_basic_operations(db):
    task = await jobs.publish(db, "atask")
    assert task is not None
    assert await count(db, "jobs.job_queue") == 1
    tasks = await jobs.consume(db, 1)
    assert len(tasks) == 1
    assert tasks[0]["task"] == task["task"]
    assert tasks[0]["job_id"] == task["job_id"]

    finished = await jobs.ack(db, task["job_id"])
    assert finished["status"] == "success"

    assert await count(db, "jobs.job_queue") == 0
    assert await count(db, "jobs.job") == 1


async def test_schedule_at(db):
    sec1 = datetime.datetime.utcnow() + datetime.timedelta(milliseconds=500)
    task = await jobs.publish(db, "atask", scheduled_at=sec1)
    cons = await jobs.consume(db, 1)
    assert len(cons) == 0
    await asyncio.sleep(0.5)
    cons = await jobs.consume(db, 1)
    assert len(cons) == 1
    assert task["job_id"] == cons[0]["job_id"]


async def test_retiry_failed_tasks(db):
    task = await jobs.publish(db, "task", max_retries=2)
    await jobs.consume(db, 1)
    await jobs.nack(
        db,
        task["job_id"],
        scheduled_at=(
            datetime.datetime.utcnow() + datetime.timedelta(milliseconds=500)
        ),
    )
    assert await count(db, "jobs.job_queue") == 1
    tasks = await jobs.consume(db, 1)
    assert len(tasks) == 0  # backoff
    await asyncio.sleep(0.5)
    [task2] = await jobs.consume(db, 1)
    assert task2["job_id"] == task["job_id"]
    assert task2["retries"] == 1
    await jobs.nack(db, task2["job_id"])
    assert await count(db, "jobs.job", condition="status = 'failed'") == 1


async def test_fails_to_ack_already_done_tasks(db):
    await jobs.publish(db, "task")
    [task] = await jobs.consume(db, 1)
    await jobs.ack(db, task["job_id"])
    with pytest.raises(asyncpg.exceptions.RaiseError):
        await jobs.ack(db, task["job_id"])


async def test_fails_to_nack_already_done_tasks(db):
    await jobs.publish(db, "task")
    [task] = await jobs.consume(db, 1)
    await jobs.ack(db, task["job_id"])
    with pytest.raises(asyncpg.exceptions.RaiseError):
        await jobs.nack(db, task["job_id"])


async def test_run_a_task(db):
    await jobs.publish(db, "jobs.tests.task.task", args=[1, 2])
    [task] = await jobs.consume(db, 1)
    result = await jobs.run(db, task, sync=True)
    task = await jobs.get(db, task["job_id"])
    assert result == 3


async def test_publish_bulk(db):
    tasks = [
        (
            "jobs.tests.task.task",
            json.dumps({"args": [item, item]}),
            None,
            None,
            None,
            3,
        )
        for item in range(0, 10)
    ]
    result = await jobs.publish_bulk(db, tasks)
    assert len(result) == 10
    assert await count(db, "jobs.job_queue") == 10
    [task] = await jobs.consume(db, 1)
    assert task["task"] == "jobs.tests.task.task"


async def test_view_expired_jobs(db):
    await jobs.publish(db, "task", timeout=0.1)
    # expired jobs should be consumed first
    await jobs.consume(db, 1)
    running = await db.fetchval("SELECT count(*) FROM jobs.running")
    assert running == 1
    await asyncio.sleep(0.5)
    amount = await db.fetchval("SELECT count(*) FROM jobs.expired")
    assert amount == 1


async def test_timeout_jobs(db):
    await jobs.publish(db, "task", timeout=0.1)
    [job] = await jobs.consume(db, 1)
    await asyncio.sleep(0.2)
    await jobs.consume(db, 1)
    with pytest.raises(asyncpg.exceptions.RaiseError):
        await jobs.ack(db, job["job_id"])


async def test_task_priorities(db):
    tasks = [
        (
            "jobs.tests.task.task",
            json.dumps({"args": [item, item]}),
            None,
            None,
            None,
            3,
        )
        for item in range(0, 3)
    ]
    await jobs.publish_bulk(db, tasks)
    task = await jobs.publish(db, "atask", priority=10)
    [t2] = await jobs.consume(db, 1)
    assert task["job_id"] == t2["job_id"]


async def test_consume_topics(db):
    t1 = await jobs.publish(db, "task.new.1")
    t2 = await jobs.publish(db, "task.old.3")
    tasks = await jobs.consume_topic(db, "task.%", 2)
    assert len(tasks) == 2

    r1 = {t["job_id"] for t in tasks}
    r2 = {t1["job_id"], t2["job_id"]}

    assert r1 == r2

    await jobs.ack(db, tasks[0]["job_id"])
    await jobs.ack(db, tasks[1]["job_id"])

    await jobs.publish(db, "task.new.1.x")
    no_tasks = await jobs.consume_topic(db, "xxxx")
    assert len(no_tasks) == 0

    t3 = await jobs.consume_topic(db, "task.new.%")
    assert len(t3) == 1
