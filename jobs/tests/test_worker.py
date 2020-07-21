from .utils import count
from jobs.migrations import migrate
from jobs.worker import Worker

import asyncio
import asyncpg
import jobs
import json
import pytest

pytestmark = pytest.mark.asyncio


async def test_worker(db_settings):
    host, port = db_settings
    dsn = f"postgresql://postgres@{host}:{port}/guillotina"
    worker = Worker(dsn, wait=0)
    # create schema db
    db = await asyncpg.connect(dsn)
    try:
        await migrate(db)
        job = await jobs.publish(db, "jobs.tests.task.task", args=[1, 3])
        future = asyncio.create_task(worker.work())
        await asyncio.sleep(1)
        task = await jobs.get(db, job["job_id"])
        assert task["status"] == "success"
        future.cancel()
        await db.execute("DROP schema jobs CASCADE;")
    finally:
        await db.close()


def create_jobs(amount):
    return [
        (
            "jobs.tests.task.task",
            json.dumps({"args": [num, num]}),
            None,
            None,
            None,
            None,
        )
        for num in range(0, amount)
    ]


async def test_a_bit_of_work(db_settings):
    host, port = db_settings
    dsn = f"postgresql://postgres@{host}:{port}/guillotina"
    AMOUNT = 300
    WORKERS = 10
    _workers = []
    for item in range(0, WORKERS):
        _workers.append(Worker(dsn, wait=0))
    # create schema db
    db = await asyncpg.connect(
        dsn, server_settings={"application_name": "test"}
    )
    await migrate(db)

    j_ = create_jobs(AMOUNT)
    assert len(j_) == AMOUNT
    await jobs.publish_bulk(db, j_)

    tasks = []
    for worker in _workers:
        tasks.append(asyncio.create_task(worker.work()))

    processed = 0
    while processed < AMOUNT:
        processed = await db.fetchval("select count(*) from jobs.job")
        await asyncio.sleep(1)
        print(f"processed: {processed}")
    assert await count(db, "jobs.job") == AMOUNT

    for t in _workers:
        t.close()
    await asyncio.gather(*tasks)

    # ensuere connections are closed
    cons = await db.fetchval("SELECT sum(numbackends) FROM pg_stat_database")
    # there sould be only two connections,
    # one for the main fixture: fixture.session
    # another one that we re using
    assert cons <= 3

    await db.execute("DROP schema jobs CASCADE;")
    await db.close()


async def test_server_closes_conn(db_settings):
    host, port = db_settings
    dsn = f"postgresql://postgres@{host}:{port}/guillotina"
    db = await asyncpg.connect(dsn)
    await migrate(db)

    j_ = create_jobs(10)
    await jobs.publish_bulk(db, j_)

    worker = Worker(dsn, wait=0)
    runner = asyncio.create_task(worker.work())
    await asyncio.sleep(1)
    pid = worker._con.get_server_pid()

    await db.execute("SELECT pg_terminate_backend($1)", pid)

    processed = 0
    while processed < 10:
        processed = await db.fetchval("select count(*) from jobs.job")
        await asyncio.sleep(1)

    assert await count(db, "jobs.job") == 10
    worker.close()
    await runner
    await db.execute("DROP schema jobs CASCADE;")
    await db.close()
