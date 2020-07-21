from jobs.migrations import migrate
from jobs.worker import process_run
from multiprocessing import Process

import asyncio
import asyncpg
import jobs
import json
import pytest

pytestmark = pytest.mark.asyncio


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


async def test_multiple_process_workers(db_settings):

    host, port = db_settings
    dsn = f"postgresql://postgres@{host}:{port}/guillotina"
    db = await asyncpg.connect(
        dsn, server_settings={"application_name": "test"}
    )

    async def task_creator(dsn, num=100000, wait=0.5, batch=1000):
        conn = await asyncpg.connect(
            dsn, server_settings={"application_name": "generator"}
        )
        total = 0
        while total < num:
            jobs_ = create_jobs(batch)
            await asyncio.sleep(wait)
            await jobs.publish_bulk(conn, jobs_)
            total += 1
            print("Added %s" % batch)

        print("task finished")

    # create schema db
    AMOUNT = 300
    DELAYED = 300
    PROCESS = 3
    db = await asyncpg.connect(dsn)
    try:
        await migrate(db)
        j_ = create_jobs(AMOUNT)
        assert len(j_) == AMOUNT
        await jobs.publish_bulk(db, j_)
        task = asyncio.create_task(task_creator(dsn, num=DELAYED, batch=100))

        process_ = []
        for _ in range(0, PROCESS):
            p = Process(
                target=process_run,
                args=(dsn,),
                kwargs={"wait": 0, "batch_size": 64, "num_workers": 3},
            )
            p.start()
            process_.append(p)

        processed = 0
        counter = 1
        # try to fuck it adding tasks while it's processing
        while processed < (AMOUNT + DELAYED):
            processed = await db.fetchval("select count(*) from jobs.job")
            running = await db.fetchval("select count(*) from jobs.running")
            pending = await db.fetchval(
                "select count(*) from jobs.job_queue where run_at IS NULL"
            )
            run = processed / counter
            counter += 1
            await asyncio.sleep(1)
            print(
                f"processed: {processed}, running: {running}, pending: {pending} --> {run} req/s"
            )

        for proc in process_:
            proc.kill()

        task.cancel()
        await db.execute("DROP schema jobs CASCADE")

    finally:
        await db.close()
