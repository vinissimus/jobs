## Jobs

A PL/PGSQL based work queue (Publisher/Consumer),
with a python asyncio/asyncpg api

**alpha software**

## Features

- Implements a two layer API:

    A postgresql layer: tasks can be published from PL/PGSQL functions, 
    or procedures. Also can be extended using triggers.

    A python layer (or any client with a postgresql driver). The default
    implementations is based on asyncio python, using the awesome
    asyncpg driver.

- It's compatible with postgrest. All procedures, and tables, are scoped
  on an owned postgresql schema, and can be exposed throught it, with postgrest

- Retry logic, schedule_at or timeout, are implemented on the
  publish method. A task, can be published, with a max_retries, param,
  or an especific timeout.

- Internally uses two tables `jobs.job_queue` the table where pending and
  running tasks are scheduled, and `jobs.job` the table where ended tasks,
  are moved (success or failures).

- By default, tasks are retyried three times, with backoff.

- Timeout jobs, are expired, tasks by default had a 60s tiemout.

- Tasks can be scheduled on the future, just provide a `scheduled_at` param.

- There are views to monitor queue stats: `jobs.all` (all tasks),
  `jobs.expired` and `jobs.running`

- Tasks could also be priorized, provide a priority number, greater priority,
  precedence over other tasks

- consumer_topic, allows to consume tasks with a * (*topic.element.%*)

- rudimentary benchs on my laptop showed that it can handle 1000 tasks/second, 
  but anyway it depends on your postgres instance.

- instead of a worker daemon, tasks could also be consumed from a cronjob, or
a regular python or a kubernetes job. (It could be used to parallelize k8 jobs)

## tradeofs

- All jobs had to be aknowledged positive or negative (ack/nack)
- 

## Use from postgresql

```sql
SELECT job_id FROM
    jobs.publish(
        i_task -- method or function to be executed,
        i_body::jsonb = null -- arguments passed to it (on python {args:[], kwargs:{}}),
        i_scheduled_at: timestamp = null, -- when the task should run
        i_timeout:numeric(7,2) -- timeout in seconds for the job
        i_priority:integer = null -- gretare number more priority
    )
```

On the worker side:

```sql
SELECT * from jobs.consume(
    num: integer -- number of desired jobs
);
```
returns a list of jobs to be processed, 

Or selective consume a topic:

```sql
SELECT * from jobs.consume_topic('topic.xxx.%', 10)
```

jobs are marked as processing,
and should be acnlowledged with:

```sql
SELECT FROM jobs.ack(job_id);
```

or to return a failed job.

```sql
SELECT FROM jobs.nack(job_id, traceback, i_schedule_at)
```

Also you can batch enqueue multiple jobs in a single request, using

```sql
SELECT * FROM jobs.publish_bulk(jobs.bulk_job[]);
```

where jobs.bulk_job is

```sql
create type jobs.bulk_job as (
    task varchar,
    body jsonb,
    scheduled_at timestamp,
    timeout integer,
    priority integer,
    max_retries integer
);
```

## Use from python

On this side, implementing a worker, should be something like

```python
    db = await asyncpg.connect(dsn)
    while True:
        jobs = await jobs.consume(db, 1)
        for job in jobs:
            try:
                await jobs.run(db, job["job_id"])
                await jobs.ack(job["job_id"])
            except Exception as e:
                await jobs.nack(job["job_id"], str(e))
        await asyncio.sleep(1)
```

On the publisher side, jobs could be enqueued from between a
postgresql transaction:

```python
db = await asyncpg.connect(dsn)
async with db.transaction():
    # do whatever is needed,
    # queue a task
    await jobs.publish("package.file.sum", args=[1,2])
```

## Installing the package

```bash

pip install pgjobs
jobs-migrate postgresql://user:password@localhost:5432/db

This will create the schema on the `jobs` postgresql schema

```

To run the worker,

```
jobs-worker postgresql://dsn
```

At the moment there are no too much things implemented there,
just a single threaded worker, that needs a bit more of love :)
If your application resides on a python package,
tasks like `yourpackage.file.method` will be runnable as is.

## Observavility and monitor

With psql, or exposing them throught postgresql_exporter

## TODO

- [ ] connect notifications, using pg_notify, when tasks are queued,
      are picked, are completed. With this in place, it's easy
      enought to write o WS to send notifications to connected customers.

- [ ] improve the worker to run every job on an asyncio task

- [ ] handle better exceptions on the python side

- [x] fix requirements file

- [ ] add github actions to run CI

- [ ] write better docs and some examples
