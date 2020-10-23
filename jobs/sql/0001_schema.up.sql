

CREATE SCHEMA jobs;

create table jobs.migrations (
    migration integer
);

insert into jobs.migrations values (0);

create type
    jobs.job_status as enum ('success', 'failed');


create table jobs.job_queue (
    id serial primary key,
    job_id varchar(32) not null,
    task varchar not null,
    body jsonb,
    retries integer,
    max_retries integer default 3,
    priority integer default 1,
    timeout numeric(7, 2) default 60,
    created_at timestamp,
    run_at timestamp,
    scheduled_at timestamp
);

CREATE index idx_task_name ON jobs.job_queue(task);

alter table jobs.job_queue
    add constraint unique_jobqueue_job_id
    unique(job_id);

create or replace view jobs.expired as (
    SELECT *
    FROM jobs.job_queue
    WHERE
        run_at IS NOT NULL
        AND run_at + make_interval(secs=>timeout) < clock_timestamp()
);

create or replace view jobs.running as (
    SELECT * FROM jobs.job_queue
    WHERE run_at is NOT NULL
);


create table jobs.job (
    id integer primary key,
    job_id varchar(32) not null,
    task varchar not null,
    body jsonb,
    retries integer,
    max_retries integer default 3,
    priority integer default 1,
    timeout integer default 30000,
    status jobs.job_status not null,
    created_at timestamp,
    run_at timestamp,
    scheduled_at timestamp,
    complete_on timestamp,
    result jsonb,
    traceback text
);

alter table jobs.job
    add constraint unique_job_job_id
    unique(job_id);


create or replace view jobs.all as (
    SELECT
        id,
        job_id,
        task,
        body,
        retries,
        max_retries,
        CASE WHEN run_at is null THEN 'pending'
            ELSE 'running'
        END as status,
        priority,
        timeout,
        created_at,
        run_at,
        scheduled_at,
        null as complete_on,
        null as result,
        null as traceback
    FROM jobs.job_queue
        UNION
    SELECT
        id,
        job_id,
        task,
        body,
        retries,
        max_retries,
        status::varchar,
        priority, -- priority
        timeout,
        created_at,
        run_at,
        scheduled_at,
        complete_on,
        result,
        traceback
    FROM jobs.job
);



CREATE sequence jobs.job_number;

create or replace function jobs.publish(
    i_task varchar,
    i_body jsonb = null,
    i_scheduled_at timestamp = null,
    i_timeout numeric(7,2) =  60,
    i_priority integer = null,
    i_max_retries integer = 3
) returns jobs.job_queue as $$
DECLARE
    out jobs.job_queue;
BEGIN
    insert
        into jobs.job_queue
    values (
        default,
        md5(current_time::varchar || i_task || nextval('jobs.job_number')::varchar),
        i_task,
        i_body,
        0,
        i_max_retries,
        i_priority,
        i_timeout,
        clock_timestamp(),
        null,
        i_scheduled_at
    ) returning * INTO out;
    return out;
END;
$$ LANGUAGE plpgsql;

create type jobs.bulk_job as (
    task varchar,
    body jsonb,
    scheduled_at timestamp,
    timeout integer,
    priority integer,
    max_retries integer
);

create or replace function jobs.publish_bulk(jobs.bulk_job[])
returns setof jobs.job_queue as $$
declare
  r jobs.bulk_job;
  result jobs.job_queue;
BEGIN
    FOR r in select * from unnest($1) LOOP
        select * FROM jobs.publish(
            r.task,
            r.body,
            r.scheduled_at,
            r.timeout,
            r.priority,
            r.max_retries
        ) INTO result;
        RETURN NEXT result;
    END LOOP;
    return;
END;
$$ language plpgsql;


create or replace function jobs.ack(
    i_job varchar(32),
    i_result jsonb = null
) returns jobs.job as $$
DECLARE
  current jobs.job_queue;
  dest jobs.job;
BEGIN
    SELECT * from jobs.job_queue
        WHERE job_id=i_job
        AND run_at IS NOT NULL
        FOR UPDATE
        INTO current;
    -- ensure tasks existss
    IF current IS NULL THEN
        raise EXCEPTION 'inexistent_task %', i_job;
    END IF;

    raise INFO 'Current %', current;
    INSERT INTO jobs.job
        VALUES (
            current.id,
            current.job_id,
            current.task,
            current.body,
            current.retries,
            current.max_retries,
            current.priority,
            current.timeout,
            'success',
            current.created_at,
            current.run_at,
            current.scheduled_at,
            now(),
            i_result,
            null
        ) RETURNING * INTO dest;
    DELETE FROM jobs.job_queue
        WHERE job_id = i_job;
    RETURN dest;
END;
$$ LANGUAGE plpgsql;

create or replace function jobs.nack(
    i_job varchar(32),
    i_traceback text = null,
    i_scheduled_at timestamp = null,
    ensure_running boolean = true
) RETURNS void as $$
DECLARE
    current jobs.job_queue;
BEGIN

    IF ensure_running = true THEN
        SELECT * from jobs.job_queue
            WHERE job_id=i_job
            AND run_at IS NOT NULL
            FOR UPDATE INTO current;
    ELSE
        SELECT * from jobs.job_queue
            WHERE job_id=i_job
            FOR UPDATE INTO current;
    END IF;
    -- ensure tasks existss
    IF current IS NULL THEN
        raise EXCEPTION 'inexistent_task %', i_job;
    END IF;

    IF i_scheduled_at IS NULL THEN
        i_scheduled_at = clock_timestamp() + make_interval(secs=>3*(current.retries+1));
    END IF;

    IF (current.retries+1) >= current.max_retries THEN
        raise INFO 'max retries, remove job';
        INSERT INTO jobs.job
        VALUES (
            current.id,
            current.job_id,
            current.task,
            current.body,
            current.retries,
            current.max_retries,
            current.priority,
            current.timeout,
            'failed',
            current.created_at,
            current.run_at,
            current.scheduled_at,
            now(),
            null,
            i_traceback
        );
        DELETE FROM jobs.job_queue
            WHERE job_id = i_job;
    ELSE
        update
            jobs.job_queue
        set
            retries = retries+1,
            run_at = null,
            scheduled_at = i_scheduled_at
        where job_id=i_job;
    END IF;
END;
$$ language plpgsql;


create or replace function jobs.clean_timeout()
RETURNS void as $$
declare
    id varchar;
BEGIN
    raise INFO 'cleaning';
    FOR id in select job_id from jobs.expired LOOP
        raise INFO '  cleaned %', id;
        PERFORM jobs.nack(id, 'expired', ensure_running=>false);
    END LOOP;
END;
$$ LANGUAGE plpgsql;


create or replace function jobs.consume(num integer)
    returns SETOF jobs.job_queue as $$
BEGIN
    PERFORM jobs.clean_timeout();
    RETURN QUERY WITH tasks AS (
        SELECT *
            from jobs.job_queue
        WHERE
            (scheduled_at <= clock_timestamp() OR scheduled_at IS NULL)
            AND run_at is NULL
        ORDER BY priority desc NULLS LAST
        FOR UPDATE SKIP LOCKED
        limit num
    )
    UPDATE
        jobs.job_queue
    SET
        run_at=now()
    WHERE id IN (select id from tasks) RETURNING *;
END;
$$ LANGUAGE plpgsql;


create or replace function jobs.consume(topic varchar, num integer)
    RETURNS SETOF jobs.job_queue as $$
BEGIN
    PERFORM jobs.clean_timeout();
    RETURN QUERY WITH tasks AS (
        SELECT id
            from jobs.job_queue
        WHERE
            (scheduled_at <= clock_timestamp() OR scheduled_at IS NULL)
            AND run_at is NULL
            AND task like topic
        ORDER BY priority desc NULLS LAST
        FOR UPDATE SKIP LOCKED
        limit num
    ), update AS (
        UPDATE
            jobs.job_queue
        SET
            run_at=now()
        WHERE id IN (select id from tasks)
    ) SELECT * from jobs.job_queue WHERE id IN (
        SELECT id FROM tasks
    );
END;
$$ LANGUAGE plpgsql;
