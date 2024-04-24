begin;

create table controller_jobs (
    catalog_name catalog_name not null,
    -- The type of controller, for example 'autoDiscover' or 'updateInferredSchema'
    controller text not null,
    -- Inactive jobs are never run, though their status may still be updated in response
    -- to publications. Makes it easy to disable things and resume later
    active boolean not null,
    -- Time of the next scheduled run, or null if no run is scheduled
    next_run timestamptz,
    -- Arbitrary JSON that's updated by the controller. Can be used as state for the controller,
    -- and also for communicating status to end users.
    status json not null,
    updated_at timestamptz not null,
    -- Always use the same logs_token for each controller, so the logs from all runs are in one place
    logs_token uuid not null default gen_random_uuid(),
    failures integer not null default 0,
    -- Errors executing the controller will be shown here
    error text,

    -- Controller jobs are generally always considered background jobs, but it seems nice to have the ability
    -- to manually trigger them, and have those manually triggered jobs jump to the top of the queue.
    background boolean not null default true,

    primary key (catalog_name, controller)
);


commit;
