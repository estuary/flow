BEGIN;

-- Function to be triggered on new data_plane_migrations row
CREATE FUNCTION internal.trigger_create_data_plane_migration_task()
RETURNS TRIGGER AS $$
DECLARE
    new_task_id public.flowid;
    task_message JSON;
    -- This MUST match the i16 value of automations::task_types::DATA_PLANE_MIGRATION
    -- which we defined as TaskType(8) in flow/crates/automations/src/lib.rs
    data_plane_migration_task_type_id SMALLINT := 8;
BEGIN
    new_task_id := internal.id_generator();

    -- Construct the JSON message for MigrationTaskMessage::Initialize { migration_id: NEW.id }
    task_message := json_build_object(
        'initialize', json_build_object(
            'migration_id', NEW.id
        )
    );

    -- Use the value of automations::task_types::DATA_PLANE_MIGRATION
    -- which we defined as TaskType(8) in flow/crates/automations/src/lib.rs
    PERFORM internal.create_task(new_task_id, data_plane_migration_task_type_id, NULL);

    -- Sender task_id is zero flowid as this is a system-initiated message.
    PERFORM internal.send_to_task(new_task_id, '00:00:00:00:00:00:00:00'::public.flowid, task_message);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- Create the trigger to call the above function after a new row is inserted.
CREATE TRIGGER on_data_plane_migration_insert
AFTER INSERT ON public.data_plane_migrations
FOR EACH ROW
EXECUTE FUNCTION internal.trigger_create_data_plane_migration_task();

COMMIT;