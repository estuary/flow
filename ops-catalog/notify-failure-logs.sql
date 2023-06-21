INSERT INTO failures (shard, ts)
    SELECT
        $name,
        $ts
    WHERE $message = 'shard failed';

INSERT INTO related_log_lines (shard, ts, level, message)
    SELECT
        $name,
        $ts,
        $level,
        $message
    WHERE $message != 'shard failed'
    ON CONFLICT DO NOTHING;

-- Only keep around the last 10 log lines
DELETE FROM related_log_lines
WHERE (shard, ts) IN
    (
        SELECT shard, ts from related_log_lines
        WHERE shard = $name
        ORDER BY ts DESC
        LIMIT -1 OFFSET 10
    );

WITH return_values as (
    select
    $message,
    substr(COALESCE($fields->>'error','Missing `error` field, failure reason unknown.'),0,2500) as reason,
    $ts,
    $name as task_name,
    $shard$name as sub_name,
    $shard$kind as task_kind,
    substr($name,0,length($name)-length($shard$name)) as tenant_name,
    (
        SELECT json_group_array(json_object(
            'shard',shard,
            'ts',ts,
            'level',level,
            'message',message,
            'formatted_message',SUBSTR('[ts='||ts||' level='||level||']: '||message,0,2500)
        ))
        FROM related_log_lines
        WHERE shard=$name
        ORDER BY ts DESC
    ) as log_lines,
    (SELECT json_group_object(key, value) from json_each($fields) where key != 'error') as filtered_metadata
)
INSERT INTO
    failure_notifications (shard, ts)
SELECT
    $name,
    $ts
WHERE
    $ts IS NOT NULL AND
    (
        $message = 'shard failed'
        -- We actually _do_ want these because they represent useful failure signals
        -- But we don't intelligently track errors, so it's a bit spammy atm
        -- OR $message = 'connector failed'
    )
    -- No more than 1 notification messages every hour
    AND (
        select
            count(*)
        from
            failure_notifications
        where
            failure_notifications.shard = $name
            AND cast(julianday(failure_notifications.ts)*24 as int) = cast(julianday($ts)*24 as int)
    ) = 0
    -- No more than 3 notifications per day
    AND (
        select
            count(*)
        from
            failure_notifications
        where
            failure_notifications.shard = $name
            AND cast(julianday(failure_notifications.ts) as int) = cast(julianday($ts) as int)
    ) < 3
RETURNING
    $message,
    (select reason from return_values) as reason,
    $ts,
    (select task_name from return_values) as task_name,
    (select sub_name from return_values) as sub_name,
    (select task_kind from return_values) as task_kind,
    (select tenant_name from return_values) as tenant_name,
    (select log_lines from return_values) as log_lines,
    (
        select
            $message|| ': ' || return_values.task_kind || ' `' || $name || '`' || char(10) || 'Date: _' || Datetime($ts) || '_' || char(10) || '```' || char(10) || return_values.reason || '```' as slack_text
        from return_values
    ) as slack_text,
    (
        select JSON_GROUP_ARRAY(value)
        from JSON_EACH(
            JSON_ARRAY(
                JSON_OBJECT(
                    'type','header',
                    'text',JSON_OBJECT(
                        'type', 'plain_text',
                        'text', ':rotating_light: ' || $message
                    )
                ),
                JSON_OBJECT(
                    'type','section',
                    'text',JSON_OBJECT(
                        'type', 'mrkdwn',
                        'text', '*Task Name:* `' || $name || '`'
                    )
                ),
                JSON_OBJECT(
                    'type', 'context',
                    'elements', JSON_ARRAY(
                        JSON_OBJECT(
                            'text', '*'||datetime($ts)||'* | Kind: *'||return_values.task_kind||'*',
                            'type', 'mrkdwn'
                        )
                    )
                ),
                JSON_OBJECT('type', 'divider'),
                JSON_OBJECT(
                    'type','section',
                    'text',JSON_OBJECT(
                        'type', 'mrkdwn',
                        'text', '*Failure Reason:*'
                    )
                ),
                JSON_OBJECT(
                    'type','section',
                    'text',JSON_OBJECT(
                        'type', 'mrkdwn',
                        'text', '```'||return_values.reason||'```'
                    )
                ),
                CASE (select count(*) from json_each(return_values.log_lines))
                    WHEN 0 THEN NULL
                    ELSE JSON_OBJECT('type', 'divider')
                END,
                CASE (select count(*) from json_each(return_values.log_lines))
                    WHEN 0 THEN NULL
                    ELSE JSON_OBJECT(
                        'type','section',
                        'text',JSON_OBJECT(
                            'type', 'mrkdwn',
                            'text', '*Adjacent Log Messages:*'
                        )
                    )
                END,
                CASE (select count(*) from json_each(return_values.log_lines))
                    WHEN 0 THEN NULL
                    ELSE JSON_OBJECT(
                        'type','section',
                        'text',JSON_OBJECT(
                            'type', 'mrkdwn',
                            'text', '```'||(SELECT group_concat(value->>'formatted_message', CHAR(10)) from json_each(return_values.log_lines))||'```'
                            -- 'text', '```'||(SELECT group_concat(value->'message', CHAR(10)) from json_each(return_values.log_lines))||'```'
                        )
                    )
                END,
                CASE (select count(*) from json_each(return_values.filtered_metadata))
                    WHEN 0 THEN NULL
                    ELSE JSON_OBJECT('type', 'divider')
                END,
                CASE (select count(*) from json_each(return_values.filtered_metadata))
                    WHEN 0 THEN NULL
                    ELSE JSON_OBJECT(
                        'type','section',
                        'text',JSON_OBJECT(
                            'type', 'mrkdwn',
                            'text', '*Metadata:*'
                        )
                    )
                END,
                CASE (select count(*) from json_each(return_values.filtered_metadata))
                    WHEN 0 THEN NULL
                    ELSE
                        JSON_OBJECT(
                            'type',
                            'section',
                            'fields',
                            (
                                select
                                    JSON_GROUP_ARRAY(
                                        JSON_OBJECT(
                                            'type',
                                            'mrkdwn',
                                            'text',
                                            CASE
                                                WHEN length(value) < 10 THEN ('*' || key || '*: `' || value || '`')
                                                ELSE ('*' || key || '*: ```' || value || '```')
                                            END
                                        )
                                    )
                                from
                                    json_each(return_values.filtered_metadata)
                            )
                        )
                END
            )
        ), return_values
        WHERE type != 'null'
    ) as slack_blocks;