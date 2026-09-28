-- # Copyright 2026 Google LLC
-- #
-- # Licensed under the Apache License, Version 2.0 (the "License");
-- # you may not use this file except in compliance with the License.
-- # You may obtain a copy of the License at
-- #
-- #     https://www.apache.org/licenses/LICENSE-2.0
-- #
-- # Unless required by applicable law or agreed to in writing, software
-- # distributed under the License is distributed on an "AS IS" BASIS,
-- # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- # See the License for the specific language governing permissions and
-- # limitations under the License.

-- Step 1: Incremental Window Materialization ({lookback_days} DAY Lookback)
-- Represents the raw, filtered migration event logs pulled from the source table within the dynamic lookback window.
CREATE OR REPLACE TEMP TABLE incremental_logs
AS
SELECT
  time_usec,
  record_type,
  event_name,
  event_type,
  status,
  data_migration
FROM
  `{project}.{dataset}.activity`
WHERE
  record_type = 'data_migration'
  AND data_migration.migration_type = 'Exchange Online Migration'
  AND _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY);

-- Step 2: Latest Wave Names Extraction
-- Extracts the latest user-friendly name for each wave from both configuration setups and updates in the current window.
CREATE OR REPLACE TEMP TABLE latest_wave_names AS
SELECT
  REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)') AS wave_id,
  IFNULL(NULLIF(data_migration.target_identifier, ''), 'Default Import Batch') AS batch_name,
  time_usec
FROM incremental_logs
WHERE
  REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)') IS NOT NULL
  AND (
    (
      event_name = 'START_MIGRATION_SETUP'
    )
    OR (
      event_name = 'UPDATE_MIGRATION_SETTINGS'
      AND data_migration.target_identifier IS NOT NULL
      AND data_migration.target_identifier != ''
    )
  )
QUALIFY
  ROW_NUMBER()
    OVER (
      PARTITION BY REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)')
      ORDER BY time_usec DESC
    )
  = 1;

-- Step 3: Session Mapping Generation
-- Represents the mapping between raw migration execution IDs and their corresponding logical migration batches (waves).
CREATE OR REPLACE TEMP TABLE current_wave_map
AS
WITH
  StartEvents AS (
    SELECT
      data_migration.migration_type AS data_type,
      REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)') AS wave_id,
      data_migration.execution_id AS execution_id,
      ROW_NUMBER()
        OVER (
          PARTITION BY REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)')
          ORDER BY time_usec DESC
        )
        = 1 AS is_latest_execution
    FROM incremental_logs
    WHERE event_name = 'START_MIGRATION'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY data_migration.execution_id ORDER BY time_usec DESC) = 1
  ),
  HistoricalDetails AS (
    SELECT
      batch_id AS wave_id,
      batch_name
    FROM `{project}.{dataset}.map_wave_details`
  )
SELECT
  e.data_type,
  e.wave_id AS batch_id,
  COALESCE(s.batch_name, h.batch_name, 'Default Migration Batch') AS batch_name,
  CONCAT(COALESCE(s.batch_name, h.batch_name, 'Default Migration Batch'), " (ID: ", CAST(e.wave_id AS STRING), ")") AS batch_filter,
  e.execution_id,
  e.is_latest_execution
FROM StartEvents e
LEFT JOIN latest_wave_names s ON e.wave_id = s.wave_id
LEFT JOIN HistoricalDetails h ON e.wave_id = h.wave_id;

-- Step 4: Execute Sequenced MERGE Operations (Mapping Tables)

-- 1. Merge map_execution_wave
-- Represents the persistent mapping table linking every distinct execution_id to its parent wave/batch and tracking the latest attempt.
MERGE `{project}.{dataset}.map_execution_wave` t
USING current_wave_map s
ON
  t.execution_id = s.execution_id
    WHEN MATCHED
      THEN
        UPDATE
SET
  t.data_type = s.data_type,
  t.batch_id = s.batch_id,
  t.batch_name = s.batch_name,
  t.batch_filter = s.batch_filter,
  t.is_latest_execution = s.is_latest_execution
    WHEN NOT MATCHED
      THEN
        INSERT(data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution)
          VALUES(
            s.data_type,
            s.batch_id,
            s.batch_name,
            s.batch_filter,
            s.execution_id,
            s.is_latest_execution);

-- Ensure older historical executions are marked as inactive if a newer execution was processed today
UPDATE `{project}.{dataset}.map_execution_wave` t
SET is_latest_execution = FALSE
WHERE is_latest_execution = TRUE
  AND EXISTS (
    SELECT 1 FROM current_wave_map s
    WHERE s.batch_id = t.batch_id
      AND s.is_latest_execution = TRUE
      AND s.execution_id != t.execution_id
  );

-- Update all executions of a wave if the wave name has been updated/set in the lookback window
UPDATE `{project}.{dataset}.map_execution_wave` t
SET
  t.batch_name = s.batch_name,
  t.batch_filter = CONCAT(s.batch_name, " (ID: ", CAST(t.batch_id AS STRING), ")")
FROM latest_wave_names s
WHERE t.batch_id = s.wave_id;

-- 2. Merge map_wave_details
-- Represents the persistent registry of all unique migration batches, maintaining their latest friendly names and filter labels.
MERGE `{project}.{dataset}.map_wave_details` t
USING (
  SELECT DISTINCT
    data_type,
    batch_id,
    batch_name,
    batch_filter
  FROM
    `{project}.{dataset}.map_execution_wave`
) s
ON t.data_type = s.data_type AND t.batch_id = s.batch_id WHEN MATCHED THEN UPDATE
SET
  t.batch_name = s.batch_name,
  t.batch_filter = s.batch_filter
    WHEN NOT MATCHED
      THEN
        INSERT(data_type, batch_id, batch_name, batch_filter)
          VALUES(s.data_type, s.batch_id, s.batch_name, s.batch_filter);


-- Step 5: Active Entity Tracking for Incremental Filtering
-- Represents unique migration batches active in the current window, used to optimize downstream metric recalculations.
CREATE OR REPLACE TEMP TABLE active_batches AS
SELECT DISTINCT m.batch_id
FROM incremental_logs a
JOIN `{project}.{dataset}.map_execution_wave` m
  ON a.data_migration.execution_id = m.execution_id
UNION DISTINCT
SELECT DISTINCT wave_id AS batch_id
FROM latest_wave_names;

-- Represents unique migration platforms (data types) active in the current window.
CREATE OR REPLACE TEMP TABLE active_datatypes AS
SELECT DISTINCT m.data_type
FROM incremental_logs a
JOIN `{project}.{dataset}.map_execution_wave` m
  ON a.data_migration.execution_id = m.execution_id
UNION DISTINCT
SELECT DISTINCT t.data_type
FROM latest_wave_names s
JOIN `{project}.{dataset}.map_execution_wave` t ON s.wave_id = t.batch_id;

-- Represents unique user-platform combinations active in the current window.
CREATE OR REPLACE TEMP TABLE active_users_datatypes AS
SELECT DISTINCT m.data_type, a.data_migration.source_name AS user_identifier
FROM incremental_logs a
JOIN `{project}.{dataset}.map_execution_wave` m
  ON a.data_migration.execution_id = m.execution_id
WHERE a.data_migration.source_name IS NOT NULL;


-- Step 6: Update Persistent Snapshots

-- 1. Item-Level Lifetime Status Generation (Incremental user-specific wave state)
-- Represents the deduplicated list of individual migrated items from the current window, preserving only the single most definitive status per item.
CREATE OR REPLACE TEMP TABLE incremental_item_user_wave AS
WITH BaseItems AS (
  SELECT
    m.data_type,
    m.batch_id,
    a.data_migration.source_name AS user_identifier,
    a.data_migration.source_identifier,
    a.event_name,
    a.data_migration.source_type AS source_type,
    a.status.event_status,
    a.time_usec,
    a.data_migration.execution_id,
    GENERATE_UUID() AS row_uuid
  FROM incremental_logs a
  JOIN `{project}.{dataset}.map_execution_wave` m
    ON a.data_migration.execution_id = m.execution_id
  WHERE LOWER(a.event_type) = 'migration'
    AND a.data_migration.source_name IS NOT NULL
    AND (
      a.data_migration.source_type IN ('Exchange Online Calendar Event', 'Exchange Online Email Message', 'Exchange Online Contact')
      OR a.status.event_status = 'FAILED'
    )
),
RankedItems AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY batch_id, user_identifier, IF(IFNULL(source_identifier, '') = '', row_uuid, source_identifier)
      ORDER BY 
        CASE WHEN LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END ASC,
        time_usec DESC
    ) AS rn
  FROM BaseItems
)
SELECT * EXCEPT(rn, row_uuid) FROM RankedItems WHERE rn = 1;

-- Represents the definitive, persistent state of each individual migrated item per user per batch, overwriting transient failures when retries succeed.
MERGE `{project}.{dataset}.snapshot_item_user_wave` t
USING incremental_item_user_wave s
ON t.batch_id = s.batch_id 
  AND t.user_identifier = s.user_identifier 
  AND t.source_identifier = s.source_identifier
WHEN MATCHED AND (
  CASE WHEN LOWER(s.event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END 
  < CASE WHEN LOWER(t.event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END
  OR (
    CASE WHEN LOWER(s.event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END 
    = CASE WHEN LOWER(t.event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END
    AND s.time_usec > t.time_usec
  )
) THEN UPDATE SET
  t.event_name = s.event_name,
  t.source_type = s.source_type,
  t.event_status = s.event_status,
  t.time_usec = s.time_usec,
  t.execution_id = s.execution_id
WHEN NOT MATCHED THEN INSERT(data_type, batch_id, user_identifier, source_identifier, event_name, source_type, event_status, time_usec, execution_id)
VALUES(s.data_type, s.batch_id, s.user_identifier, s.source_identifier, s.event_name, s.source_type, s.event_status, s.time_usec, s.execution_id);


-- 2. Update User State Persistent Snapshots
-- Represents the persistent lifetime summary state of each user within a specific migration batch (wave), tracking earliest start times and final completion flags.
MERGE `{project}.{dataset}.snapshot_user_wave` t
USING (
  WITH UserBaseStats AS (
    SELECT 
      m.data_type, m.batch_id, a.data_migration.source_name AS user_identifier,
      MIN(a.time_usec) AS start_time_usec,
      ARRAY_AGG(a.data_migration.execution_id ORDER BY a.time_usec DESC)[OFFSET(0)] AS latest_execution_id,
      MAX(a.time_usec) AS latest_execution_time_usec
    FROM incremental_logs a
    JOIN `{project}.{dataset}.map_execution_wave` m ON a.data_migration.execution_id = m.execution_id
    WHERE a.data_migration.source_name IS NOT NULL
    GROUP BY m.data_type, m.batch_id, user_identifier
  ),
  UserExecCompletions AS (
    SELECT DISTINCT
      m.batch_id, a.data_migration.source_name AS user_identifier, a.data_migration.execution_id,
      TRUE AS is_completed
    FROM incremental_logs a
    JOIN `{project}.{dataset}.map_execution_wave` m ON a.data_migration.execution_id = m.execution_id
    WHERE a.event_name = 'USER_MIGRATION_COMPLETE' AND a.data_migration.source_name IS NOT NULL
  )
  SELECT 
    b.data_type, b.batch_id, b.user_identifier,
    b.start_time_usec,
    IFNULL(c.is_completed, FALSE) AS is_completed,
    b.latest_execution_id,
    b.latest_execution_time_usec
  FROM UserBaseStats b
  LEFT JOIN UserExecCompletions c 
    ON b.batch_id = c.batch_id AND b.user_identifier = c.user_identifier AND b.latest_execution_id = c.execution_id
) s
ON t.batch_id = s.batch_id AND t.user_identifier = s.user_identifier
WHEN MATCHED THEN UPDATE SET
  t.start_time_usec = LEAST(t.start_time_usec, s.start_time_usec),
  t.is_completed = IF(s.latest_execution_id != IFNULL(t.latest_execution_id, ''), s.is_completed, t.is_completed OR s.is_completed),
  t.latest_execution_id = IF(s.latest_execution_time_usec > IFNULL(t.latest_execution_time_usec, 0), s.latest_execution_id, t.latest_execution_id),
  t.latest_execution_time_usec = GREATEST(IFNULL(t.latest_execution_time_usec, 0), s.latest_execution_time_usec)
WHEN NOT MATCHED THEN INSERT(data_type, batch_id, user_identifier, start_time_usec, is_completed, latest_execution_id, latest_execution_time_usec)
VALUES(s.data_type, s.batch_id, s.user_identifier, s.start_time_usec, s.is_completed, s.latest_execution_id, s.latest_execution_time_usec);


-- 3. Update Migration Errors Persistent Snapshot
-- Represents the append-only historical ledger of all unique migration failure events, broad-catching failures across all executions for presentation reconciliation.
MERGE `{project}.{dataset}.snapshot_migration_errors` t
USING (
  SELECT
    m.data_type,
    m.batch_id,
    m.batch_name,
    m.batch_filter,
    a.data_migration.migration_error_code,
    a.data_migration.migration_error_title,
    a.status.error_message,
    a.data_migration.source_name AS user_identifier,
    a.data_migration.execution_id,
    a.time_usec,
    TO_HEX(MD5(CONCAT(CAST(a.time_usec AS STRING), "|", IFNULL(a.data_migration.migration_error_code, ""), "|", IFNULL(a.data_migration.source_name, "")))) AS event_uuid
  FROM incremental_logs a
  JOIN `{project}.{dataset}.map_execution_wave` m
    ON a.data_migration.execution_id = m.execution_id
  WHERE a.status.event_status = 'FAILED'
    AND a.data_migration.migration_error_code IS NOT NULL
    AND a.data_migration.migration_error_code != ''
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY m.batch_id, TO_HEX(MD5(CONCAT(CAST(a.time_usec AS STRING), "|", IFNULL(a.data_migration.migration_error_code, ""), "|", IFNULL(a.data_migration.source_name, ""))))
    ORDER BY a.time_usec DESC
  ) = 1
) s
ON t.batch_id = s.batch_id AND t.event_uuid = s.event_uuid
WHEN MATCHED THEN UPDATE SET
  t.batch_name = s.batch_name,
  t.batch_filter = s.batch_filter
WHEN NOT MATCHED THEN INSERT(data_type, batch_id, batch_name, batch_filter, migration_error_code, migration_error_title, error_message, user_identifier, execution_id, time_usec, event_uuid)
VALUES(s.data_type, s.batch_id, s.batch_name, s.batch_filter, s.migration_error_code, s.migration_error_title, s.error_message, s.user_identifier, s.execution_id, s.time_usec, s.event_uuid);



-- Step 7: Update Fact Tables

-- 1. Merge fact_datatype_metrics
-- Represents top-level global scorecard metrics aggregated at the overall migration platform layer, tracking total user volumes and consolidated global health.
MERGE `{project}.{dataset}.fact_datatype_metrics` t
USING (
  WITH UserCounts AS (
    SELECT
      data_type,
      COUNT(DISTINCT user_identifier) AS total_users_migrated
    FROM `{project}.{dataset}.snapshot_user_wave`
    WHERE data_type IN (SELECT data_type FROM active_datatypes)
    GROUP BY data_type
  ),
  OverallItems AS (
    SELECT
      data_type,
      event_name,
      source_type,
      event_status
    FROM `{project}.{dataset}.snapshot_item_user_wave`
    WHERE data_type IN (SELECT data_type FROM active_datatypes)
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY data_type, user_identifier, source_identifier 
      ORDER BY 
        CASE WHEN LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END ASC,
        time_usec DESC
    ) = 1
  ),
  ItemMetrics AS (
    SELECT
      data_type,
      COUNTIF(
        LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND event_name IN ('CREATE_FILE', 'CREATE_FOLDER', 'CREATE_GMAIL_MESSAGE', 'CREATE_CALENDAR_EVENT', 'CREATE_CONTACT')
      ) AS successfully_migrated_items,
      COUNTIF(event_status = 'FAILED' AND event_name NOT IN ('CRAWL_FAILURE')) AS failed_items
    FROM OverallItems
    GROUP BY data_type
  )
  SELECT
    u.data_type,
    u.total_users_migrated,
    IFNULL(i.successfully_migrated_items, 0) AS successfully_migrated_items,
    IFNULL(i.failed_items, 0) AS failed_items
  FROM UserCounts u
  LEFT JOIN ItemMetrics i ON u.data_type = i.data_type
) s
ON
  t.data_type = s.data_type
    WHEN MATCHED
      THEN
        UPDATE
SET
  t.total_users_migrated = s.total_users_migrated,
  t.success_percentage = IFNULL(
    SAFE_DIVIDE(
      s.successfully_migrated_items, s.successfully_migrated_items + IFNULL(s.failed_items, 0)),
    0)
    WHEN NOT MATCHED
      THEN
        INSERT(data_type, total_users_migrated, success_percentage)
          VALUES(
            s.data_type,
            s.total_users_migrated,
            IFNULL(
              SAFE_DIVIDE(
                s.successfully_migrated_items,
                s.successfully_migrated_items + IFNULL(s.failed_items, 0)),
              0));

-- 2. Merge fact_wave_metrics
-- Represents consolidated operational dashboard metrics specific to individual migration batches, summarizing user progress, item throughput, and wave completion health.
MERGE `{project}.{dataset}.fact_wave_metrics` t
USING (
  WITH WaveUserStats AS (
    SELECT
      data_type,
      batch_id,
      MIN(TIMESTAMP_MICROS(start_time_usec)) AS start_date,
      COUNT(DISTINCT user_identifier) AS user_count,
      COUNTIF(is_completed) AS completed_user_count
    FROM `{project}.{dataset}.snapshot_user_wave`
    WHERE batch_id IN (SELECT batch_id FROM active_batches)
      AND data_type = 'Exchange Online Migration'
    GROUP BY data_type, batch_id
  ),
  WaveBase AS (
    SELECT DISTINCT
      data_type,
      batch_id,
      batch_name,
      batch_filter
    FROM `{project}.{dataset}.map_execution_wave`
    WHERE batch_id IN (SELECT batch_id FROM active_batches)
      AND data_type = 'Exchange Online Migration'
  ),
  ItemAggs AS (
    SELECT
      i.data_type,
      i.batch_id,
      COUNTIF(
        LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND i.event_name IN ('CREATE_GMAIL_MESSAGE', 'CREATE_CALENDAR_EVENT', 'CREATE_CONTACT')
      ) AS successfully_migrated_items,
      COUNTIF(
        LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND i.event_name = 'CREATE_GMAIL_MESSAGE'
      ) AS migrated_mails_count,
      COUNTIF(
        LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND i.event_name = 'CREATE_CALENDAR_EVENT'
      ) AS migrated_calendars_count,
      COUNTIF(
        LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND i.event_name = 'CREATE_CONTACT'
      ) AS migrated_contacts_count,
      COUNTIF(i.event_status = 'FAILED' AND i.event_name NOT IN ('CRAWL_FAILURE')) AS failed_items,
      COUNTIF(
        i.event_name = 'CRAWL_FAILURE'
        AND EXISTS (
          SELECT 1 FROM `{project}.{dataset}.map_execution_wave` m
          WHERE m.batch_id = i.batch_id
            AND m.execution_id = i.execution_id
            AND m.is_latest_execution = TRUE
        )
      ) AS crawl_failure_items
    FROM `{project}.{dataset}.snapshot_item_user_wave` i
    JOIN `{project}.{dataset}.snapshot_user_wave` u ON i.batch_id = u.batch_id AND i.user_identifier = u.user_identifier AND i.data_type = u.data_type
    WHERE i.batch_id IN (SELECT batch_id FROM active_batches)
      AND i.data_type = 'Exchange Online Migration'
    GROUP BY i.data_type, i.batch_id
  )
  SELECT
    w.data_type,
    w.batch_id,
    w.batch_name,
    w.batch_filter,
    u.start_date,
    IFNULL(u.user_count, 0) AS user_count,
    IFNULL(u.completed_user_count, 0) AS completed_user_count,
    IFNULL(i.successfully_migrated_items, 0) AS successfully_migrated_items,
    IFNULL(i.successfully_migrated_items, 0) + IFNULL(i.failed_items, 0) AS total_migrated_items,
    IFNULL(i.migrated_mails_count, 0) AS migrated_mails_count,
    IFNULL(i.migrated_calendars_count, 0) AS migrated_calendars_count,
    IFNULL(i.migrated_contacts_count, 0) AS migrated_contacts_count,
    IFNULL(
      SAFE_DIVIDE(
        i.successfully_migrated_items,
        i.successfully_migrated_items + IFNULL(i.failed_items, 0)),
      0) AS success_percentage,
    IFNULL(SAFE_DIVIDE(u.completed_user_count, u.user_count), 0) AS completion_percentage,
    IFNULL(SAFE_DIVIDE(IFNULL(i.successfully_migrated_items, 0) + IFNULL(i.failed_items, 0), u.user_count), 0) AS avg_items_per_user,
    IF(u.user_count = IFNULL(u.completed_user_count, 0) AND u.user_count > 0, 'Completed', 'Running') AS status
  FROM WaveBase w
  LEFT JOIN WaveUserStats u ON w.batch_id = u.batch_id AND w.data_type = u.data_type
  LEFT JOIN ItemAggs i ON w.batch_id = i.batch_id AND w.data_type = i.data_type
) s
ON t.data_type = s.data_type AND t.batch_id = s.batch_id WHEN MATCHED THEN UPDATE
SET
  t.batch_name = s.batch_name,
  t.batch_filter = s.batch_filter,
  t.start_date = s.start_date,
  t.user_count = s.user_count,
  t.completed_user_count = s.completed_user_count,
  t.successfully_migrated_items = s.successfully_migrated_items,
  t.total_migrated_items = s.total_migrated_items,
  t.migrated_mails_count = s.migrated_mails_count,
  t.migrated_calendars_count = s.migrated_calendars_count,
  t.migrated_contacts_count = s.migrated_contacts_count,
  t.success_percentage = s.success_percentage,
  t.completion_percentage = s.completion_percentage,
  t.avg_items_per_user = s.avg_items_per_user,
  t.status = s.status
    WHEN NOT MATCHED
      THEN
        INSERT(
          data_type,
          batch_id,
          batch_name,
          batch_filter,
          start_date,
          user_count,
          completed_user_count,
          successfully_migrated_items,
          total_migrated_items,
          migrated_mails_count,
          migrated_calendars_count,
          migrated_contacts_count,
          success_percentage,
          completion_percentage,
          avg_items_per_user,
          status)
          VALUES(
            s.data_type,
            s.batch_id,
            s.batch_name,
            s.batch_filter,
            s.start_date,
            s.user_count,
            s.completed_user_count,
            s.successfully_migrated_items,
            s.total_migrated_items,
            s.migrated_mails_count,
            s.migrated_calendars_count,
            s.migrated_contacts_count,
            s.success_percentage,
            s.completion_percentage,
            s.avg_items_per_user,
            s.status);

-- 3. Merge fact_user_wave_metrics
-- Represents the granular operational scorecard profiling each individual user's progress within a specific batch.
MERGE `{project}.{dataset}.fact_user_wave_metrics` t
USING (
  WITH UserBase AS (
    SELECT
      u.data_type,
      u.batch_id,
      u.user_identifier,
      u.is_completed,
      ANY_VALUE(m.batch_name) AS batch_name,
      ANY_VALUE(m.batch_filter) AS batch_filter
    FROM `{project}.{dataset}.snapshot_user_wave` u
    LEFT JOIN `{project}.{dataset}.map_execution_wave` m
      ON u.batch_id = m.batch_id AND u.data_type = m.data_type
    WHERE u.batch_id IN (SELECT batch_id FROM active_batches)
      AND u.data_type = 'Exchange Online Migration'
    GROUP BY u.data_type, u.batch_id, u.user_identifier, u.is_completed
  ),
  ItemCounts AS (
    SELECT
      i.data_type,
      i.batch_id,
      i.user_identifier,
      COUNTIF(
        LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND i.event_name IN ('CREATE_GMAIL_MESSAGE', 'CREATE_CALENDAR_EVENT', 'CREATE_CONTACT')
      ) AS total_items_migrated,
      COUNTIF(
        LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND i.event_name = 'CREATE_GMAIL_MESSAGE'
      ) AS migrated_mails_count,
      COUNTIF(
        LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND i.event_name = 'CREATE_CALENDAR_EVENT'
      ) AS migrated_calendars_count,
      COUNTIF(
        LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND i.event_name = 'CREATE_CONTACT'
      ) AS migrated_contacts_count,
      COUNTIF(i.event_status = 'FAILED' AND i.event_name NOT IN ('CRAWL_FAILURE')) AS failed_items,
      COUNTIF(
        i.event_name = 'CRAWL_FAILURE'
        AND EXISTS (
          SELECT 1 FROM `{project}.{dataset}.map_execution_wave` m
          WHERE m.batch_id = i.batch_id
            AND m.execution_id = i.execution_id
            AND m.is_latest_execution = TRUE
        )
      ) AS crawl_failure_items
    FROM `{project}.{dataset}.snapshot_item_user_wave` i
    JOIN `{project}.{dataset}.snapshot_user_wave` u
      ON i.batch_id = u.batch_id AND i.user_identifier = u.user_identifier AND i.data_type = u.data_type
    WHERE i.batch_id IN (SELECT batch_id FROM active_batches)
      AND i.data_type = 'Exchange Online Migration'
    GROUP BY i.data_type, i.batch_id, i.user_identifier
  )
  SELECT
    b.data_type,
    b.batch_id,
    b.batch_name,
    b.batch_filter,
    b.user_identifier,
    IFNULL(i.total_items_migrated, 0) AS total_items_migrated,
    IFNULL(i.migrated_mails_count, 0) AS migrated_mails_count,
    IFNULL(i.migrated_calendars_count, 0) AS migrated_calendars_count,
    IFNULL(i.migrated_contacts_count, 0) AS migrated_contacts_count,
    IFNULL(i.failed_items, 0) AS failed_items,
    IFNULL(i.crawl_failure_items, 0) AS crawl_failure_items,
    IFNULL(i.total_items_migrated, 0) + IFNULL(i.failed_items, 0) AS total_items,
    IFNULL(
      SAFE_DIVIDE(
        i.total_items_migrated,
        i.total_items_migrated + IFNULL(i.failed_items, 0)),
      0) AS success_rate_percentage,
    CASE
      WHEN IFNULL(i.total_items_migrated, 0) = 0 AND (IFNULL(i.failed_items, 0) > 0 OR IFNULL(i.crawl_failure_items, 0) > 0) THEN 'Failed'
      WHEN b.is_completed THEN 'Completed'
      ELSE 'Running'
    END AS status
  FROM UserBase b
  LEFT JOIN ItemCounts i ON b.batch_id = i.batch_id AND b.user_identifier = i.user_identifier AND b.data_type = i.data_type
) s
ON
  t.data_type = s.data_type
  AND t.batch_id = s.batch_id
  AND t.user_identifier
    = s.user_identifier
      WHEN MATCHED
        THEN UPDATE
SET
  t.batch_name = s.batch_name,
  t.batch_filter = s.batch_filter,
  t.total_items_migrated = s.total_items_migrated,
  t.migrated_mails_count = s.migrated_mails_count,
  t.migrated_calendars_count = s.migrated_calendars_count,
  t.migrated_contacts_count = s.migrated_contacts_count,
  t.failed_items = s.failed_items,
  t.crawl_failure_items = s.crawl_failure_items,
  t.total_items = s.total_items,
  t.success_rate_percentage = s.success_rate_percentage,
  t.status = s.status
    WHEN NOT MATCHED
      THEN
        INSERT(
          data_type,
          batch_id,
          batch_name,
          batch_filter,
          user_identifier,
          total_items_migrated,
          migrated_mails_count,
          migrated_calendars_count,
          migrated_contacts_count,
          failed_items,
          crawl_failure_items,
          total_items,
          success_rate_percentage,
          status)
          VALUES(
            s.data_type,
            s.batch_id,
            s.batch_name,
            s.batch_filter,
            s.user_identifier,
            s.total_items_migrated,
            s.migrated_mails_count,
            s.migrated_calendars_count,
            s.migrated_contacts_count,
            s.failed_items,
            s.crawl_failure_items,
            s.total_items,
            s.success_rate_percentage,
            s.status);

-- 4. Merge fact_user_overall_metrics
-- Represents the definitive multi-batch scorecard tracking each user's consolidated lifetime progress per platform.
MERGE `{project}.{dataset}.fact_user_overall_metrics` t
USING (
  WITH UserBase AS (
    SELECT
      u.data_type,
      u.user_identifier,
      LOGICAL_AND(u.is_completed) AS is_completed,
      ANY_VALUE(m.batch_filter) AS batch_filter
    FROM `{project}.{dataset}.snapshot_user_wave` u
    LEFT JOIN `{project}.{dataset}.map_execution_wave` m
      ON u.data_type = m.data_type
    WHERE u.data_type IN (SELECT data_type FROM active_datatypes)
      AND u.data_type = 'Exchange Online Migration'
    GROUP BY u.data_type, u.user_identifier
  ),
  UserOverallItems AS (
    SELECT
      data_type,
      user_identifier,
      event_name,
      source_type,
      event_status
    FROM `{project}.{dataset}.snapshot_item_user_wave`
    WHERE data_type IN (SELECT data_type FROM active_datatypes)
      AND data_type = 'Exchange Online Migration'
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY data_type, user_identifier, source_identifier 
      ORDER BY 
        CASE WHEN LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END ASC,
        time_usec DESC
    ) = 1
  ),
  ItemCounts AS (
    SELECT
      data_type,
      user_identifier,
      COUNTIF(
        LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND event_name IN ('CREATE_GMAIL_MESSAGE', 'CREATE_CALENDAR_EVENT', 'CREATE_CONTACT')
      ) AS total_items_migrated,
      COUNTIF(
        LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND event_name = 'CREATE_GMAIL_MESSAGE'
      ) AS migrated_mails_count,
      COUNTIF(
        LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND event_name = 'CREATE_CALENDAR_EVENT'
      ) AS migrated_calendars_count,
      COUNTIF(
        LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')
        AND event_name = 'CREATE_CONTACT'
      ) AS migrated_contacts_count,
      COUNTIF(event_status = 'FAILED' AND event_name NOT IN ('CRAWL_FAILURE')) AS failed_items,
      COUNTIF(event_name = 'CRAWL_FAILURE') AS crawl_failure_items
    FROM UserOverallItems
    GROUP BY data_type, user_identifier
  )
  SELECT
    b.user_identifier,
    b.data_type,
    b.batch_filter,
    IFNULL(i.total_items_migrated, 0) AS total_items_migrated,
    IFNULL(i.migrated_mails_count, 0) AS migrated_mails_count,
    IFNULL(i.migrated_calendars_count, 0) AS migrated_calendars_count,
    IFNULL(i.migrated_contacts_count, 0) AS migrated_contacts_count,
    IFNULL(i.total_items_migrated, 0) + IFNULL(i.failed_items, 0) AS total_items,
    IFNULL(
      SAFE_DIVIDE(
        i.total_items_migrated,
        i.total_items_migrated + IFNULL(i.failed_items, 0)),
      0) AS success_rate_percentage,
    CASE
      WHEN IFNULL(i.total_items_migrated, 0) = 0 AND (IFNULL(i.failed_items, 0) > 0 OR IFNULL(i.crawl_failure_items, 0) > 0) THEN 'Failed'
      WHEN b.is_completed THEN 'Completed'
      ELSE 'Running'
    END AS status
  FROM UserBase b
  LEFT JOIN ItemCounts i ON b.data_type = i.data_type AND b.user_identifier = i.user_identifier
) s
ON t.data_type = s.data_type AND t.user_identifier = s.user_identifier WHEN MATCHED THEN UPDATE
SET
  t.batch_filter = s.batch_filter,
  t.total_items_migrated = s.total_items_migrated,
  t.migrated_mails_count = s.migrated_mails_count,
  t.migrated_calendars_count = s.migrated_calendars_count,
  t.migrated_contacts_count = s.migrated_contacts_count,
  t.total_items = s.total_items,
  t.success_rate_percentage = s.success_rate_percentage,
  t.status = s.status
    WHEN NOT MATCHED
      THEN
        INSERT(
          user_identifier,
          data_type,
          batch_filter,
          total_items_migrated,
          migrated_mails_count,
          migrated_calendars_count,
          migrated_contacts_count,
          total_items,
          success_rate_percentage,
          status)
          VALUES(
            s.user_identifier,
            s.data_type,
            s.batch_filter,
            s.total_items_migrated,
            s.migrated_mails_count,
            s.migrated_calendars_count,
            s.migrated_contacts_count,
            s.total_items,
            s.success_rate_percentage,
            s.status);

-- 5. Update fact_migration_errors
-- Represents the pre-aggregated blocker summary tracking specific error code frequencies per user and per batch.
DELETE FROM `{project}.{dataset}.fact_migration_errors`
WHERE batch_id IN (SELECT batch_id FROM active_batches);

INSERT INTO `{project}.{dataset}.fact_migration_errors` (
  data_type,
  batch_id,
  batch_name,
  batch_filter,
  migration_error_code,
  migration_error_title,
  error_message,
  user_identifier,
  occurrence_count
)
SELECT
  s.data_type,
  s.batch_id,
  ANY_VALUE(s.batch_name) AS batch_name,
  ANY_VALUE(s.batch_filter) AS batch_filter,
  s.migration_error_code,
  ANY_VALUE(s.migration_error_title) AS migration_error_title,
  s.error_message,
  s.user_identifier,
  COUNT(*) AS occurrence_count
FROM `{project}.{dataset}.snapshot_migration_errors` s
JOIN `{project}.{dataset}.map_execution_wave` m
  ON s.batch_id = m.batch_id 
  AND s.execution_id = m.execution_id
WHERE s.batch_id IN (SELECT batch_id FROM active_batches)
  AND m.is_latest_execution = TRUE
GROUP BY s.data_type, s.batch_id, s.migration_error_code, s.error_message, s.user_identifier;

-- 6. Merge fact_migration_timeline
-- Represents the daily throughput velocity mapping distinct successful items migrated per day over a rolling 1-year window.
MERGE `{project}.{dataset}.fact_migration_timeline` t
USING (
  WITH DailyUniqueSuccesses AS (
    SELECT
      m.data_type,
      m.batch_id,
      m.batch_name,
      m.batch_filter,
      DATE(TIMESTAMP_MICROS(a.time_usec)) AS migration_date,
      ROW_NUMBER() OVER (
        PARTITION BY m.batch_id, DATE(TIMESTAMP_MICROS(a.time_usec)), IF(IFNULL(a.data_migration.source_identifier, '') = '', GENERATE_UUID(), a.data_migration.source_identifier)
        ORDER BY a.time_usec DESC
      ) AS rn
    FROM
      incremental_logs a
    JOIN
      `{project}.{dataset}.map_execution_wave` m
      ON a.data_migration.execution_id = m.execution_id
    WHERE
      LOWER(a.event_type) = 'migration'
      AND LOWER(a.status.event_status) IN ('succeeded', 'succeeded_with_warnings')
      AND a.data_migration.source_type IN ('Exchange Online Calendar Event', 'Exchange Online Email Message', 'Exchange Online Contact')
      AND DATE(TIMESTAMP_MICROS(a.time_usec)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  )
  SELECT
    data_type,
    batch_id,
    batch_name,
    batch_filter,
    migration_date,
    COUNT(*) AS items_migrated
  FROM
    DailyUniqueSuccesses
  WHERE
    rn = 1
  GROUP BY
    data_type,
    batch_id,
    batch_name,
    batch_filter,
    migration_date
) s
ON
  t.data_type = s.data_type
  AND t.batch_id = s.batch_id
  AND t.migration_date = s.migration_date
WHEN MATCHED
THEN UPDATE
SET
  t.batch_name = s.batch_name,
  t.batch_filter = s.batch_filter,
  t.items_migrated = s.items_migrated
    WHEN NOT MATCHED
      THEN
        INSERT(data_type, batch_id, batch_name, batch_filter, migration_date, items_migrated)
          VALUES(
            s.data_type,
            s.batch_id,
            s.batch_name,
            s.batch_filter,
            s.migration_date,
            s.items_migrated);


-- 7. Update fact_top_errors
-- Represents the top 10 unique migration error reasons (based on title) for the latest executions across all batches, partitioned per data type.
DELETE FROM `{project}.{dataset}.fact_top_errors` WHERE TRUE;

INSERT INTO `{project}.{dataset}.fact_top_errors` (
  data_type,
  migration_error_title,
  error_message,
  user_identifier,
  occurrence_count
)
WITH RankedUniqueErrors AS (
  SELECT
    data_type,
    migration_error_title,
    error_message,
    user_identifier,
    SUM(occurrence_count) OVER (PARTITION BY data_type, migration_error_title) AS total_occurrence_count,
    ROW_NUMBER() OVER (
      PARTITION BY data_type, migration_error_title 
      ORDER BY occurrence_count DESC
    ) AS rn
  FROM `{project}.{dataset}.fact_migration_errors`
)
SELECT
  data_type,
  migration_error_title,
  error_message,
  user_identifier,
  total_occurrence_count AS occurrence_count
FROM RankedUniqueErrors
WHERE rn = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY data_type ORDER BY occurrence_count DESC) <= 10;