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

DROP TABLE IF EXISTS `{project}.{dataset}.map_execution_wave`;
DROP TABLE IF EXISTS `{project}.{dataset}.map_wave_details`;
DROP TABLE IF EXISTS `{project}.{dataset}.fact_datatype_metrics`;
DROP TABLE IF EXISTS `{project}.{dataset}.fact_wave_metrics`;
DROP TABLE IF EXISTS `{project}.{dataset}.fact_user_wave_metrics`;
DROP TABLE IF EXISTS `{project}.{dataset}.fact_user_overall_metrics`;
DROP TABLE IF EXISTS `{project}.{dataset}.fact_migration_errors`;
DROP TABLE IF EXISTS `{project}.{dataset}.fact_migration_timeline`;
DROP TABLE IF EXISTS `{project}.{dataset}.fact_top_errors`;
DROP TABLE IF EXISTS `{project}.{dataset}.snapshot_item_user_wave`;
DROP TABLE IF EXISTS `{project}.{dataset}.snapshot_user_wave`;
DROP TABLE IF EXISTS `{project}.{dataset}.snapshot_migration_errors`;
-- Base mapping table
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.map_execution_wave`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  execution_id STRING,
  is_latest_execution BOOLEAN)
  CLUSTER BY data_type;

-- Unique wave details
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.map_wave_details`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING)
  CLUSTER BY data_type, batch_name;

-- fact_datatype_metrics
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_datatype_metrics`(
  data_type STRING,
  total_users_migrated INT64,
  success_percentage FLOAT64)
  CLUSTER BY data_type;

-- fact_wave_metrics
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_wave_metrics`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  start_date TIMESTAMP,
  user_count INT64,
  completed_user_count INT64,
  successfully_migrated_items INT64,
  total_migrated_items INT64,
  migrated_mails_count INT64,
  migrated_calendars_count INT64,
  migrated_contacts_count INT64,
  migrated_files_count INT64,
  migrated_file_versions_count INT64,
  migrated_folders_count INT64,
  migrated_item_crawlers_count INT64,
  success_percentage FLOAT64,
  completion_percentage FLOAT64,
  avg_items_per_user FLOAT64,
  status STRING)
  CLUSTER BY data_type, batch_id;

-- fact_user_wave_metrics
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_user_wave_metrics`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  user_identifier STRING,
  total_items_migrated INT64,
  migrated_mails_count INT64,
  migrated_calendars_count INT64,
  migrated_contacts_count INT64,
  migrated_files_count INT64,
  migrated_file_versions_count INT64,
  migrated_folders_count INT64,
  migrated_item_crawlers_count INT64,
  failed_items INT64,
  crawl_failure_items INT64,
  total_items INT64,
  success_rate_percentage FLOAT64,
  status STRING)
  CLUSTER BY data_type, batch_id;

-- fact_user_overall_metrics
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_user_overall_metrics`(
  user_identifier STRING,
  data_type STRING,
  batch_filter STRING,
  total_items_migrated INT64,
  migrated_mails_count INT64,
  migrated_calendars_count INT64,
  migrated_contacts_count INT64,
  migrated_files_count INT64,
  migrated_file_versions_count INT64,
  migrated_folders_count INT64,
  migrated_item_crawlers_count INT64,
  total_items INT64,
  success_rate_percentage FLOAT64,
  status STRING)
  CLUSTER BY data_type;

-- fact_migration_errors
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_migration_errors`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  migration_error_code STRING,
  migration_error_title STRING,
  error_message STRING,
  user_identifier STRING,
  occurrence_count INT64)
  CLUSTER BY data_type, batch_id;

-- fact_top_errors
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_top_errors`(
  migration_error_title STRING,
  error_message STRING,
  user_identifier STRING,
  occurrence_count INT64)
  CLUSTER BY migration_error_title;

-- fact_migration_timeline
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_migration_timeline`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  migration_date DATE,
  items_migrated INT64)
  CLUSTER BY data_type, batch_id;

-- Persistent snapshot for item statuses per batch
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.snapshot_item_user_wave`(
  data_type STRING,
  batch_id STRING,
  user_identifier STRING,
  source_identifier STRING,
  event_name STRING,
  source_type STRING,
  event_status STRING,
  time_usec INT64,
  execution_id STRING)
  CLUSTER BY data_type, batch_id;

-- Persistent snapshot for user execution and completion state per batch
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.snapshot_user_wave`(
  data_type STRING,
  batch_id STRING,
  user_identifier STRING,
  start_time_usec INT64,
  is_completed BOOLEAN,
  latest_execution_id STRING,
  latest_execution_time_usec INT64)
  CLUSTER BY data_type, batch_id;

-- Persistent snapshot for unique migration error events
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.snapshot_migration_errors`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  migration_error_code STRING,
  migration_error_title STRING,
  error_message STRING,
  user_identifier STRING,
  execution_id STRING,
  time_usec INT64,
  event_uuid STRING)
  CLUSTER BY data_type, batch_id;