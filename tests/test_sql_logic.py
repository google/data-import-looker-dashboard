import os
import unittest
import logging
from datetime import datetime, timedelta, timezone

# Try loading DuckDB, SQLGlot, and pytz for local in-memory SQL testing
try:
    import duckdb
    import sqlglot
    import pytz
    HAS_LOCAL_TESTING_LIBS = True
except ImportError:
    HAS_LOCAL_TESTING_LIBS = False


class TestSQLLogicLocal(unittest.TestCase):
    """
    Offline Unit Testing for SQL Files using DuckDB + SQLGlot
    ---------------------------------------------------------
    This suite parses and translates BigQuery SQL files into DuckDB SQL dialect,
    populates mock data, runs the pipeline, and asserts output metrics.
    """
    
    def setUp(self):
        if not HAS_LOCAL_TESTING_LIBS:
            self.skipTest("Missing local testing packages. Run: pip install duckdb sqlglot pytz")
        
        # Suppress harmless warnings from sqlglot about BQ features not supported by DuckDB (like CLUSTER BY)
        logging.getLogger('sqlglot').setLevel(logging.ERROR)
        
        # Initialize an in-memory DuckDB database connection
        self.conn = duckdb.connect(database=":memory:")
        
        # Resolve workspace directory and file paths
        self.workspace_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.ddl_path = os.path.join(self.workspace_dir, "ddl.sql")
        self.dml_path = os.path.join(self.workspace_dir, "dml.sql")

        # Create source table 'activity' matching BigQuery schema structure
        self.conn.execute("""
            CREATE TABLE activity (
                time_usec BIGINT,
                record_type VARCHAR,
                event_name VARCHAR,
                event_type VARCHAR,
                status STRUCT(event_status VARCHAR, error_message VARCHAR),
                data_migration STRUCT(
                    migration_type VARCHAR, 
                    target_uri VARCHAR, 
                    target_identifier VARCHAR, 
                    execution_id VARCHAR,
                    source_name VARCHAR,
                    source_uri VARCHAR,
                    source_identifier VARCHAR,
                    source_type VARCHAR,
                    migration_error_code VARCHAR,
                    migration_error_title VARCHAR
                ),
                _PARTITIONTIME TIMESTAMP
            );
        """)

    def tearDown(self):
        if HAS_LOCAL_TESTING_LIBS:
            self.conn.close()

    def run_sql_file(self, file_path, replacements=None):
        """Helper to read, replace variables, transpile, and execute SQL statements in DuckDB."""
        with open(file_path, "r") as f:
            sql_content = f.read()

        # Clean project/dataset prefixes so they execute in the local namespace
        sql_content = sql_content.replace("{project}.{dataset}.", "")
        # Default: no OneDrive/SharePoint cutoff, unless a test passes its own value.
        replacements = {"{od_sp_cutoff}": "1970-01-01 00:00:00+00", **(replacements or {})}
        if replacements:
            for k, v in replacements.items():
                sql_content = sql_content.replace(k, v)

        # Parse and execute statement by statement
        statements = sqlglot.parse(sql_content, read="bigquery")
        for statement in statements:
            if not statement:
                continue
            
            # AST Optimization: strip qualifiers from LHS of Update SET clauses
            for update_node in statement.find_all(sqlglot.exp.Update):
                for eq_node in update_node.find_all(sqlglot.exp.EQ):
                    lhs = eq_node.left
                    if isinstance(lhs, sqlglot.exp.Column):
                        lhs.set('table', None)

            duck_sql = statement.sql(dialect="duckdb")
            self.conn.execute(duck_sql)

    def test_full_pipeline_logic(self):
        """Test that log events are correctly mapped, processed, and aggregated in fact tables."""
        # 1. Run DDL script to generate target tables
        self.run_sql_file(self.ddl_path)

        # 2. Insert mock log data for a migration run
        # User: user1@example.com
        # Batch: Wave-X (ID: 101)
        # 3 succeeded items, 1 failed item, 1 completion event
        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # Populate source activity logs
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Setup event
            ({now_usec - 100000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': 'Wave-X Batch', 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Start event
            ({now_usec - 90000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated Item 1 (Email - Success)
            ({now_usec - 80000}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user1@example.com', 'source_identifier': 'msg-1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated Item 2 (Calendar - Success)
            ({now_usec - 70000}, 'data_migration', 'CREATE_CALENDAR_EVENT', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user1@example.com', 'source_identifier': 'cal-1', 'source_type': 'Exchange Online Calendar Event', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated Item 3 (Contact - Success)
            ({now_usec - 60000}, 'data_migration', 'CREATE_CONTACT', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user1@example.com', 'source_identifier': 'con-1', 'source_type': 'Exchange Online Contact', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated Item 4 (Email - Failed)
            ({now_usec - 50000}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Server timeout'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user1@example.com', 'source_identifier': 'msg-2', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'TIMEOUT', 'migration_error_title': 'Request Timeout'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- User complete event
            ({now_usec - 40000}, 'data_migration', 'USER_MIGRATION_COMPLETE', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user1@example.com', 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # 3. Run the DML SQL script
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # 4. Assertions on mapped/deduplicated session registry
        map_wave_rows = self.conn.execute("SELECT * FROM map_wave_details").fetchall()
        self.assertEqual(len(map_wave_rows), 1)
        self.assertEqual(map_wave_rows[0][0], "Exchange Online Migration")
        self.assertEqual(map_wave_rows[0][1], "101")
        self.assertEqual(map_wave_rows[0][2], "Wave-X Batch")

        # 5. Assertions on fact table metrics
        wave_metrics = self.conn.execute("SELECT * FROM fact_wave_metrics").fetchall()
        self.assertEqual(len(wave_metrics), 1)
        
        # Unpack result row (matches fact_wave_metrics schema)
        data_type, batch_id, batch_name, filter_lbl, start_date, user_cnt, comp_user, succ_items, tot_items, email_cnt, cal_cnt, con_cnt, succ_pct, comp_pct, avg_items, status = wave_metrics[0]
        
        self.assertEqual(user_cnt, 1)
        self.assertEqual(comp_user, 1)
        self.assertEqual(succ_items, 3) # Email, Calendar, Contact succeeded
        self.assertEqual(tot_items, 4) # 3 succeeded + 1 failed
        self.assertEqual(email_cnt, 1)
        self.assertEqual(cal_cnt, 1)
        self.assertEqual(con_cnt, 1)
        self.assertEqual(succ_pct, 0.75) # 3 / 4
        self.assertEqual(comp_pct, 1.0) # 1 / 1 user completed
        self.assertEqual(status, "Completed")

        # 6. Assertions on fact user metrics
        user_metrics = self.conn.execute("SELECT * FROM fact_user_wave_metrics").fetchall()
        self.assertEqual(len(user_metrics), 1)
        self.assertEqual(user_metrics[0][4], "user1@example.com") # user_identifier
        self.assertEqual(user_metrics[0][13], "Completed") # status

    def test_incremental_lookback_filtering(self):
        """Test that event logs outside of the lookback window are correctly excluded."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        
        # Calculate timestamp outside lookback window (e.g. 5 days ago)
        old_ts = now_ts - timedelta(days=5)
        old_usec = int(old_ts.timestamp() * 1_000_000)

        # Setup event outside of 2-day lookback window
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({old_usec}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 999', 'target_identifier': 'Old Batch', 'execution_id': 'exec-old', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{old_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run the DML SQL script with 2-day lookback
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Check map_wave_details: since setup was 5 days ago, it should NOT be processed
        map_wave_rows = self.conn.execute("SELECT * FROM map_wave_details").fetchall()
        self.assertEqual(len(map_wave_rows), 0, "Event outside lookback window was incorrectly processed!")

    def test_batch_name_not_exists(self):
        """Test that a missing setup event causes the batch name to default to 'Default Migration Batch'."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # Insert ONLY a START_MIGRATION event (no setup event)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-99', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify that it uses the default name
        map_wave_rows = self.conn.execute("SELECT * FROM map_wave_details").fetchall()
        self.assertEqual(len(map_wave_rows), 1)
        self.assertEqual(map_wave_rows[0][2], "Default Migration Batch")

    def test_multiple_executions_in_wave(self):
        """Test that having multiple executions in a wave properly marks only the latest execution as active."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup wave with a setup event
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 1000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': 'Wave-1 Batch', 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # 2. Run DML first time with execution 'exec-1'
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 500}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Check exec-1 is latest
        exec_rows = self.conn.execute("SELECT execution_id, is_latest_execution FROM map_execution_wave").fetchall()
        self.assertEqual(exec_rows, [("exec-1", True)])

        # Clean/truncate activity for the second run simulator
        self.conn.execute("DELETE FROM activity;")

        # 3. Simulate a second execution run 'exec-2'
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-2', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # 4. Verify exec-1 is marked False and exec-2 is marked True
        exec_rows = self.conn.execute("SELECT execution_id, is_latest_execution FROM map_execution_wave ORDER BY execution_id").fetchall()
        self.assertEqual(exec_rows, [("exec-1", False), ("exec-2", True)])

    def test_batch_filter_creation(self):
        """Test that batch_filter string combines batch name and wave ID correctly."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 100}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-ABC', 'target_identifier': 'Alpha Batch', 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-ABC', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify batch_filter
        map_wave_rows = self.conn.execute("SELECT batch_filter FROM map_wave_details").fetchall()
        self.assertEqual(map_wave_rows[0][0], "Alpha Batch (ID: Wave-ABC)")

    def test_old_wave_snapshot_interaction(self):
        """Test that late/new logs for a wave older than 30 days correctly update metrics by using persistent snapshots."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Manually populate persistent mapping and snapshot tables for an OLD wave (35 days ago)
        old_time_usec = now_usec - int(timedelta(days=35).total_seconds() * 1_000_000)
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Old', 'Old Wave Batch', 'Old Wave Batch (ID: Wave-Old)', 'exec-old', TRUE);
            
            INSERT INTO map_wave_details VALUES 
            ('Exchange Online Migration', 'Wave-Old', 'Old Wave Batch', 'Old Wave Batch (ID: Wave-Old)');

            INSERT INTO snapshot_item_user_wave VALUES 
            ('Exchange Online Migration', 'Wave-Old', 'user-old@example.com', 'msg-old', 'CREATE_GMAIL_MESSAGE', 'Exchange Online Email Message', 'SUCCEEDED', {old_time_usec}, 'exec-old');

            INSERT INTO snapshot_user_wave (data_type, batch_id, user_identifier, start_time_usec, is_completed, latest_execution_id, latest_execution_time_usec) VALUES 
            ('Exchange Online Migration', 'Wave-Old', 'user-old@example.com', {old_time_usec}, FALSE, 'exec-old', {old_time_usec});
        """)

        # 2. Insert a new event TODAY (within the 2-day lookback window) indicating the old user has completed
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'USER_MIGRATION_COMPLETE', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Old', 'target_identifier': NULL, 'execution_id': 'exec-old', 'source_name': 'user-old@example.com', 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # 3. Run the DML script with 2-day lookback (only processes today's complete event incrementally)
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # 4. Verify that the fact table contains correct combined metrics for the old wave
        wave_metrics = self.conn.execute("SELECT user_count, completed_user_count, successfully_migrated_items, total_migrated_items, status FROM fact_wave_metrics WHERE batch_id='Wave-Old'").fetchall()
        self.assertEqual(len(wave_metrics), 1)
        
        user_cnt, comp_user, succ_items, tot_items, status = wave_metrics[0]
        self.assertEqual(user_cnt, 1)
        self.assertEqual(comp_user, 1)      # Now marked as completed! (from today's event)
        self.assertEqual(succ_items, 1)     # Kept the 1 email succeeded! (from the 35-day-old snapshot)
        self.assertEqual(tot_items, 1)      # Total item count is correct
        self.assertEqual(status, "Completed")

    def test_success_and_failure_event_calculations(self):
        """Test exact categorization of successful items vs failures in metrics computation."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup mapping
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Metrics', 'Metrics Batch', 'Metrics (ID: Wave-Metrics)', 'exec-1', TRUE);
        """)

        # 2. Insert various items:
        # - 1 Email Success (SUCCEEDED)
        # - 1 Calendar Success with Warning (SUCCEEDED_WITH_WARNINGS)
        # - 1 Failed event (FAILED)
        # - 1 Crawl Failure (event_name=CRAWL_FAILURE) -> should be filtered out from metrics
        # - 1 Success with wrong event name -> should be filtered out from metrics
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Email Success
            ({now_usec - 50}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Metrics', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user@example.com', 'source_identifier': 'i1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Calendar Warning
            ({now_usec - 40}, 'data_migration', 'CREATE_CALENDAR_EVENT', 'migration', {{'event_status': 'SUCCEEDED_WITH_WARNINGS', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Metrics', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user@example.com', 'source_identifier': 'i2', 'source_type': 'Exchange Online Calendar Event', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            -- Failed Event
            ({now_usec - 30}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Fatal Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Metrics', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user@example.com', 'source_identifier': 'i3', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'ERR-100', 'migration_error_title': 'Fatal Error'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            -- Crawl Failure
            ({now_usec - 20}, 'data_migration', 'CRAWL_FAILURE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Failed to read directory'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Metrics', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user@example.com', 'source_identifier': 'i4', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'CRAWL_ERR', 'migration_error_title': 'Crawl failed'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Success with wrong event_name (should be filtered out from item metrics)
            ({now_usec - 10}, 'data_migration', 'MAILBOX_FOLDER_SETTINGS', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Metrics', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user@example.com', 'source_identifier': 'i5', 'source_type': 'Exchange Online Mailbox Folder Settings', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Assert results in fact table
        wave_metrics = self.conn.execute("SELECT successfully_migrated_items, total_migrated_items FROM fact_wave_metrics WHERE batch_id='Wave-Metrics'").fetchall()
        self.assertEqual(len(wave_metrics), 1)
        succ_items, tot_items = wave_metrics[0]
        
        # succ_items: Email (success) + Calendar (warning) = 2
        self.assertEqual(succ_items, 2)
        # tot_items: Email (success) + Calendar (warning) + Failed = 3
        self.assertEqual(tot_items, 3)

    def test_user_state_persistent_snapshot(self):
        """Test updates of start_time_usec and latest_execution_id inside snapshot_user_wave."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Run DML first time with early execution 'exec-early'
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-User', 'User Batch', 'User (ID: Wave-User)', 'exec-early', TRUE);

            INSERT INTO activity VALUES 
            ({now_usec - 10000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-User', 'target_identifier': NULL, 'execution_id': 'exec-early', 'source_name': 'u1@example.com', 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{(now_ts - timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')}')
        """)
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify initial snapshot
        snap_rows = self.conn.execute("SELECT start_time_usec, latest_execution_id, is_completed FROM snapshot_user_wave").fetchall()
        self.assertEqual(len(snap_rows), 1)
        self.assertEqual(snap_rows[0][0], now_usec - 10000)
        self.assertEqual(snap_rows[0][1], "exec-early")
        self.assertEqual(snap_rows[0][2], False)

        # Clear activity logs
        self.conn.execute("DELETE FROM activity;")

        # 2. Run DML second time with a LATER execution 'exec-late' which completes
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-User', 'User Batch', 'User (ID: Wave-User)', 'exec-late', TRUE);
            
            UPDATE map_execution_wave SET is_latest_execution = FALSE WHERE execution_id = 'exec-early';

            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'USER_MIGRATION_COMPLETE', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-User', 'target_identifier': NULL, 'execution_id': 'exec-late', 'source_name': 'u1@example.com', 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # 3. Verify that start_time_usec remains the minimum (now_usec - 10000)
        # while latest_execution_id is updated to 'exec-late' and is_completed is True
        snap_rows = self.conn.execute("SELECT start_time_usec, latest_execution_id, is_completed FROM snapshot_user_wave").fetchall()
        self.assertEqual(len(snap_rows), 1)
        self.assertEqual(snap_rows[0][0], now_usec - 10000)
        self.assertEqual(snap_rows[0][1], "exec-late")
        self.assertEqual(snap_rows[0][2], True)

    def test_transient_failure_recovery(self):
        """Test that a transient failure in an earlier execution is resolved by a success in a later execution."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup mapping
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Retry', 'Retry Wave', 'Retry (ID: Wave-Retry)', 'exec-1', FALSE),
            ('Exchange Online Migration', 'Wave-Retry', 'Retry Wave', 'Retry (ID: Wave-Retry)', 'exec-2', TRUE);
        """)

        # 2. Log first: failure in exec-1 for msg-retry
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 1000}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Connection reset'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Retry', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'msg-retry', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'CONN_RESET', 'migration_error_title': 'Connection Reset'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Check snapshot has the failure
        snap_items = self.conn.execute("SELECT event_status, execution_id FROM snapshot_item_user_wave WHERE source_identifier='msg-retry'").fetchall()
        self.assertEqual(snap_items, [("FAILED", "exec-1")])

        # Clear logs
        self.conn.execute("DELETE FROM activity;")

        # 3. Log second: success in exec-2 for msg-retry (later timestamp)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Retry', 'target_identifier': NULL, 'execution_id': 'exec-2', 'source_name': 'u@example.com', 'source_identifier': 'msg-retry', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # 4. Verify snapshot is updated to SUCCEEDED and execution_id is exec-2
        snap_items = self.conn.execute("SELECT event_status, execution_id FROM snapshot_item_user_wave WHERE source_identifier='msg-retry'").fetchall()
        self.assertEqual(snap_items, [("SUCCEEDED", "exec-2")])

        # 5. Verify fact metrics has 1 succeeded item and 0 failed
        wave_metrics = self.conn.execute("SELECT successfully_migrated_items, total_migrated_items FROM fact_wave_metrics WHERE batch_id='Wave-Retry'").fetchall()
        self.assertEqual(wave_metrics, [(1, 1)])

    def test_intra_window_deduplication(self):
        """Test that if failure and success events occur in the same window, success is prioritized."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Intra', 'Intra Wave', 'Intra (ID: Wave-Intra)', 'exec-1', TRUE);
        """)

        # Insert failure event first, then success event shortly after, but within the same daily window run
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Timeout'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Intra', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'item-dup', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'TIMEOUT', 'migration_error_title': 'Timeout'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            ({now_usec + 10}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Intra', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'item-dup', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify snapshot has SUCCEEDED status (prioritized)
        snap_items = self.conn.execute("SELECT event_status FROM snapshot_item_user_wave WHERE source_identifier='item-dup'").fetchall()
        self.assertEqual(snap_items, [("SUCCEEDED",)])

    def test_error_deduplication(self):
        """Test that duplicate error events with identical credentials/timestamps do not result in duplicate snapshot records."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Err', 'Err Wave', 'Err (ID: Wave-Err)', 'exec-1', TRUE);
        """)

        # Insert two identical error logs (representing duplicates)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Err', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'msg-1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            ({now_usec}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Err', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'msg-1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify snapshot_migration_errors only has 1 record
        err_rows = self.conn.execute("SELECT * FROM snapshot_migration_errors").fetchall()
        self.assertEqual(len(err_rows), 1)

    def test_timeline_aggregation_limits(self):
        """Test daily migration timeline counts and ensure events older than 1 year are excluded from timeline table."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.now(timezone.utc)
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup mapping
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Time', 'Time Wave', 'Time (ID: Wave-Time)', 'exec-1', TRUE);
        """)

        # 2. Insert items:
        # - Item 1: succeeded today (within 1 year)
        # - Item 2: succeeded 420 days ago (outside 1 year)
        old_time_usec = now_usec - int(timedelta(days=420).total_seconds() * 1_000_000)
        old_ts = now_ts - timedelta(days=420)
        
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Today success
            ({now_usec}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Time', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'i1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            -- 14 months ago success
            ({old_time_usec}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Time', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'i2', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{old_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run with 500-day lookback so both events are technically captured by Step 1
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "500"})

        # Verify timeline counts (timeline should only contain today's success)
        timeline_rows = self.conn.execute("SELECT migration_date, items_migrated FROM fact_migration_timeline").fetchall()
        self.assertEqual(len(timeline_rows), 1)
        self.assertEqual(timeline_rows[0][0], now_ts.date())
        self.assertEqual(timeline_rows[0][1], 1)

    def test_user_overall_multi_wave_consolidation(self):
        """Test platform-wide metrics aggregation in fact_user_overall_metrics when a user is in multiple waves."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # User u-multi@example.com is in:
        # - Wave-A: completed (1 succeeded item)
        # - Wave-B: in-progress (1 failed item)
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-A', 'A Batch', 'A (ID: Wave-A)', 'exec-a', TRUE),
            ('Exchange Online Migration', 'Wave-B', 'B Batch', 'B (ID: Wave-B)', 'exec-b', TRUE);
            
            -- Setup old completed wave snapshot
            INSERT INTO snapshot_user_wave (data_type, batch_id, user_identifier, start_time_usec, is_completed, latest_execution_id, latest_execution_time_usec) VALUES 
            ('Exchange Online Migration', 'Wave-A', 'u-multi@example.com', {now_usec - 100}, TRUE, 'exec-a', {now_usec - 100});
            INSERT INTO snapshot_item_user_wave VALUES 
            ('Exchange Online Migration', 'Wave-A', 'u-multi@example.com', 'item-a', 'CREATE_GMAIL_MESSAGE', 'Exchange Online Email Message', 'SUCCEEDED', {now_usec - 100}, 'exec-a');

            -- Setup new in-progress wave logs
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-B', 'target_identifier': NULL, 'execution_id': 'exec-b', 'source_name': 'u-multi@example.com', 'source_identifier': 'item-b', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Auth'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify fact_user_overall_metrics:
        # total_items_migrated = 1 (from wave A)
        # total_items = 2 (1 success + 1 failure)
        # status should be 'Running' since wave-B is not marked complete
        user_overall = self.conn.execute("SELECT total_items_migrated, total_items, status FROM fact_user_overall_metrics WHERE user_identifier='u-multi@example.com'").fetchall()
        self.assertEqual(len(user_overall), 1)
        succ, tot, status = user_overall[0]
        self.assertEqual(succ, 1)
        self.assertEqual(tot, 2)
        self.assertEqual(status, "Running")

    def test_empty_string_vs_null_normalization(self):
        """Test that empty string target_identifier resolves to Default Import Batch just like NULL."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # Setup event with empty string target_identifier AND a start event to trigger the mapping
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 100}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Empty', 'target_identifier': '', 'execution_id': 'exec-empty', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            ({now_usec}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Empty', 'target_identifier': NULL, 'execution_id': 'exec-empty', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify name in map_wave_details is Default Import Batch (due to NULLIF mapping)
        map_wave_rows = self.conn.execute("SELECT batch_name FROM map_wave_details WHERE batch_id='Wave-Empty'").fetchall()
        self.assertEqual(len(map_wave_rows), 1)
        self.assertEqual(map_wave_rows[0][0], "Default Import Batch")

    def test_zero_users_division_by_zero_safety(self):
        """Test that a wave with zero users is processed without crashing due to division-by-zero."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Wave mapped
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Zero', 'Zero User Batch', 'Zero (ID: Wave-Zero)', 'exec-zero', TRUE);
        """)

        # 2. Insert only a setup event (makes the wave active, but user_count = 0)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Zero', 'target_identifier': 'Zero User Batch', 'execution_id': 'exec-zero', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Execute DML pipeline
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify no crash, metrics should be zero
        wave_metrics = self.conn.execute("SELECT user_count, completed_user_count, avg_items_per_user, completion_percentage FROM fact_wave_metrics WHERE batch_id='Wave-Zero'").fetchall()
        self.assertEqual(len(wave_metrics), 1)
        user_cnt, comp_user, avg_items, comp_pct = wave_metrics[0]
        self.assertEqual(user_cnt, 0)
        self.assertEqual(comp_user, 0)
        self.assertEqual(avg_items, 0.0)
        self.assertEqual(comp_pct, 0.0)

    def test_zero_items_user_completion(self):
        """Test that a user with zero migrated items completes successfully without division-by-zero crashes."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # Wave mapping
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-EmptyUser', 'Empty User Batch', 'EmptyUser (ID: Wave-EmptyUser)', 'exec-1', TRUE);
        """)

        # Insert ONLY START_MIGRATION and USER_MIGRATION_COMPLETE events (no item events)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 100}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-EmptyUser', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'empty@example.com', 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            ({now_usec}, 'data_migration', 'USER_MIGRATION_COMPLETE', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-EmptyUser', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'empty@example.com', 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Execute DML pipeline
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify fact_wave_metrics: user_count=1, completed_user_count=1, total_migrated_items=0, success_percentage=0.0
        wave_metrics = self.conn.execute("SELECT user_count, completed_user_count, total_migrated_items, success_percentage, status FROM fact_wave_metrics WHERE batch_id='Wave-EmptyUser'").fetchall()
        self.assertEqual(len(wave_metrics), 1)
        user_cnt, comp_user, tot_items, succ_pct, status = wave_metrics[0]
        self.assertEqual(user_cnt, 1)
        self.assertEqual(comp_user, 1)
        self.assertEqual(tot_items, 0)
        self.assertEqual(succ_pct, 0.0)
        self.assertEqual(status, "Completed")

    def test_user_completion_rollback(self):
        """Test that starting a new execution rolls back a user's completion status to in-progress."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup user as completed in exec-1
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Rollback', 'Rollback Batch', 'Rollback (ID: Wave-Rollback)', 'exec-1', TRUE);
            
            INSERT INTO snapshot_user_wave (data_type, batch_id, user_identifier, start_time_usec, is_completed, latest_execution_id, latest_execution_time_usec) VALUES 
            ('Exchange Online Migration', 'Wave-Rollback', 'u1@example.com', {now_usec - 10000}, TRUE, 'exec-1', {now_usec - 10000});
        """)

        # 2. Simulate a new run exec-2 starting today (active in logs, but no completion event yet)
        self.conn.execute(f"""
            UPDATE map_execution_wave SET is_latest_execution = FALSE WHERE execution_id = 'exec-1';
            
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Rollback', 'Rollback Batch', 'Rollback (ID: Wave-Rollback)', 'exec-2', TRUE);

            -- Start event for exec-2
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Rollback', 'target_identifier': NULL, 'execution_id': 'exec-2', 'source_name': 'u1@example.com', 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # 3. Verify user is_completed has rolled back to False for the latest execution exec-2
        snap_rows = self.conn.execute("SELECT is_completed, latest_execution_id FROM snapshot_user_wave WHERE user_identifier='u1@example.com'").fetchall()
        self.assertEqual(snap_rows, [(False, "exec-2")])

    def test_blocker_error_removal_on_success(self):
        """Test that historical blocker errors are removed from fact_migration_errors when a retry succeeds."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup mapping for exec-1 (failed execution)
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Clear', 'Clear Wave', 'Clear (ID: Wave-Clear)', 'exec-1', TRUE);
        """)

        # Insert failure event in activity for exec-1
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 1000}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth Failure'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Clear', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'msg-err', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{(now_ts - timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run first DML to populate snapshot and fact tables with the blocker error
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify blocker error is listed in fact table
        err_rows = self.conn.execute("SELECT occurrence_count FROM fact_migration_errors WHERE user_identifier='u@example.com'").fetchall()
        self.assertEqual(err_rows, [(1,)])

        # Clean/truncate activity
        self.conn.execute("DELETE FROM activity;")

        # 2. Simulate exec-2 starting where the user has NO errors (e.g. they succeed)
        self.conn.execute(f"""
            UPDATE map_execution_wave SET is_latest_execution = FALSE WHERE execution_id = 'exec-1';
            
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Clear', 'Clear Wave', 'Clear (ID: Wave-Clear)', 'exec-2', TRUE);

            -- Success event for u@example.com in exec-2
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Clear', 'target_identifier': NULL, 'execution_id': 'exec-2', 'source_name': 'u@example.com', 'source_identifier': 'item-ok', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Execute DML pipeline
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # 3. Verify that the blocker error is removed from fact_migration_errors on success
        err_rows = self.conn.execute("SELECT occurrence_count FROM fact_migration_errors WHERE user_identifier='u@example.com'").fetchall()
        self.assertEqual(len(err_rows), 0, "Historical blocker error was not deleted on success!")

    def test_wave_name_update(self):
        """Test that wave name updates from 'UPDATE_MIGRATION_SETTINGS' are processed and propagated to metrics."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup mapping for exec-1 with name 'Old Name'
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Setup event
            ({now_usec - 100000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 123', 'target_identifier': 'Old Name', 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{(now_ts - timedelta(seconds=100)).strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Start event
            ({now_usec - 90000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 123', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{(now_ts - timedelta(seconds=90)).strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated Item
            ({now_usec - 80000}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 123', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u1@example.com', 'source_identifier': 'msg-1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{(now_ts - timedelta(seconds=80)).strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run pipeline to establish initial state
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Assert initial name is 'Old Name'
        rows = self.conn.execute("SELECT batch_name, batch_filter FROM map_wave_details WHERE batch_id='123'").fetchall()
        self.assertEqual(rows, [('Old Name', 'Old Name (ID: 123)')])

        wave_metrics_rows = self.conn.execute("SELECT batch_name, batch_filter FROM fact_wave_metrics WHERE batch_id='123'").fetchall()
        self.assertEqual(wave_metrics_rows, [('Old Name', 'Old Name (ID: 123)')])

        # 2. Insert Update migration settings event with 'New Name'
        self.conn.execute("DELETE FROM activity;")
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'UPDATE_MIGRATION_SETTINGS', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 123', 'target_identifier': 'New Name', 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run pipeline again
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify updated name is 'New Name' in map_wave_details and map_execution_wave
        map_wave_rows = self.conn.execute("SELECT batch_name, batch_filter FROM map_wave_details WHERE batch_id='123'").fetchall()
        self.assertEqual(map_wave_rows, [('New Name', 'New Name (ID: 123)')])

        exec_wave_rows = self.conn.execute("SELECT batch_name, batch_filter FROM map_execution_wave WHERE batch_id='123'").fetchall()
        self.assertEqual(exec_wave_rows, [('New Name', 'New Name (ID: 123)')])

        # Verify updated name is propagated to fact_wave_metrics
        wave_metrics_rows2 = self.conn.execute("SELECT batch_name, batch_filter FROM fact_wave_metrics WHERE batch_id='123'").fetchall()
        self.assertEqual(wave_metrics_rows2, [('New Name', 'New Name (ID: 123)')])

    def test_wave_name_update_null_override(self):
        """Test that an update event with a NULL or empty target_identifier does not override a previously set friendly wave name."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup mapping for exec-1 with name 'Old Name'
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Setup event
            ({now_usec - 100000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 123', 'target_identifier': 'Old Name', 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{(now_ts - timedelta(seconds=100)).strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Start event
            ({now_usec - 90000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 123', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{(now_ts - timedelta(seconds=90)).strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run pipeline to establish initial state
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Assert initial name is 'Old Name'
        rows = self.conn.execute("SELECT batch_name FROM map_wave_details WHERE batch_id='123'").fetchall()
        self.assertEqual(rows, [('Old Name',)])

        # 2. Insert UPDATE_MIGRATION_SETTINGS event with a NULL target_identifier (name is not specified in this event)
        self.conn.execute("DELETE FROM activity;")
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'UPDATE_MIGRATION_SETTINGS', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 123', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run pipeline again
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify the name remains 'Old Name' and did not revert to default
        map_wave_rows = self.conn.execute("SELECT batch_name FROM map_wave_details WHERE batch_id='123'").fetchall()
        self.assertEqual(map_wave_rows, [('Old Name',)])

    def test_wave_name_update_no_wave_id(self):
        """Test that an update event without a wave ID in the target_uri is ignored and doesn't update the wave name."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup mapping for exec-1 with name 'Old Name'
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Setup event
            ({now_usec - 100000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 123', 'target_identifier': 'Old Name', 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{(now_ts - timedelta(seconds=100)).strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Start event
            ({now_usec - 90000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 123', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{(now_ts - timedelta(seconds=90)).strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run pipeline to establish initial state
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Assert initial name is 'Old Name'
        rows = self.conn.execute("SELECT batch_name FROM map_wave_details WHERE batch_id='123'").fetchall()
        self.assertEqual(rows, [('Old Name',)])

        # 2. Insert UPDATE_MIGRATION_SETTINGS event without a WaveId in target_uri (e.g. invalid URI or NULL)
        self.conn.execute("DELETE FROM activity;")
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'UPDATE_MIGRATION_SETTINGS', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'SomeInvalidUriWithoutWaveId', 'target_identifier': 'New Name', 'execution_id': 'exec-1', 'source_name': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
              '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run pipeline again
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify the name remains 'Old Name' and was not updated to 'New Name'
        map_wave_rows = self.conn.execute("SELECT batch_name FROM map_wave_details WHERE batch_id='123'").fetchall()
        self.assertEqual(map_wave_rows, [('Old Name',)])

    def test_user_failed_status_conditions(self):
        """Test that user status is marked as Failed if total items migrated is zero and there is any failure (item or crawl)."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup execution and users:
        # User 1 (failed item): total_items_migrated = 0, failed_items = 1, crawl_failure_items = 0 -> status 'Failed'
        # User 2 (crawl failure): total_items_migrated = 0, failed_items = 0, crawl_failure_items = 1 -> status 'Failed'
        # User 3 (no items/failures): total_items_migrated = 0, failed_items = 0, crawl_failure_items = 0 -> status 'Running'
        # User 4 (completed but failed items): total_items_migrated = 0, failed_items = 1, is_completed = True -> status 'Failed'
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-Fail-Test', 'Fail Test Batch', 'Fail Test', 'exec-fail', TRUE);
            
            INSERT INTO snapshot_user_wave (data_type, batch_id, user_identifier, start_time_usec, is_completed, latest_execution_id, latest_execution_time_usec) VALUES 
            ('Exchange Online Migration', 'Wave-Fail-Test', 'u1@example.com', {now_usec}, FALSE, 'exec-fail', {now_usec}),
            ('Exchange Online Migration', 'Wave-Fail-Test', 'u2@example.com', {now_usec}, FALSE, 'exec-fail', {now_usec}),
            ('Exchange Online Migration', 'Wave-Fail-Test', 'u3@example.com', {now_usec}, FALSE, 'exec-fail', {now_usec}),
            ('Exchange Online Migration', 'Wave-Fail-Test', 'u4@example.com', {now_usec}, TRUE, 'exec-fail', {now_usec});
            
            -- User 1 item failure event
            INSERT INTO activity VALUES 
            ({now_usec + 10}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Fail-Test', 'target_identifier': NULL, 'execution_id': 'exec-fail', 'source_name': 'u1@example.com', 'source_identifier': 'item-1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '500', 'migration_error_title': 'Err'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}');

            -- User 2 crawl failure event
            INSERT INTO activity VALUES 
            ({now_usec + 20}, 'data_migration', 'CRAWL_FAILURE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Crawl Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Fail-Test', 'target_identifier': NULL, 'execution_id': 'exec-fail', 'source_name': 'u2@example.com', 'source_identifier': 'item-2', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'CRAWL_ERR', 'migration_error_title': 'Crawl Err'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}');

            -- User 4 item failure event
            INSERT INTO activity VALUES 
            ({now_usec + 30}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Fail-Test', 'target_identifier': NULL, 'execution_id': 'exec-fail', 'source_name': 'u4@example.com', 'source_identifier': 'item-4', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '500', 'migration_error_title': 'Err'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}');
        """)

        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify fact_user_wave_metrics
        u1_status = self.conn.execute("SELECT status FROM fact_user_wave_metrics WHERE user_identifier='u1@example.com'").fetchone()[0]
        u2_status = self.conn.execute("SELECT status FROM fact_user_wave_metrics WHERE user_identifier='u2@example.com'").fetchone()[0]
        u3_status = self.conn.execute("SELECT status FROM fact_user_wave_metrics WHERE user_identifier='u3@example.com'").fetchone()[0]
        u4_status = self.conn.execute("SELECT status FROM fact_user_wave_metrics WHERE user_identifier='u4@example.com'").fetchone()[0]

        self.assertEqual(u1_status, "Failed")
        self.assertEqual(u2_status, "Failed")
        self.assertEqual(u3_status, "Running")
        self.assertEqual(u4_status, "Failed")

        # Verify fact_user_overall_metrics
        u1_overall = self.conn.execute("SELECT status FROM fact_user_overall_metrics WHERE user_identifier='u1@example.com'").fetchone()[0]
        u2_overall = self.conn.execute("SELECT status FROM fact_user_overall_metrics WHERE user_identifier='u2@example.com'").fetchone()[0]
        u3_overall = self.conn.execute("SELECT status FROM fact_user_overall_metrics WHERE user_identifier='u3@example.com'").fetchone()[0]
        u4_overall = self.conn.execute("SELECT status FROM fact_user_overall_metrics WHERE user_identifier='u4@example.com'").fetchone()[0]

        self.assertEqual(u1_overall, "Failed")
        self.assertEqual(u2_overall, "Failed")
        self.assertEqual(u3_overall, "Running")
        self.assertEqual(u4_overall, "Failed")

    def test_top_errors_aggregation(self):
        """Test that fact_top_errors properly aggregates, unique-ifies, and ranks errors."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # Setup mapping
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-1', 'Wave 1', 'Wave 1 Filter', 'exec-1', TRUE);
        """)

        # Insert events in activity
        # Title A: 3 occurrences (u1)
        # Title B: 1 occurrence (u2)
        # Title C: 5 occurrences (u3)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 100}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u1@example.com', 'source_identifier': 'i1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 90}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u1@example.com', 'source_identifier': 'i2', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 80}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u1@example.com', 'source_identifier': 'i3', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            ({now_usec - 70}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error B'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u2@example.com', 'source_identifier': 'i4', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '500', 'migration_error_title': 'Internal Error'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            ({now_usec - 60}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i5', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 50}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i6', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 40}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i7', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 30}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i8', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 20}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i9', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 15}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u4@example.com', 'source_identifier': 'i10', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 10}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u4@example.com', 'source_identifier': 'i11', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run DML
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify fact_top_errors has unique title rows ordered by count desc with data_type:
        # 1. Forbidden (5), u3@example.com
        # 2. Unauthorized (5), u1@example.com (sum is 5, u1 has higher count 3 than u4's 2)
        # 3. Internal Error (1), u2@example.com
        rows = self.conn.execute("SELECT data_type, migration_error_title, occurrence_count, user_identifier FROM fact_top_errors ORDER BY occurrence_count DESC, migration_error_title").fetchall()
        self.assertEqual(rows, [
            ('Exchange Online Migration', 'Forbidden', 5, 'u3@example.com'),
            ('Exchange Online Migration', 'Unauthorized', 5, 'u1@example.com'),
            ('Exchange Online Migration', 'Internal Error', 1, 'u2@example.com')
        ])

    def test_latest_execution_per_batch_errors(self):
        """Test that fact_migration_errors only contains errors from the absolute latest execution of the batch."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup execution exec-1 (initial run with failures for u1 and u2)
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-A', 'Wave A', 'Wave A Filter', 'exec-1', TRUE);
        """)

        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 1000}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-A', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u1@example.com', 'source_identifier': 'i1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 900}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Forbidden'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-A', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u2@example.com', 'source_identifier': 'i2', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run DML for first execution
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify both u1 and u2 errors are in the fact table
        err_rows = self.conn.execute("SELECT user_identifier, migration_error_code FROM fact_migration_errors ORDER BY user_identifier").fetchall()
        self.assertEqual(err_rows, [('u1@example.com', '401'), ('u2@example.com', '403')])

        # Clean/truncate activity
        self.conn.execute("DELETE FROM activity;")

        # 2. Simulate exec-2 starting (u1 retried and failed with a new error; u2 not retried)
        self.conn.execute(f"""
            UPDATE map_execution_wave SET is_latest_execution = FALSE WHERE execution_id = 'exec-1';
            
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('Exchange Online Migration', 'Wave-A', 'Wave A', 'Wave A Filter', 'exec-2', TRUE);

            -- u1 fails with 500 error in exec-2
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Internal Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-A', 'target_identifier': NULL, 'execution_id': 'exec-2', 'source_name': 'u1@example.com', 'source_identifier': 'i3', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '500', 'migration_error_title': 'Internal'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run DML for second execution
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify that ONLY u1's new error from exec-2 exists in the table. u2's old error is gone.
        err_rows = self.conn.execute("SELECT user_identifier, migration_error_code FROM fact_migration_errors").fetchall()
        self.assertEqual(err_rows, [('u1@example.com', '500')])

    def test_onedrive_pipeline_logic(self):
        """Test full pipeline execution for OneDrive file migrations (mapping, snapshots, and file fact tables)."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Populate activity logs for OneDrive
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Setup event
            ({now_usec - 100000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'OneDrive Enterprise Migration', 'target_uri': 'WaveId: OD-101', 'target_identifier': 'OneDrive Batch 1', 'execution_id': 'od-exec-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Start event
            ({now_usec - 90000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'OneDrive Enterprise Migration', 'target_uri': 'WaveId: OD-101', 'target_identifier': NULL, 'execution_id': 'od-exec-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated File 1 (Success)
            ({now_usec - 80000}, 'data_migration', 'CREATE_FILE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'OneDrive Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'files/g1', 'execution_id': 'od-exec-1', 'source_name': NULL, 'source_uri': 'https://tenant-my.sharepoint.com/personal/user1_tenant_com/Documents/Doc1.docx', 'source_identifier': 'documentLibraries/lib1/files/f1', 'source_type': 'OneDrive Item', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated Folder 1 (Success)
            ({now_usec - 70000}, 'data_migration', 'CREATE_FOLDER', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'OneDrive Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'folders/g2', 'execution_id': 'od-exec-1', 'source_name': NULL, 'source_uri': 'https://tenant-my.sharepoint.com/personal/user1_tenant_com/Documents/Folder1', 'source_identifier': 'documentLibraries/lib1/folders/fld1', 'source_type': 'OneDrive Folder Crawler', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated File 2 (Failed)
            ({now_usec - 60000}, 'data_migration', 'CREATE_FILE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Upload quota exceeded'}},
             {{'migration_type': 'OneDrive Enterprise Migration', 'target_uri': NULL, 'target_identifier': NULL, 'execution_id': 'od-exec-1', 'source_name': NULL, 'source_uri': 'https://tenant-my.sharepoint.com/personal/user1_tenant_com/Documents/Doc2.docx', 'source_identifier': 'documentLibraries/lib1/files/f2', 'source_type': 'OneDrive Item', 'migration_error_code': 'QUOTA_EXCEEDED', 'migration_error_title': 'Quota Exceeded'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- User complete event
            ({now_usec - 50000}, 'data_migration', 'USER_MIGRATION_COMPLETE', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'OneDrive Enterprise Migration', 'target_uri': 'WaveId: OD-101', 'target_identifier': NULL, 'execution_id': 'od-exec-1', 'source_name': NULL, 'source_uri': 'https://tenant-my.sharepoint.com/personal/user1_tenant_com/Documents', 'source_identifier': 'documentLibraries/lib1', 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # 2. Run DML SQL
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # 3. Assertions on fact_file_wave_metrics
        file_wave_rows = self.conn.execute("SELECT * FROM fact_file_wave_metrics WHERE batch_id='OD-101'").fetchall()
        self.assertEqual(len(file_wave_rows), 1)
        data_type, batch_id, batch_name, batch_filter, start_date, entity_count, completed_entity_count, succ_items, tot_items, files_cnt, fld_cnt, dl_cnt, succ_pct, comp_pct, avg_items, status = file_wave_rows[0]
        self.assertEqual(data_type, "OneDrive Enterprise Migration")
        self.assertEqual(batch_name, "OneDrive Batch 1")
        self.assertEqual(entity_count, 1)
        self.assertEqual(completed_entity_count, 1)
        self.assertEqual(succ_items, 2) # 1 file + 1 folder
        self.assertEqual(tot_items, 3) # 2 success + 1 failed
        self.assertEqual(files_cnt, 1)
        self.assertEqual(fld_cnt, 1)
        self.assertEqual(dl_cnt, 1)
        self.assertAlmostEqual(succ_pct, 2.0 / 3.0, places=4)
        self.assertEqual(comp_pct, 1.0)
        self.assertEqual(status, "Completed")

        # 4. Assertions on fact_entity_file_wave_metrics
        entity_wave_rows = self.conn.execute("SELECT * FROM fact_entity_file_wave_metrics WHERE batch_id='OD-101'").fetchall()
        self.assertEqual(len(entity_wave_rows), 1)
        e_dtype, e_bid, e_bname, e_bfilter, entity_id, e_succ_items, e_files, e_flds, e_dls, e_failed, e_crawl_fail, e_tot, e_succ_pct, e_status = entity_wave_rows[0]
        self.assertEqual(entity_id, "tenant-my.sharepoint.com/personal/user1_tenant_com")
        self.assertEqual(e_succ_items, 2)
        self.assertEqual(e_files, 1)
        self.assertEqual(e_flds, 1)
        self.assertEqual(e_dls, 1)
        self.assertEqual(e_failed, 1)
        self.assertEqual(e_status, "Completed")

        # 5. Assertions on fact_entity_file_overall_metrics
        entity_overall_rows = self.conn.execute("SELECT * FROM fact_entity_file_overall_metrics WHERE entity_identifier='tenant-my.sharepoint.com/personal/user1_tenant_com'").fetchall()
        self.assertEqual(len(entity_overall_rows), 1)
        self.assertEqual(entity_overall_rows[0][3], 2) # total_items_migrated
        self.assertEqual(entity_overall_rows[0][4], 1) # migrated_files_count
        self.assertEqual(entity_overall_rows[0][5], 1) # migrated_folders_count
        self.assertEqual(entity_overall_rows[0][6], 1) # migrated_document_libraries_count
        self.assertEqual(entity_overall_rows[0][7], 3) # total_items
        self.assertEqual(entity_overall_rows[0][9], "Completed") # status

    def test_sharepoint_pipeline_logic(self):
        """Test full pipeline execution for SharePoint Online Enterprise migrations (site extraction, sharing links, file metrics)."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Populate activity logs for SharePoint (2 sites: Marketing via standard URI, Engineering via sharing-link URI in source_identifier)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Setup event
            ({now_usec - 100000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: SP-201', 'target_identifier': 'SP Enterprise Batch 1', 'execution_id': 'sp-exec-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Start event
            ({now_usec - 90000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: SP-201', 'target_identifier': NULL, 'execution_id': 'sp-exec-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Site 1: Marketing (File 1 Success)
            ({now_usec - 80000}, 'data_migration', 'CREATE_FILE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'files/sp-g1', 'execution_id': 'sp-exec-1', 'source_name': NULL, 'source_uri': 'https://tenant.sharepoint.com/sites/Marketing/Shared Documents/Presentation.pptx', 'source_identifier': 'documentLibraries/lib-m/files/f1', 'source_type': 'Sharepoint File', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Site 1: Marketing (Folder 1 Success)
            ({now_usec - 70000}, 'data_migration', 'CREATE_FOLDER', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'folders/sp-g2', 'execution_id': 'sp-exec-1', 'source_name': NULL, 'source_uri': 'https://tenant.sharepoint.com/sites/Marketing/Shared Documents/SubFolder', 'source_identifier': 'documentLibraries/lib-m/folders/fld1', 'source_type': 'Sharepoint Folder', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Site 2: Engineering (Crawler sharing link failure - source_uri is empty, site URL is in source_identifier)
            ({now_usec - 60000}, 'data_migration', 'CRAWL_FAILURE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Crawler error accessing site collection'}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': NULL, 'target_identifier': NULL, 'execution_id': 'sp-exec-1', 'source_name': NULL, 'source_uri': '', 'source_identifier': 'sites/https://tenant.sharepoint.com/:f:/g/sites/Engineering/IgBq6zNVWuZSQJJhQYr8wzDrAenqRTaL6uoT04yI6UkmOds/', 'source_type': 'Sharepoint Site', 'migration_error_code': 'CRAWL_FAILED', 'migration_error_title': 'Site Crawl Failed'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Site 1 Complete event (SharePoint sites send SITE_MIGRATION_COMPLETE with the site URL in source_name)
            ({now_usec - 50000}, 'data_migration', 'SITE_MIGRATION_COMPLETE', 'MIGRATION', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': NULL, 'target_identifier': NULL, 'execution_id': 'sp-exec-1', 'source_name': 'https://tenant.sharepoint.com/sites/Marketing', 'source_uri': NULL, 'source_identifier': 'sites/https://tenant.sharepoint.com/sites/Marketing', 'source_type': 'Sharepoint Site', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # 2. Run DML SQL
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # 3. Assertions on fact_file_wave_metrics
        file_wave_rows = self.conn.execute("SELECT * FROM fact_file_wave_metrics WHERE batch_id='SP-201'").fetchall()
        self.assertEqual(len(file_wave_rows), 1)
        data_type, batch_id, batch_name, batch_filter, start_date, entity_count, completed_entity_count, succ_items, tot_items, files_cnt, fld_cnt, dl_cnt, succ_pct, comp_pct, avg_items, status = file_wave_rows[0]
        self.assertEqual(data_type, "SharePoint Online Enterprise Migration")
        self.assertEqual(entity_count, 2) # Marketing + Engineering
        self.assertEqual(completed_entity_count, 1) # Only Marketing completed
        self.assertEqual(succ_items, 2) # 1 file + 1 folder
        self.assertEqual(files_cnt, 1)
        self.assertEqual(fld_cnt, 1)
        self.assertEqual(dl_cnt, 1)
        # Engineering only has a crawl failure and no completion event yet, so the batch is still running.
        self.assertEqual(status, "Running")

        # 4. Assertions on fact_entity_file_wave_metrics
        entities = self.conn.execute("SELECT entity_identifier, total_items_migrated, migrated_document_libraries_count, crawl_failure_items, status FROM fact_entity_file_wave_metrics WHERE batch_id='SP-201' ORDER BY entity_identifier").fetchall()
        self.assertEqual(len(entities), 2)
        
        # Engineering site extracted from sharing link
        self.assertEqual(entities[0][0], "tenant.sharepoint.com/:f:/g/sites/Engineering")
        self.assertEqual(entities[0][1], 0) # 0 migrated
        self.assertEqual(entities[0][2], 0) # 0 DLs
        self.assertEqual(entities[0][3], 1) # 1 crawl failure
        # Only a crawl failure, no completion event and no failed site container -> still Running (as in the DMS report).
        self.assertEqual(entities[0][4], "Running")
        
        # Marketing site
        self.assertEqual(entities[1][0], "tenant.sharepoint.com/sites/Marketing")
        self.assertEqual(entities[1][1], 2) # 2 migrated
        self.assertEqual(entities[1][2], 1) # 1 DL
        self.assertEqual(entities[1][3], 0)
        self.assertEqual(entities[1][4], "Completed")

    def test_sharepoint_subsite_and_dl_counting(self):
        """Test SharePoint sites with subsites and multiple document libraries (matching real scenario 1)."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # Site1 (has 2 DLs: Shared Documents & SitePages), Site1/subsite1 (has 2 DLs: Shared Documents & SitePages)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Setup event
            ({now_usec - 100000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: SP-SUBSITES', 'target_identifier': 'SP Subsites Batch', 'execution_id': 'sp-sub-exec-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Start event
            ({now_usec - 90000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: SP-SUBSITES', 'target_identifier': NULL, 'execution_id': 'sp-sub-exec-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Site 1 Container Setup (Source type: Sharepoint Site -> should NOT be counted in folder count!)
            ({now_usec - 85000}, 'data_migration', 'CREATE_FOLDER', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'folders/root1', 'execution_id': 'sp-sub-exec-1', 'source_name': NULL, 'source_uri': 'https://smh3v.sharepoint.com/sites/Site1', 'source_identifier': 'sites/https://smh3v.sharepoint.com/sites/Site1/', 'source_type': 'Sharepoint Site', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Site 1: DL 1 (/Shared Documents) -> 1 File, 1 Folder
            ({now_usec - 80000}, 'data_migration', 'CREATE_FILE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'files/f1', 'execution_id': 'sp-sub-exec-1', 'source_name': NULL, 'source_uri': 'https://smh3v.sharepoint.com/sites/Site1//Shared Documents/File1.docx', 'source_identifier': 'documentLibraries/4c433867-be53-4240-aefe-aa1612bf1363/files/5396a7ec', 'source_type': 'Sharepoint File', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 78000}, 'data_migration', 'CREATE_FOLDER', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'folders/fld1', 'execution_id': 'sp-sub-exec-1', 'source_name': NULL, 'source_uri': 'https://smh3v.sharepoint.com/sites/Site1/Shared Documents/Folder1', 'source_identifier': 'documentLibraries/4c433867-be53-4240-aefe-aa1612bf1363/folders/df316493', 'source_type': 'Sharepoint Folder', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Site 1: DL 2 (/SitePages) -> 1 File
            ({now_usec - 75000}, 'data_migration', 'CREATE_FILE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'files/f2', 'execution_id': 'sp-sub-exec-1', 'source_name': NULL, 'source_uri': 'https://smh3v.sharepoint.com/sites/Site1//SitePages/Home.aspx', 'source_identifier': 'documentLibraries/22a0a0da-2a83-421a-a0ce-94d4172147b6/files/c3ff3352', 'source_type': 'Sharepoint File', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Subsite 1 Container Setup (Source type: Sharepoint Site)
            ({now_usec - 70000}, 'data_migration', 'CREATE_FOLDER', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'folders/subroot1', 'execution_id': 'sp-sub-exec-1', 'source_name': NULL, 'source_uri': 'https://smh3v.sharepoint.com/sites/Site1/subsite1', 'source_identifier': 'sites/https://smh3v.sharepoint.com/sites/Site1/subsite1', 'source_type': 'Sharepoint Site', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Subsite 1: DL 1 (/Shared Documents) -> 1 Folder
            ({now_usec - 65000}, 'data_migration', 'CREATE_FOLDER', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'folders/subfld1', 'execution_id': 'sp-sub-exec-1', 'source_name': NULL, 'source_uri': 'https://smh3v.sharepoint.com/sites/Site1/subsite1/Shared Documents', 'source_identifier': 'documentLibraries/00bcca06-132d-4fb6-ab40-77f14339f19c/folders/', 'source_type': 'Sharepoint Folder', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Subsite 1: DL 2 (/SitePages) -> 1 File
            ({now_usec - 60000}, 'data_migration', 'CREATE_FILE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'files/subf1', 'execution_id': 'sp-sub-exec-1', 'source_name': NULL, 'source_uri': 'https://smh3v.sharepoint.com/sites/Site1/subsite1/SitePages/Home.aspx', 'source_identifier': 'documentLibraries/66db3539-ba37-4c2f-afe3-1d6c26f8dab7/files/c48a9f8e', 'source_type': 'Sharepoint File', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Site completion events for Site1 and subsite1
            ({now_usec - 50000}, 'data_migration', 'SITE_MIGRATION_COMPLETE', 'MIGRATION', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': NULL, 'target_identifier': NULL, 'execution_id': 'sp-sub-exec-1', 'source_name': 'https://smh3v.sharepoint.com/sites/Site1', 'source_uri': NULL, 'source_identifier': 'sites/https://smh3v.sharepoint.com/sites/Site1', 'source_type': 'Sharepoint Site', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 40000}, 'data_migration', 'SITE_MIGRATION_COMPLETE', 'MIGRATION', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': NULL, 'target_identifier': NULL, 'execution_id': 'sp-sub-exec-1', 'source_name': 'https://smh3v.sharepoint.com/sites/Site1/subsite1/', 'source_uri': NULL, 'source_identifier': 'sites/https://smh3v.sharepoint.com/sites/Site1/subsite1', 'source_type': 'Sharepoint Site', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run DML
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Assert entity level
        entities = self.conn.execute("SELECT entity_identifier, migrated_files_count, migrated_folders_count, migrated_document_libraries_count, status FROM fact_entity_file_wave_metrics WHERE batch_id='SP-SUBSITES' ORDER BY entity_identifier").fetchall()
        self.assertEqual(len(entities), 2)

        # Site1
        self.assertEqual(entities[0][0], "smh3v.sharepoint.com/sites/Site1")
        self.assertEqual(entities[0][1], 2) # 2 files (File1.docx, Home.aspx)
        self.assertEqual(entities[0][2], 1) # 1 folder (Folder1, root site excluded!)
        self.assertEqual(entities[0][3], 2) # 2 DLs (4c43... and 22a0...)
        self.assertEqual(entities[0][4], "Completed")

        # Subsite1
        self.assertEqual(entities[1][0], "smh3v.sharepoint.com/sites/Site1/subsite1")
        self.assertEqual(entities[1][1], 1) # 1 file (Home.aspx)
        self.assertEqual(entities[1][2], 1) # 1 folder (Shared Documents, subroot excluded!)
        self.assertEqual(entities[1][3], 2) # 2 DLs (00bc... and 66db...)
        self.assertEqual(entities[1][4], "Completed")

        # Assert batch level
        batch = self.conn.execute("SELECT entity_count, completed_entity_count, migrated_files_count, migrated_folders_count, migrated_document_libraries_count, status FROM fact_file_wave_metrics WHERE batch_id='SP-SUBSITES'").fetchall()
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch[0][0], 2) # 2 entities
        self.assertEqual(batch[0][1], 2) # 2 completed entities
        self.assertEqual(batch[0][2], 3) # 3 files total
        self.assertEqual(batch[0][3], 2) # 2 folders total
        self.assertEqual(batch[0][4], 4) # 4 DLs total (2 + 2)
        self.assertEqual(batch[0][5], "Completed")

    def test_entity_unification_logic(self):
        """Test that Run 1 crawl failure (NULL source_uri) unifies with Run 2 file creation under the same clean URL."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # Populate Run 1 (crawl failure with NULL source_uri) and Run 2 (success with clean source_uri) for the same document library
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Setup event
            ({now_usec - 100000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: OD-UNIFY', 'target_identifier': 'OneDrive Unify Batch', 'execution_id': 'exec-unify-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Run 1 Start
            ({now_usec - 90000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: OD-UNIFY', 'target_identifier': NULL, 'execution_id': 'exec-unify-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Run 1: Crawl failure (source_uri is NULL, source_identifier is documentLibraries/lib-guid/folders/)
            ({now_usec - 80000}, 'data_migration', 'CRAWL_FAILURE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Discovery failure'}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': NULL, 'target_identifier': NULL, 'execution_id': 'exec-unify-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': 'documentLibraries/f6226517-690c-4b9d-ac74-9bff2c99c78a/folders/', 'source_type': 'OneDrive Folder', 'migration_error_code': 'CRAWL_FAILED', 'migration_error_title': 'Crawl Failed'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Run 2 Start
            ({now_usec - 70000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: OD-UNIFY', 'target_identifier': NULL, 'execution_id': 'exec-unify-2', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Run 2: File Success (source_uri populated with personal URL, source_identifier is documentLibraries/lib-guid/files/f1)
            ({now_usec - 60000}, 'data_migration', 'CREATE_FILE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'files/g1', 'execution_id': 'exec-unify-2', 'source_name': NULL, 'source_uri': 'https://tenant-my.sharepoint.com/personal/bugbash1_tenant_com/Documents/Test3.txt', 'source_identifier': 'documentLibraries/f6226517-690c-4b9d-ac74-9bff2c99c78a/files/b2aac29f-f99f-4752-b407-36dbbf04ab30', 'source_type': 'OneDrive Item', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run DML
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Assert exactly 1 unified entity row exists for that document library
        entities = self.conn.execute("SELECT entity_identifier, total_items_migrated, crawl_failure_items FROM fact_entity_file_wave_metrics WHERE batch_id='OD-UNIFY'").fetchall()
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0][0], "tenant-my.sharepoint.com/personal/bugbash1_tenant_com")
        self.assertEqual(entities[0][1], 1) # 1 item migrated from Run 2

    def test_multidatatype_coexistence(self):
        """Test concurrent processing of Exchange, OneDrive, and SharePoint migrations in the same pipeline run."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- 1. Exchange Online
            ({now_usec - 60000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: EX-1', 'target_identifier': 'Exchange Batch', 'execution_id': 'ex-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 55000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: EX-1', 'target_identifier': NULL, 'execution_id': 'ex-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 50000}, 'data_migration', 'CREATE_GMAIL_MESSAGE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: EX-1', 'target_identifier': NULL, 'execution_id': 'ex-1', 'source_name': 'ex_user@example.com', 'source_uri': NULL, 'source_identifier': 'msg-10', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- 2. OneDrive
            ({now_usec - 40000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'OneDrive Enterprise Migration', 'target_uri': 'WaveId: OD-1', 'target_identifier': 'OneDrive Batch', 'execution_id': 'od-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 35000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'OneDrive Enterprise Migration', 'target_uri': 'WaveId: OD-1', 'target_identifier': NULL, 'execution_id': 'od-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 30000}, 'data_migration', 'CREATE_FILE', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'OneDrive Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'files/g1', 'execution_id': 'od-1', 'source_name': NULL, 'source_uri': 'https://tenant-my.sharepoint.com/personal/od_user_tenant_com/Documents/file.pdf', 'source_identifier': 'files/od-10', 'source_type': 'OneDrive Item', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- 3. SharePoint
            ({now_usec - 20000}, 'data_migration', 'START_MIGRATION_SETUP', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: SP-1', 'target_identifier': 'SharePoint Batch', 'execution_id': 'sp-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 15000}, 'data_migration', 'START_MIGRATION', 'SUCCESS', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: SP-1', 'target_identifier': NULL, 'execution_id': 'sp-1', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 10000}, 'data_migration', 'CREATE_FOLDER', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'https://drive.google.com/...', 'target_identifier': 'folders/g1', 'execution_id': 'sp-1', 'source_name': NULL, 'source_uri': 'https://tenant.sharepoint.com/sites/Legal/Docs', 'source_identifier': 'folders/sp-10', 'source_type': 'Sharepoint Folder', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run DML
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Assert fact_datatype_metrics has all 3 data types
        datatypes = self.conn.execute("SELECT data_type, total_users_migrated, success_percentage FROM fact_datatype_metrics ORDER BY data_type").fetchall()
        self.assertEqual(len(datatypes), 3)
        self.assertEqual(datatypes[0][0], "Exchange Online Migration")
        self.assertEqual(datatypes[1][0], "OneDrive Enterprise Migration")
        self.assertEqual(datatypes[2][0], "SharePoint Online Enterprise Migration")

        # Assert Exchange fact table only has Exchange wave
        ex_waves = self.conn.execute("SELECT batch_id FROM fact_wave_metrics").fetchall()
        self.assertEqual(ex_waves, [("EX-1",)])

        # Assert File fact table only has OneDrive and SharePoint waves
        file_waves = self.conn.execute("SELECT batch_id, data_type FROM fact_file_wave_metrics ORDER BY batch_id").fetchall()
        self.assertEqual(file_waves, [("OD-1", "OneDrive Enterprise Migration"), ("SP-1", "SharePoint Online Enterprise Migration")])

        # Assert Timeline includes all 3 events
        timeline_rows = self.conn.execute("SELECT data_type, items_migrated FROM fact_migration_timeline ORDER BY data_type").fetchall()
        self.assertEqual(len(timeline_rows), 3)

    def test_onedrive_amr_crawl_failure_resolution(self):
        """Test that OneDrive amrJobs crawl failure URLs are cleanly resolved to personal/account_name."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # Setup mapping
        self.conn.execute(f"""
            INSERT INTO map_execution_wave (data_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution) VALUES 
            ('SharePoint Online Enterprise Migration', 'Wave-Fail', 'Wave Fail', 'Wave Fail Filter', 'exec-fail', TRUE);
        """)

        # Insert 3 events from user's crawl failure sheet
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 300}, 'data_migration', 'START_MIGRATION', 'MIGRATION_SETUP', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': 'WaveId: Wave-Fail, ExecutionType: Full', 'target_identifier': NULL, 'execution_id': 'exec-fail', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': NULL, 'source_type': NULL, 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            ({now_usec - 200}, 'data_migration', 'CRAWL_FAILURE', 'MIGRATION', {{'event_status': 'FAILED', 'error_message': 'Target email not found'}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': NULL, 'target_identifier': 'users/user1@example.com', 'execution_id': 'exec-fail', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': 'https://smh3v-my.sharepoint.com/personal/bugbash1_smh3v_onmicrosoft_com/amrJobs/scopes/Documents', 'source_type': 'Sharepoint Document Library Crawler Phase 1', 'migration_error_code': 'C002', 'migration_error_title': 'TargetUserExternal'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            ({now_usec - 100}, 'data_migration', 'CRAWL_FAILURE', 'MIGRATION', {{'event_status': 'FAILED', 'error_message': 'Target email not found'}},
             {{'migration_type': 'SharePoint Online Enterprise Migration', 'target_uri': NULL, 'target_identifier': 'users/user2@example.com', 'execution_id': 'exec-fail', 'source_name': NULL, 'source_uri': NULL, 'source_identifier': 'https://smh3v-my.sharepoint.com/personal/bugbash2_smh3v_onmicrosoft_com/amrJobs/scopes/Documents', 'source_type': 'Sharepoint Document Library Crawler Phase 1', 'migration_error_code': 'C002', 'migration_error_title': 'TargetUserExternal'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run DML
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify fact_entity_file_wave_metrics has the clean URLs
        entities = self.conn.execute("SELECT entity_identifier, crawl_failure_items, status FROM fact_entity_file_wave_metrics ORDER BY entity_identifier").fetchall()
        self.assertEqual(len(entities), 2)
        # No site container row and a document-library-level discovery failure -> Failed (DMS report rule).
        self.assertEqual(entities[0], ('smh3v-my.sharepoint.com/personal/bugbash1_smh3v_onmicrosoft_com', 1, 'Failed'))
        self.assertEqual(entities[1], ('smh3v-my.sharepoint.com/personal/bugbash2_smh3v_onmicrosoft_com', 1, 'Failed'))

        # Verify batch status: every drive failed (failed drives count as processed)
        wave_status = self.conn.execute("SELECT status, entity_count, completed_entity_count FROM fact_file_wave_metrics WHERE batch_id='Wave-Fail'").fetchone()
        self.assertEqual(wave_status, ('Failed', 2, 2))

        # Verify fact_top_errors contains the failure
        top_err = self.conn.execute("SELECT data_type, migration_error_title, occurrence_count FROM fact_top_errors WHERE migration_error_title='TargetUserExternal'").fetchone()
        self.assertEqual(top_err, ('SharePoint Online Enterprise Migration', 'TargetUserExternal', 2))

    # ------------------------------------------------------------------
    # OneDrive / SharePoint hard cutoff
    # ------------------------------------------------------------------
    def _activity_row(self, time_usec, event_name, migration_type, execution_id, wave_id=None,
                      execution_type=None, source_name=None, source_identifier=None, source_type=None,
                      status='SUCCEEDED', error_code=None, error_title=None, event_type='migration'):
        """Builds one activity row literal for INSERT INTO activity VALUES."""
        def q(v):
            return 'NULL' if v is None else "'" + str(v).replace("'", "''") + "'"
        target_uri = None
        if wave_id is not None:
            target_uri = f'WaveId: {wave_id}' + (f', ExecutionType: {execution_type}' if execution_type else '')
        part = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        return (
            f"({time_usec}, 'data_migration', {q(event_name)}, {q(event_type)}, "
            f"{{'event_status': {q(status)}, 'error_message': {q('err' if status == 'FAILED' else None)}}}, "
            f"{{'migration_type': {q(migration_type)}, 'target_uri': {q(target_uri)}, 'target_identifier': NULL, "
            f"'execution_id': {q(execution_id)}, 'source_name': {q(source_name)}, 'source_uri': NULL, "
            f"'source_identifier': {q(source_identifier)}, 'source_type': {q(source_type)}, "
            f"'migration_error_code': {q(error_code)}, 'migration_error_title': {q(error_title)}}}, '{part}')"
        )

    def _insert_activity(self, rows):
        self.conn.execute("INSERT INTO activity VALUES " + ",\n".join(rows))

    def _run_with_cutoff(self, cutoff_dt, lookback_days="500"):
        self.run_sql_file(self.dml_path, replacements={
            "{lookback_days}": lookback_days,
            "{od_sp_cutoff}": cutoff_dt.strftime('%Y-%m-%d %H:%M:%S') + '+00',
        })

    def test_od_sp_cutoff_hides_batches_started_before_cutoff(self):
        """Only OD/SP batches whose earliest visible run is a Full run on/after the cutoff are reported."""
        self.run_sql_file(self.ddl_path)
        now = datetime.utcnow()
        day = 86_400 * 1_000_000
        now_usec = int(now.timestamp() * 1_000_000)
        cutoff = now - timedelta(days=10)
        SP, OD, EX = ('SharePoint Online Enterprise Migration', 'OneDrive Enterprise Migration',
                      'Exchange Online Migration')
        site = 'https://tenant.sharepoint.com/sites/Site{}/'
        r = self._activity_row
        rows = []

        def sp_items(wave, exec_id, t, n, fail=False):
            out = [
                r(t, 'CREATE_FILE', SP, exec_id, source_name=site.format(wave),
                  source_identifier=f'documentLibraries/dl-{wave}/files/f{n}', source_type='Sharepoint File'),
            ]
            if fail:
                out.append(r(t + 1, 'CREATE_FILE', SP, exec_id, source_name=site.format(wave),
                             source_identifier=f'documentLibraries/dl-{wave}/files/bad{n}',
                             source_type='Sharepoint File', status='FAILED',
                             error_code=f'E-{wave}', error_title=f'Error {wave}'))
            return out

        # Shown: Full after the cutoff.
        rows += [r(now_usec - 5 * day, 'START_MIGRATION', SP, 'new-full', 'SPNEW', 'Full')]
        rows += sp_items('SPNEW', 'new-full', now_usec - 5 * day + 10, 1, fail=True)
        # Shown: after the cutoff, ExecutionType missing (treated as Full).
        rows += [r(now_usec - 4 * day, 'START_MIGRATION', SP, 'notype-1', 'SPNOTYPE')]
        rows += sp_items('SPNOTYPE', 'notype-1', now_usec - 4 * day + 10, 1)
        # Hidden: Full before the cutoff, Delta after it.
        rows += [r(now_usec - 20 * day, 'START_MIGRATION', SP, 'old-full', 'SPOLD', 'Full')]
        rows += sp_items('SPOLD', 'old-full', now_usec - 20 * day + 10, 1)
        rows += [r(now_usec - 2 * day, 'START_MIGRATION', SP, 'old-delta', 'SPOLD', 'Delta')]
        rows += sp_items('SPOLD', 'old-delta', now_usec - 2 * day + 10, 2, fail=True)
        # Hidden: run started before the cutoff, item logs after it.
        rows += [r(now_usec - 15 * day, 'START_MIGRATION', OD, 'od-running', 'ODOLD', 'Full'),
                 r(now_usec - 1 * day, 'CREATE_FILE', OD, 'od-running',
                   source_name='https://tenant-my.sharepoint.com/personal/u1',
                   source_identifier='documentLibraries/dl-od/files/f1', source_type='Sharepoint File')]
        # Hidden: only a Delta is visible (Full outside the window).
        rows += [r(now_usec - 3 * day, 'START_MIGRATION', SP, 'delta-only', 'SPDELTA', 'Delta')]
        rows += sp_items('SPDELTA', 'delta-only', now_usec - 3 * day + 10, 1)
        # Hidden: only a Retry Failure run is visible.
        rows += [r(now_usec - 3 * day, 'START_MIGRATION', SP, 'retry-only', 'SPRETRY', 'Retry Failure')]
        rows += sp_items('SPRETRY', 'retry-only', now_usec - 3 * day + 10, 1)
        # Shown: Exchange is never cut off.
        rows += [r(now_usec - 20 * day, 'START_MIGRATION', EX, 'ex-old', 'EXOLD', 'Full'),
                 r(now_usec - 20 * day + 10, 'CREATE_GMAIL_MESSAGE', EX, 'ex-old', source_name='u@example.com',
                   source_identifier='msg-1', source_type='Exchange Online Email Message')]
        self._insert_activity(rows)

        self._run_with_cutoff(cutoff)

        def ids(sql):
            return sorted({row[0] for row in self.conn.execute(sql).fetchall()})

        # map_execution_wave keeps every run, with run type, start time and the reportable flag.
        flags = dict(self.conn.execute(
            "SELECT execution_id, is_reportable FROM map_execution_wave").fetchall())
        self.assertEqual(flags, {
            'new-full': True, 'notype-1': True, 'old-full': False, 'old-delta': False,
            'od-running': False, 'delta-only': False, 'retry-only': False, 'ex-old': True})
        self.assertEqual(self.conn.execute(
            "SELECT execution_type, execution_start_usec FROM map_execution_wave WHERE execution_id='old-delta'"
        ).fetchone(), ('Delta', now_usec - 2 * day))

        # Batch filter source and every reporting table only contain reportable batches.
        self.assertEqual(ids("SELECT batch_id FROM map_wave_details"), ['EXOLD', 'SPNEW', 'SPNOTYPE'])
        self.assertEqual(ids("SELECT batch_id FROM fact_file_wave_metrics"), ['SPNEW', 'SPNOTYPE'])
        self.assertEqual(ids("SELECT batch_id FROM fact_entity_file_wave_metrics"), ['SPNEW', 'SPNOTYPE'])
        self.assertEqual(ids("SELECT batch_id FROM fact_wave_metrics"), ['EXOLD'])
        self.assertEqual(ids("SELECT batch_id FROM snapshot_item_user_wave"), ['EXOLD', 'SPNEW', 'SPNOTYPE'])
        self.assertEqual(ids("SELECT batch_id FROM snapshot_user_wave"), ['EXOLD', 'SPNEW', 'SPNOTYPE'])
        self.assertEqual(ids("SELECT batch_id FROM fact_migration_errors"), ['SPNEW'])
        self.assertEqual(ids("SELECT batch_id FROM fact_migration_timeline"), ['EXOLD', 'SPNEW', 'SPNOTYPE'])
        self.assertEqual(ids("SELECT migration_error_title FROM fact_top_errors"), ['Error SPNEW'])
        self.assertEqual(ids("SELECT entity_identifier FROM fact_entity_file_overall_metrics"),
                         ['tenant.sharepoint.com/sites/SiteSPNEW', 'tenant.sharepoint.com/sites/SiteSPNOTYPE'])

        # Counts for the reportable batch are unaffected.
        self.assertEqual(self.conn.execute(
            "SELECT migrated_files_count FROM fact_file_wave_metrics WHERE batch_id='SPNEW'").fetchone(), (1,))

    def test_site_completion_event_drives_site_status(self):
        """Site status follows the completion event: SITE_MIGRATION_COMPLETE (SharePoint sites and OneDrives)."""
        self.run_sql_file(self.ddl_path)
        now = datetime.utcnow()
        now_usec = int(now.timestamp() * 1_000_000)
        hour = 3_600 * 1_000_000
        SP, OD = 'SharePoint Online Enterprise Migration', 'OneDrive Enterprise Migration'
        r = self._activity_row
        sp_done, sp_open = 'https://tenant.sharepoint.com/sites/Done/', 'https://tenant.sharepoint.com/sites/Open'
        od_user = 'https://tenant-my.sharepoint.com/personal/u1'

        def file_row(t, mtype, exec_id, site, n):
            return r(t, 'CREATE_FILE', mtype, exec_id, source_name=site,
                     source_identifier=f'documentLibraries/dl-{n}/files/f{n}', source_type='Sharepoint File')

        self._insert_activity([
            # SharePoint batch: one site finished (event), one site with successes but no event yet.
            r(now_usec - 10 * hour, 'START_MIGRATION', SP, 'sp-full', 'SPC', 'Full'),
            file_row(now_usec - 10 * hour + 1, SP, 'sp-full', sp_done, 1),
            file_row(now_usec - 10 * hour + 2, SP, 'sp-full', sp_open, 2),
            r(now_usec - 9 * hour, 'SITE_MIGRATION_COMPLETE', SP, 'sp-full', source_name=sp_done,
              source_identifier='sites/' + sp_done, source_type='Sharepoint Site'),
            # OneDrive batch: drive finished with SITE_MIGRATION_COMPLETE.
            r(now_usec - 10 * hour, 'START_MIGRATION', OD, 'od-full', 'ODC', 'Full'),
            file_row(now_usec - 10 * hour + 1, OD, 'od-full', od_user, 3),
            r(now_usec - 9 * hour, 'SITE_MIGRATION_COMPLETE', OD, 'od-full', source_name=od_user,
              source_identifier='sites/' + od_user, source_type='Sharepoint Site'),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        def site_status(batch):
            return dict(self.conn.execute(
                f"SELECT entity_identifier, status FROM fact_entity_file_wave_metrics WHERE batch_id='{batch}'").fetchall())

        self.assertEqual(site_status('SPC'), {'tenant.sharepoint.com/sites/Done': 'Completed',
                                              'tenant.sharepoint.com/sites/Open': 'Running'})
        self.assertEqual(site_status('ODC'), {'tenant-my.sharepoint.com/personal/u1': 'Completed'})
        self.assertEqual(self.conn.execute(
            "SELECT entity_count, completed_entity_count, status FROM fact_file_wave_metrics WHERE batch_id='SPC'"
        ).fetchone(), (2, 1, 'Running'))
        self.assertEqual(self.conn.execute(
            "SELECT completed_entity_count, status FROM fact_file_wave_metrics WHERE batch_id='ODC'").fetchone(),
            (1, 'Completed'))

        # Second site finishes -> batch completes.
        self.conn.execute("DELETE FROM activity")
        self._insert_activity([
            r(now_usec - 8 * hour, 'SITE_MIGRATION_COMPLETE', SP, 'sp-full', source_name=sp_open,
              source_identifier='sites/' + sp_open, source_type='Sharepoint Site'),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})
        self.assertEqual(self.conn.execute(
            "SELECT completed_entity_count, status FROM fact_file_wave_metrics WHERE batch_id='SPC'").fetchone(),
            (2, 'Completed'))

        # A new delta starts on the SharePoint batch -> the touched site goes back to Running until its event.
        self.conn.execute("DELETE FROM activity")
        self._insert_activity([
            r(now_usec - 2 * hour, 'START_MIGRATION', SP, 'sp-delta', 'SPC', 'Delta'),
            file_row(now_usec - 2 * hour + 1, SP, 'sp-delta', sp_done, 4),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})
        self.assertEqual(site_status('SPC')['tenant.sharepoint.com/sites/Done'], 'Running')
        self.assertEqual(self.conn.execute(
            "SELECT status FROM fact_file_wave_metrics WHERE batch_id='SPC'").fetchone(), ('Running',))
        # Overall page: Done is not completed in its latest run, so it is Running overall too.
        self.assertEqual(self.conn.execute(
            "SELECT status FROM fact_entity_file_overall_metrics "
            "WHERE entity_identifier='tenant.sharepoint.com/sites/Done'").fetchone(), ('Running',))

        self.conn.execute("DELETE FROM activity")
        self._insert_activity([
            r(now_usec - 1 * hour, 'SITE_MIGRATION_COMPLETE', SP, 'sp-delta', source_name=sp_done,
              source_identifier='sites/' + sp_done, source_type='Sharepoint Site'),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})
        self.assertEqual(site_status('SPC')['tenant.sharepoint.com/sites/Done'], 'Completed')
        self.assertEqual(self.conn.execute(
            "SELECT status FROM fact_file_wave_metrics WHERE batch_id='SPC'").fetchone(), ('Completed',))

    def test_file_versions_count_once_per_file(self):
        """File version rows (".../versions/<n>") are grouped under their file; older versions are not extra files."""
        self.run_sql_file(self.ddl_path)
        now = datetime.utcnow()
        now_usec = int(now.timestamp() * 1_000_000)
        SP = 'SharePoint Online Enterprise Migration'
        site = 'https://tenant.sharepoint.com/sites/Ver/'
        r = self._activity_row
        lib = 'documentLibraries/dl1/files/'

        def row(t, event, exec_id, file_id, status='SUCCEEDED', code=None):
            return r(now_usec - 100_000 + t, event, SP, exec_id, source_name=site,
                     source_identifier=lib + file_id,
                     source_type='Sharepoint File Version' if event == 'CREATE_FILE_VERSION' else 'Sharepoint File',
                     status=status, error_code=code, error_title='Err' if code else None)

        self._insert_activity([
            r(now_usec - 200_000, 'START_MIGRATION', SP, 'v-full', 'SPVER', 'Full'),
            # Site container row: not a customer item, must not appear in the timeline or folder counts.
            r(now_usec - 150_000, 'CREATE_FOLDER', SP, 'v-full', source_name=site,
              source_identifier='sites/' + site, source_type='Sharepoint Site'),
            # A: current 2.0 + older 1.0, all succeeded (older version also logs CREATE_FILE .../versions/1.0).
            row(1, 'CREATE_FILE_VERSION', 'v-full', 'A/versions/2.0'),
            row(2, 'CREATE_FILE', 'v-full', 'A', 'SUCCEEDED_WITH_WARNINGS'),
            row(3, 'CREATE_FILE_VERSION', 'v-full', 'A/versions/1.0'),
            row(4, 'CREATE_FILE', 'v-full', 'A/versions/1.0'),
            # B: current version upload failed, no CREATE_FILE row at all -> 1 failed file.
            row(5, 'CREATE_FILE_VERSION', 'v-full', 'B/versions/1.0', 'FAILED', 'T009'),
            # C: upload succeeded but the file step failed -> 1 failed file (must not be hidden by the version success).
            row(6, 'CREATE_FILE_VERSION', 'v-full', 'C/versions/1.0'),
            row(7, 'CREATE_FILE', 'v-full', 'C', 'FAILED', 'T010'),
            # D: file succeeded, an older version failed -> 0 failed files.
            row(8, 'CREATE_FILE_VERSION', 'v-full', 'D/versions/2.0'),
            row(9, 'CREATE_FILE', 'v-full', 'D'),
            row(10, 'CREATE_FILE_VERSION', 'v-full', 'D/versions/1.0', 'FAILED', 'T011'),
            row(11, 'CREATE_FILE', 'v-full', 'D/versions/1.0', 'FAILED', 'T011'),
            # E: failed in the Full run, succeeded in a later delta -> failure cleared.
            row(12, 'CREATE_FILE_VERSION', 'v-full', 'E/versions/1.0', 'FAILED', 'T009'),
            r(now_usec - 50_000, 'START_MIGRATION', SP, 'v-delta', 'SPVER', 'Delta'),
            row(60_000, 'CREATE_FILE_VERSION', 'v-delta', 'E/versions/1.0'),
            row(60_001, 'CREATE_FILE', 'v-delta', 'E'),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Snapshot holds one item per file, keyed by the file's own ID.
        items = dict(self.conn.execute(
            "SELECT source_identifier, event_status FROM snapshot_item_user_wave "
            "WHERE batch_id='SPVER' AND source_identifier LIKE 'documentLibraries/%'").fetchall())
        self.assertEqual(sorted(items), [lib + f for f in 'ABCDE'])
        self.assertEqual(items[lib + 'B'], 'FAILED')
        self.assertEqual(items[lib + 'C'], 'FAILED')
        self.assertIn(items[lib + 'D'].lower(), ('succeeded', 'succeeded_with_warnings'))
        self.assertIn(items[lib + 'E'].lower(), ('succeeded', 'succeeded_with_warnings'))

        self.assertEqual(self.conn.execute(
            "SELECT migrated_files_count, failed_items, total_items FROM fact_entity_file_wave_metrics "
            "WHERE batch_id='SPVER'").fetchone(), (3, 2, 5))
        self.assertEqual(self.conn.execute(
            "SELECT migrated_files_count, successfully_migrated_items, total_migrated_items FROM fact_file_wave_metrics "
            "WHERE batch_id='SPVER'").fetchone(), (3, 3, 5))
        # Timeline: A, D and E once each; no older-version CREATE_FILE rows, no version rows, no site container.
        self.assertEqual(self.conn.execute(
            "SELECT SUM(items_migrated) FROM fact_migration_timeline WHERE batch_id='SPVER'").fetchone(), (3,))

    def test_failed_site_container_marks_site_failed(self):
        """A failed site container marks the site Failed (even after completion) and is not a failed item."""
        self.run_sql_file(self.ddl_path)
        now = datetime.utcnow()
        now_usec = int(now.timestamp() * 1_000_000)
        hour = 3_600 * 1_000_000
        SP = 'SharePoint Online Enterprise Migration'
        r = self._activity_row
        bad, ok, only = ('https://tenant.sharepoint.com/sites/Bad/', 'https://tenant.sharepoint.com/sites/Ok/',
                         'https://tenant.sharepoint.com/sites/Only/')

        def container(t, exec_id, site, status):
            return r(t, 'CREATE_FOLDER', SP, exec_id, source_name=site, source_identifier='sites/' + site,
                     source_type='Sharepoint Site', status=status,
                     error_code='T010' if status == 'FAILED' else None, error_title='Err' if status == 'FAILED' else None)

        def file_row(t, exec_id, site, n):
            return r(t, 'CREATE_FILE', SP, exec_id, source_name=site,
                     source_identifier=f'documentLibraries/dl{n}/files/f{n}', source_type='Sharepoint File')

        def done(t, exec_id, site):
            return r(t, 'SITE_MIGRATION_COMPLETE', SP, exec_id, source_name=site,
                     source_identifier='sites/' + site, source_type='Sharepoint Site')

        self._insert_activity([
            r(now_usec - 10 * hour, 'START_MIGRATION', SP, 'c-full', 'SPCON', 'Full'),
            # Bad: container failed, files still succeed, then the completion event arrives.
            container(now_usec - 10 * hour + 1, 'c-full', bad, 'FAILED'),
            file_row(now_usec - 10 * hour + 2, 'c-full', bad, 1),
            file_row(now_usec - 10 * hour + 3, 'c-full', bad, 2),
            done(now_usec - 9 * hour, 'c-full', bad),
            # Ok: container fine, completion event.
            container(now_usec - 10 * hour + 4, 'c-full', ok, 'SUCCEEDED'),
            file_row(now_usec - 10 * hour + 5, 'c-full', ok, 3),
            done(now_usec - 9 * hour, 'c-full', ok),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        sites = {row[0]: row[1:] for row in self.conn.execute(
            "SELECT entity_identifier, migrated_files_count, failed_items, status FROM fact_entity_file_wave_metrics "
            "WHERE batch_id='SPCON'").fetchall()}
        self.assertEqual(sites['tenant.sharepoint.com/sites/Bad'], (2, 0, 'Failed'))   # files counted, not a failed item
        self.assertEqual(sites['tenant.sharepoint.com/sites/Ok'], (1, 0, 'Completed'))
        # Both sites processed -> batch Completed; Bad still shows Failed.
        self.assertEqual(self.conn.execute(
            "SELECT entity_count, completed_entity_count, status FROM fact_file_wave_metrics WHERE batch_id='SPCON'"
        ).fetchone(), (2, 2, 'Completed'))
        # Overall page and container error still visible in the errors table.
        self.assertEqual(self.conn.execute(
            "SELECT status FROM fact_entity_file_overall_metrics WHERE entity_identifier='tenant.sharepoint.com/sites/Bad'"
        ).fetchone(), ('Failed',))
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM fact_migration_errors WHERE batch_id='SPCON' AND migration_error_code='T010'"
        ).fetchone(), (1,))

        # Delta: Bad's container succeeds this time -> no longer Failed (Running until its completion event).
        self.conn.execute("DELETE FROM activity")
        self._insert_activity([
            r(now_usec - 2 * hour, 'START_MIGRATION', SP, 'c-delta', 'SPCON', 'Delta'),
            container(now_usec - 2 * hour + 1, 'c-delta', bad, 'SUCCEEDED'),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})
        self.assertEqual(self.conn.execute(
            "SELECT status FROM fact_entity_file_wave_metrics WHERE batch_id='SPCON' "
            "AND entity_identifier='tenant.sharepoint.com/sites/Bad'").fetchone(), ('Running',))
        self.assertEqual(self.conn.execute(
            "SELECT status FROM fact_file_wave_metrics WHERE batch_id='SPCON'").fetchone(), ('Running',))

        # A batch whose only site has just a failed container -> site Failed, batch Failed.
        self.conn.execute("DELETE FROM activity")
        self._insert_activity([
            r(now_usec - 1 * hour, 'START_MIGRATION', SP, 'only-full', 'SPONLY', 'Full'),
            container(now_usec - 1 * hour + 1, 'only-full', only, 'FAILED'),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})
        self.assertEqual(self.conn.execute(
            "SELECT failed_items, status FROM fact_entity_file_wave_metrics WHERE batch_id='SPONLY'").fetchone(),
            (0, 'Failed'))
        self.assertEqual(self.conn.execute(
            "SELECT status FROM fact_file_wave_metrics WHERE batch_id='SPONLY'").fetchone(), ('Failed',))

    def test_crawl_failure_does_not_hide_failed_item_with_same_id(self):
        """A failed folder and the crawl failure for its children (same ID) are both counted, as in the site report."""
        self.run_sql_file(self.ddl_path)
        now = datetime.utcnow()
        now_usec = int(now.timestamp() * 1_000_000)
        SP = 'SharePoint Online Enterprise Migration'
        site = 'https://tenant.sharepoint.com/sites/Crawl'
        folder = 'documentLibraries/dl1/folders/f-bad'
        r = self._activity_row
        self._insert_activity([
            r(now_usec - 100_000, 'START_MIGRATION', SP, 'cr-full', 'SPCRAWL', 'Full'),
            r(now_usec - 90_000, 'CREATE_FOLDER', SP, 'cr-full', source_name=site,
              source_identifier='documentLibraries/dl1/folders/f-ok', source_type='Sharepoint Folder'),
            r(now_usec - 80_000, 'CREATE_FOLDER', SP, 'cr-full', source_name=site, source_identifier=folder,
              source_type='Sharepoint Folder', status='FAILED', error_code='T007', error_title='UserNotFound'),
            # Logged later, same ID: children of the failed folder could not be crawled.
            r(now_usec - 10_000, 'CRAWL_FAILURE', SP, 'cr-full', source_name=site, source_identifier=folder,
              source_type='Sharepoint Item Crawler', status='FAILED', error_code='P009', error_title='FailedToCrawl'),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})
        self.assertEqual(self.conn.execute(
            "SELECT migrated_folders_count, failed_items, crawl_failure_items, total_items "
            "FROM fact_entity_file_wave_metrics WHERE batch_id='SPCRAWL'").fetchone(), (1, 1, 1, 2))
        self.assertEqual(self.conn.execute(
            "SELECT successfully_migrated_items, total_migrated_items FROM fact_file_wave_metrics "
            "WHERE batch_id='SPCRAWL'").fetchone(), (1, 2))

    def test_site_failure_rules_permission_resolver_and_library_only(self):
        """Site Failed: permission resolver failure; library-only mapping with a library-level discovery failure."""
        self.run_sql_file(self.ddl_path)
        now = datetime.utcnow()
        now_usec = int(now.timestamp() * 1_000_000)
        SP = 'SharePoint Online Enterprise Migration'
        r = self._activity_row
        lib_site, perm_site, dl_site, mix_site = ('https://t.sharepoint.com/sites/Lib', 'https://t.sharepoint.com/sites/Perm/',
                                                  'https://t.sharepoint.com/sites/Dl', 'https://t.sharepoint.com/sites/Mix')

        def crawl(t, exec_id, site, stype, sid, code='C002'):
            return r(t, 'CRAWL_FAILURE', SP, exec_id, source_name=site, source_identifier=sid, source_type=stype,
                     status='FAILED', error_code=code, error_title='Err')

        def done(t, exec_id, site):
            return r(t, 'SITE_MIGRATION_COMPLETE', SP, exec_id, source_name=site,
                     source_identifier='sites/' + site, source_type='Sharepoint Site')

        def file_row(t, exec_id, site, sid):
            return r(t, 'CREATE_FILE', SP, exec_id, source_name=site, source_identifier=sid, source_type='Sharepoint File')

        self._insert_activity([
            # Run 1 pattern: library-only, library discovery failed (Phase 1), no completion event.
            r(now_usec - 9_000, 'START_MIGRATION', SP, 'lib', 'B-LIB', 'Full'),
            crawl(now_usec - 8_000, 'lib', lib_site, 'Sharepoint Document Library Crawler Phase 1',
                  lib_site + '/amrJobs/scopes/DL1'),
            # Run 2 pattern: site mapping, permission resolver + site crawler failed, completion event.
            r(now_usec - 9_000, 'START_MIGRATION', SP, 'perm', 'B-PERM', 'Full'),
            crawl(now_usec - 8_000, 'perm', perm_site, 'Sharepoint Site Permission Resolver', 'sites/' + perm_site),
            crawl(now_usec - 8_000, 'perm', perm_site, 'Sharepoint Site Crawler', 'sites/' + perm_site, code=None),
            done(now_usec - 7_000, 'perm', perm_site),
            # Run 3 pattern: library-only, a folder-level item crawler failure only -> not a library failure.
            r(now_usec - 9_000, 'START_MIGRATION', SP, 'dl', 'B-DL', 'Full'),
            file_row(now_usec - 8_500, 'dl', dl_site, 'documentLibraries/d1/files/f1'),
            crawl(now_usec - 8_000, 'dl', dl_site, 'Sharepoint Item Crawler', 'documentLibraries/d1/folders/x', code='P009'),
            done(now_usec - 7_000, 'dl', dl_site),
            # Site mapping (container OK) with a library-level failure -> NOT Failed (report: completed with warnings).
            r(now_usec - 9_000, 'START_MIGRATION', SP, 'mix', 'B-MIX', 'Full'),
            r(now_usec - 8_900, 'CREATE_FOLDER', SP, 'mix', source_name=mix_site, source_identifier='sites/' + mix_site,
              source_type='Sharepoint Site'),
            file_row(now_usec - 8_500, 'mix', mix_site, 'documentLibraries/m1/files/f1'),
            crawl(now_usec - 8_000, 'mix', mix_site, 'Sharepoint Document Library Crawler Phase 2', mix_site + '/amrJobs/m2'),
            done(now_usec - 7_000, 'mix', mix_site),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        status = dict(self.conn.execute(
            "SELECT batch_id, status FROM fact_entity_file_wave_metrics").fetchall())
        self.assertEqual(status, {'B-LIB': 'Failed', 'B-PERM': 'Failed', 'B-DL': 'Completed', 'B-MIX': 'Completed'})
        batches = dict(self.conn.execute("SELECT batch_id, status FROM fact_file_wave_metrics").fetchall())
        self.assertEqual(batches, {'B-LIB': 'Failed', 'B-PERM': 'Failed', 'B-DL': 'Completed', 'B-MIX': 'Completed'})

    def test_batch_completes_with_failed_site_without_completion_event(self):
        """A library-only site that failed (no completion event) + a completed site -> batch Completed, 2/2 processed."""
        self.run_sql_file(self.ddl_path)
        now_usec = int(datetime.utcnow().timestamp() * 1_000_000)
        SP = 'SharePoint Online Enterprise Migration'
        r = self._activity_row
        bad, ok = 'https://t.sharepoint.com/sites/BadLib', 'https://t.sharepoint.com/sites/OkLib'
        self._insert_activity([
            r(now_usec - 9_000, 'START_MIGRATION', SP, 'mixed', 'B-MIXED', 'Full'),
            r(now_usec - 8_000, 'CRAWL_FAILURE', SP, 'mixed', source_name=bad,
              source_identifier=bad + '/amrJobs/scopes/DL1', source_type='Sharepoint Document Library Crawler Phase 1',
              status='FAILED', error_code='C002', error_title='TargetUserExternal'),
            r(now_usec - 8_000, 'CREATE_FILE', SP, 'mixed', source_name=ok,
              source_identifier='documentLibraries/ok/files/f1', source_type='Sharepoint File'),
            r(now_usec - 7_000, 'SITE_MIGRATION_COMPLETE', SP, 'mixed', source_name=ok,
              source_identifier='sites/' + ok, source_type='Sharepoint Site'),
        ])
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})
        self.assertEqual(dict(self.conn.execute(
            "SELECT entity_identifier, status FROM fact_entity_file_wave_metrics WHERE batch_id='B-MIXED'").fetchall()),
            {'t.sharepoint.com/sites/BadLib': 'Failed', 't.sharepoint.com/sites/OkLib': 'Completed'})
        self.assertEqual(self.conn.execute(
            "SELECT entity_count, completed_entity_count, completion_percentage, status FROM fact_file_wave_metrics "
            "WHERE batch_id='B-MIXED'").fetchone(), (2, 2, 1.0, 'Completed'))

    def test_od_sp_cutoff_incremental_runs_keep_decision(self):
        """A reportable batch stays reportable in later short-window runs, and a hidden one stays hidden."""
        self.run_sql_file(self.ddl_path)
        now = datetime.utcnow()
        day = 86_400 * 1_000_000
        now_usec = int(now.timestamp() * 1_000_000)
        cutoff = now - timedelta(days=10)
        SP = 'SharePoint Online Enterprise Migration'
        r = self._activity_row
        site = 'https://tenant.sharepoint.com/sites/Inc/'

        # Backfill: one batch after the cutoff, one before it.
        self._insert_activity([
            r(now_usec - 5 * day, 'START_MIGRATION', SP, 'inc-full', 'SPINC', 'Full'),
            r(now_usec - 5 * day + 10, 'CREATE_FILE', SP, 'inc-full', source_name=site,
              source_identifier='documentLibraries/dl1/files/f1', source_type='Sharepoint File'),
            r(now_usec - 30 * day, 'START_MIGRATION', SP, 'pre-full', 'SPPRE', 'Full'),
        ])
        self._run_with_cutoff(cutoff, lookback_days="500")

        # Later scheduled run: only new logs (the old START events are outside the 2-day window).
        self.conn.execute("DELETE FROM activity")
        self._insert_activity([
            r(now_usec - 3_600_000_000, 'START_MIGRATION', SP, 'inc-delta', 'SPINC', 'Delta'),
            r(now_usec - 3_600_000_000 + 10, 'CREATE_FILE', SP, 'inc-delta', source_name=site,
              source_identifier='documentLibraries/dl1/files/f2', source_type='Sharepoint File'),
            r(now_usec - 3_600_000_000, 'START_MIGRATION', SP, 'pre-delta', 'SPPRE', 'Delta'),
            r(now_usec - 3_600_000_000 + 10, 'CREATE_FILE', SP, 'pre-delta',
              source_name='https://tenant.sharepoint.com/sites/Pre/',
              source_identifier='documentLibraries/dl9/files/f1', source_type='Sharepoint File'),
        ])
        self._run_with_cutoff(cutoff, lookback_days="2")

        # Stored run details from the backfill are kept.
        self.assertEqual(self.conn.execute(
            "SELECT execution_type, execution_start_usec, is_reportable FROM map_execution_wave "
            "WHERE execution_id='inc-full'").fetchone(), ('Full', now_usec - 5 * day, True))
        self.assertEqual(self.conn.execute(
            "SELECT is_reportable FROM map_execution_wave WHERE execution_id='pre-delta'").fetchone(), (False,))

        self.assertEqual(self.conn.execute(
            "SELECT batch_id, migrated_files_count FROM fact_file_wave_metrics").fetchall(), [('SPINC', 2)])
        self.assertEqual(sorted(r[0] for r in self.conn.execute(
            "SELECT batch_id FROM map_wave_details").fetchall()), ['SPINC'])

if __name__ == "__main__":
    unittest.main()



