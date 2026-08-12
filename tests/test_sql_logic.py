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
            ({now_usec - 80000}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user1@example.com', 'source_identifier': 'msg-1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated Item 2 (Calendar - Success)
            ({now_usec - 70000}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user1@example.com', 'source_identifier': 'cal-1', 'source_type': 'Exchange Online Calendar Event', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated Item 3 (Contact - Success)
            ({now_usec - 60000}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: 101', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user1@example.com', 'source_identifier': 'con-1', 'source_type': 'Exchange Online Contact', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Migrated Item 4 (Email - Failed)
            ({now_usec - 50000}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Server timeout'}},
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
        data_type, batch_id, batch_name, filter_lbl, start_date, user_cnt, comp_user, succ_items, tot_items, email_cnt, cal_cnt, con_cnt, file_cnt, file_ver_cnt, fld_cnt, crawl_cnt, succ_pct, comp_pct, avg_items, status = wave_metrics[0]
        
        self.assertEqual(user_cnt, 1)
        self.assertEqual(comp_user, 1)
        self.assertEqual(succ_items, 3) # Email, Calendar, Contact succeeded
        self.assertEqual(tot_items, 4) # 3 succeeded + 1 failed
        self.assertEqual(email_cnt, 1)
        self.assertEqual(cal_cnt, 1)
        self.assertEqual(con_cnt, 1)
        self.assertEqual(file_cnt, 0)
        self.assertEqual(file_ver_cnt, 0)
        self.assertEqual(fld_cnt, 0)
        self.assertEqual(crawl_cnt, 0)
        self.assertEqual(succ_pct, 0.75) # 3 / 4
        self.assertEqual(comp_pct, 1.0) # 1 / 1 user completed
        self.assertEqual(status, "Completed")

        # 6. Assertions on fact user metrics
        user_metrics = self.conn.execute("SELECT * FROM fact_user_wave_metrics").fetchall()
        self.assertEqual(len(user_metrics), 1)
        self.assertEqual(user_metrics[0][4], "user1@example.com") # user_identifier
        self.assertEqual(user_metrics[0][17], "Completed") # status

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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-Old', 'Old Wave Batch', 'Old Wave Batch (ID: Wave-Old)', 'exec-old', TRUE);
            
            INSERT INTO map_wave_details VALUES 
            ('Exchange Online Migration', 'Wave-Old', 'Old Wave Batch', 'Old Wave Batch (ID: Wave-Old)');

            INSERT INTO snapshot_item_user_wave VALUES 
            ('Exchange Online Migration', 'Wave-Old', 'user-old@example.com', 'msg-old', 'MIGRATED_ITEM', 'Exchange Online Email Message', 'SUCCEEDED', {old_time_usec}, 'exec-old');

            INSERT INTO snapshot_user_wave VALUES 
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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-Metrics', 'Metrics Batch', 'Metrics (ID: Wave-Metrics)', 'exec-1', TRUE);
        """)

        # 2. Insert various items:
        # - 1 Email Success (SUCCEEDED)
        # - 1 Calendar Success with Warning (SUCCEEDED_WITH_WARNINGS)
        # - 1 Failed event (FAILED)
        # - 1 Crawl Failure (event_name=CRAWL_FAILURE) -> should be filtered out from metrics
        # - 1 Success with wrong source type -> should be filtered out from metrics
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            -- Email Success
            ({now_usec - 50}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Metrics', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user@example.com', 'source_identifier': 'i1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            -- Calendar Warning
            ({now_usec - 40}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED_WITH_WARNINGS', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Metrics', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user@example.com', 'source_identifier': 'i2', 'source_type': 'Exchange Online Calendar Event', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            -- Failed Event
            ({now_usec - 30}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Fatal Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Metrics', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user@example.com', 'source_identifier': 'i3', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'ERR-100', 'migration_error_title': 'Fatal Error'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            -- Crawl Failure
            ({now_usec - 20}, 'data_migration', 'CRAWL_FAILURE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Failed to read directory'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Metrics', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'user@example.com', 'source_identifier': 'i4', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'CRAWL_ERR', 'migration_error_title': 'Crawl failed'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),

            -- Success with wrong source_type
            ({now_usec - 10}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
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
            INSERT INTO map_execution_wave VALUES 
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
            INSERT INTO map_execution_wave VALUES 
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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-Retry', 'Retry Wave', 'Retry (ID: Wave-Retry)', 'exec-1', FALSE),
            ('Exchange Online Migration', 'Wave-Retry', 'Retry Wave', 'Retry (ID: Wave-Retry)', 'exec-2', TRUE);
        """)

        # 2. Log first: failure in exec-1 for msg-retry
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 1000}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Connection reset'}},
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
            ({now_usec}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-Intra', 'Intra Wave', 'Intra (ID: Wave-Intra)', 'exec-1', TRUE);
        """)

        # Insert failure event first, then success event shortly after, but within the same daily window run
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Timeout'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Intra', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'item-dup', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'TIMEOUT', 'migration_error_title': 'Timeout'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            ({now_usec + 10}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-Err', 'Err Wave', 'Err (ID: Wave-Err)', 'exec-1', TRUE);
        """)

        # Insert two identical error logs (representing duplicates)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Err', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'msg-1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            ({now_usec}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth Error'}},
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
            INSERT INTO map_execution_wave VALUES 
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
            ({now_usec}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Time', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u@example.com', 'source_identifier': 'i1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': NULL, 'migration_error_title': NULL}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
             
            -- 14 months ago success
            ({old_time_usec}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-A', 'A Batch', 'A (ID: Wave-A)', 'exec-a', TRUE),
            ('Exchange Online Migration', 'Wave-B', 'B Batch', 'B (ID: Wave-B)', 'exec-b', TRUE);
            
            -- Setup old completed wave snapshot
            INSERT INTO snapshot_user_wave VALUES 
            ('Exchange Online Migration', 'Wave-A', 'u-multi@example.com', {now_usec - 100}, TRUE, 'exec-a', {now_usec - 100});
            INSERT INTO snapshot_item_user_wave VALUES 
            ('Exchange Online Migration', 'Wave-A', 'u-multi@example.com', 'item-a', 'MIGRATED_ITEM', 'Exchange Online Email Message', 'SUCCEEDED', {now_usec - 100}, 'exec-a');

            -- Setup new in-progress wave logs
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth'}},
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
            INSERT INTO map_execution_wave VALUES 
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
            INSERT INTO map_execution_wave VALUES 
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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-Rollback', 'Rollback Batch', 'Rollback (ID: Wave-Rollback)', 'exec-1', TRUE);
            
            INSERT INTO snapshot_user_wave VALUES 
            ('Exchange Online Migration', 'Wave-Rollback', 'u1@example.com', {now_usec - 10000}, TRUE, 'exec-1', {now_usec - 10000});
        """)

        # 2. Simulate a new run exec-2 starting today (active in logs, but no completion event yet)
        self.conn.execute(f"""
            UPDATE map_execution_wave SET is_latest_execution = FALSE WHERE execution_id = 'exec-1';
            
            INSERT INTO map_execution_wave VALUES 
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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-Clear', 'Clear Wave', 'Clear (ID: Wave-Clear)', 'exec-1', TRUE);
        """)

        # Insert failure event in activity for exec-1
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 1000}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth Failure'}},
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
            
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-Clear', 'Clear Wave', 'Clear (ID: Wave-Clear)', 'exec-2', TRUE);

            -- Success event for u@example.com in exec-2
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
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
            ({now_usec - 80000}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'SUCCEEDED', 'error_message': NULL}},
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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-Fail-Test', 'Fail Test Batch', 'Fail Test', 'exec-fail', TRUE);
            
            INSERT INTO snapshot_user_wave VALUES 
            ('Exchange Online Migration', 'Wave-Fail-Test', 'u1@example.com', {now_usec}, FALSE, 'exec-fail', {now_usec}),
            ('Exchange Online Migration', 'Wave-Fail-Test', 'u2@example.com', {now_usec}, FALSE, 'exec-fail', {now_usec}),
            ('Exchange Online Migration', 'Wave-Fail-Test', 'u3@example.com', {now_usec}, FALSE, 'exec-fail', {now_usec}),
            ('Exchange Online Migration', 'Wave-Fail-Test', 'u4@example.com', {now_usec}, TRUE, 'exec-fail', {now_usec});
            
            -- User 1 item failure event
            INSERT INTO activity VALUES 
            ({now_usec + 10}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Fail-Test', 'target_identifier': NULL, 'execution_id': 'exec-fail', 'source_name': 'u1@example.com', 'source_identifier': 'item-1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '500', 'migration_error_title': 'Err'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}');

            -- User 2 crawl failure event
            INSERT INTO activity VALUES 
            ({now_usec + 20}, 'data_migration', 'CRAWL_FAILURE', 'migration', {{'event_status': 'FAILED', 'error_message': 'Crawl Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-Fail-Test', 'target_identifier': NULL, 'execution_id': 'exec-fail', 'source_name': 'u2@example.com', 'source_identifier': 'item-2', 'source_type': 'Exchange Online Email Message', 'migration_error_code': 'CRAWL_ERR', 'migration_error_title': 'Crawl Err'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}');

            -- User 4 item failure event
            INSERT INTO activity VALUES 
            ({now_usec + 30}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error'}},
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
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-1', 'Wave 1', 'Wave 1 Filter', 'exec-1', TRUE);
        """)

        # Insert events in activity
        # Title A: 3 occurrences (u1)
        # Title B: 1 occurrence (u2)
        # Title C: 5 occurrences (u3)
        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 100}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u1@example.com', 'source_identifier': 'i1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 90}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u1@example.com', 'source_identifier': 'i2', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 80}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u1@example.com', 'source_identifier': 'i3', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            ({now_usec - 70}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error B'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u2@example.com', 'source_identifier': 'i4', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '500', 'migration_error_title': 'Internal Error'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            
            ({now_usec - 60}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i5', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 50}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i6', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 40}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i7', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 30}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i8', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 20}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error C'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u3@example.com', 'source_identifier': 'i9', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '403', 'migration_error_title': 'Forbidden'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 15}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u4@example.com', 'source_identifier': 'i10', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 10}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Error A'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-1', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u4@example.com', 'source_identifier': 'i11', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run DML
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify fact_top_errors has unique title rows ordered by count desc:
        # 1. Forbidden (5), u3@example.com
        # 2. Unauthorized (5), u1@example.com (sum is 5, u1 has higher count 3 than u4's 2)
        # 3. Internal Error (1), u2@example.com
        rows = self.conn.execute("SELECT migration_error_title, occurrence_count, user_identifier FROM fact_top_errors ORDER BY occurrence_count DESC, migration_error_title").fetchall()
        self.assertEqual(rows, [
            ('Forbidden', 5, 'u3@example.com'),
            ('Unauthorized', 5, 'u1@example.com'),
            ('Internal Error', 1, 'u2@example.com')
        ])

    def test_latest_execution_per_batch_errors(self):
        """Test that fact_migration_errors only contains errors from the absolute latest execution of the batch."""
        self.run_sql_file(self.ddl_path)

        now_ts = datetime.utcnow()
        now_usec = int(now_ts.timestamp() * 1_000_000)

        # 1. Setup execution exec-1 (initial run with failures for u1 and u2)
        self.conn.execute(f"""
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-A', 'Wave A', 'Wave A Filter', 'exec-1', TRUE);
        """)

        self.conn.execute(f"""
            INSERT INTO activity VALUES 
            ({now_usec - 1000}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Auth Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-A', 'target_identifier': NULL, 'execution_id': 'exec-1', 'source_name': 'u1@example.com', 'source_identifier': 'i1', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '401', 'migration_error_title': 'Unauthorized'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}'),
            ({now_usec - 900}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Forbidden'}},
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
            
            INSERT INTO map_execution_wave VALUES 
            ('Exchange Online Migration', 'Wave-A', 'Wave A', 'Wave A Filter', 'exec-2', TRUE);

            -- u1 fails with 500 error in exec-2
            INSERT INTO activity VALUES 
            ({now_usec}, 'data_migration', 'MIGRATED_ITEM', 'migration', {{'event_status': 'FAILED', 'error_message': 'Internal Error'}},
             {{'migration_type': 'Exchange Online Migration', 'target_uri': 'WaveId: Wave-A', 'target_identifier': NULL, 'execution_id': 'exec-2', 'source_name': 'u1@example.com', 'source_identifier': 'i3', 'source_type': 'Exchange Online Email Message', 'migration_error_code': '500', 'migration_error_title': 'Internal'}},
             '{now_ts.strftime('%Y-%m-%d %H:%M:%S')}')
        """)

        # Run DML for second execution
        self.run_sql_file(self.dml_path, replacements={"{lookback_days}": "2"})

        # Verify that ONLY u1's new error from exec-2 exists in the table. u2's old error is gone.
        err_rows = self.conn.execute("SELECT user_identifier, migration_error_code FROM fact_migration_errors").fetchall()
        self.assertEqual(err_rows, [('u1@example.com', '500')])

if __name__ == "__main__":
    unittest.main()
