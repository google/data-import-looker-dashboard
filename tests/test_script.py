import os
import sys
import shutil
import tempfile
import subprocess
import unittest
import json

gcloud_code = """import sys
import os
import json

args = sys.argv[1:]
log_file = os.environ.get("MOCK_LOG_FILE")
if log_file:
    with open(log_file, "a") as f:
        f.write(json.dumps({"cmd": "gcloud", "args": args}) + "\\n")

if os.environ.get("MOCK_GCLOUD_FAIL") == "1":
    sys.exit(1)
sys.exit(0)
"""

bq_code = """import sys
import os
import json

args = sys.argv[1:]
stdin_content = ""

if "query" in args:
    try:
        stdin_content = sys.stdin.read()
    except Exception:
        pass

log_file = os.environ.get("MOCK_LOG_FILE")
if log_file:
    with open(log_file, "a") as f:
        f.write(json.dumps({"cmd": "bq", "args": args, "stdin": stdin_content}) + "\\n")

# Keep track of retry attempts in a file
attempt_file = os.path.join(os.path.dirname(log_file), "bq_attempt.txt")
attempt = 1
if os.path.exists(attempt_file):
    with open(attempt_file, "r") as f:
        try:
            attempt = int(f.read().strip()) + 1
        except Exception:
            pass
with open(attempt_file, "w") as f:
    f.write(str(attempt))

if "query" in args:
    if os.environ.get("MOCK_BQ_QUERY_FAIL") == "1":
        sys.exit(1)
    if "CREATE TABLE" in stdin_content and os.environ.get("MOCK_BQ_DDL_FAIL") == "1" and attempt == 1:
        sys.exit(1)
    if "MERGE" in stdin_content and os.environ.get("MOCK_BQ_DML_FAIL") == "1":
        sys.exit(1)

if "mk" in args and os.environ.get("MOCK_BQ_MK_FAIL") == "1":
    sys.exit(1)

if "rm" in args and os.environ.get("MOCK_BQ_RM_FAIL") == "1":
    sys.exit(1)

if "ls" in args:
    ls_output = os.environ.get("MOCK_BQ_LS_OUTPUT", "[]")
    print(ls_output)
    sys.exit(0)

sys.exit(0)
"""

jq_code = """import sys
import os
import json

args = sys.argv[1:]
stdin_content = ""
if "-n" not in args:
    try:
        stdin_content = sys.stdin.read()
    except Exception:
        pass

log_file = os.environ.get("MOCK_LOG_FILE")
if log_file:
    with open(log_file, "a") as f:
        f.write(json.dumps({"cmd": "jq", "args": args, "stdin": stdin_content}) + "\\n")

if os.environ.get("MOCK_JQ_FAIL") == "1":
    sys.exit(1)

if "-n" in args:
    try:
        q_idx = args.index("q")
        q_val = args[q_idx + 1]
        print(json.dumps({"query": q_val}))
    except Exception:
        print("{}")
    sys.exit(0)
else:
    if not stdin_content.strip():
        sys.exit(0)
    try:
        data = json.loads(stdin_content)
        name_idx = args.index("name")
        target_name = args[name_idx + 1]
        
        names = []
        def traverse(obj):
            if isinstance(obj, dict):
                if obj.get("displayName") == target_name and "name" in obj:
                    names.append(obj["name"])
                for v in obj.values():
                    traverse(v)
            elif isinstance(obj, list):
                for item in obj:
                    traverse(item)
        traverse(data)
        for name in names:
            print(name)
    except Exception:
        sys.exit(1)
    sys.exit(0)
"""

class TestDeploymentScript(unittest.TestCase):

    def setUp(self):
        # Create temporary execution environment in the workspace
        self.workspace_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        self.test_dir = tempfile.mkdtemp(dir=self.workspace_dir, prefix="tmp_test_")
        
        self.mock_bin = os.path.join(self.test_dir, "mock_bin")
        self.safe_bin = os.path.join(self.test_dir, "safe_bin")
        os.makedirs(self.mock_bin)
        os.makedirs(self.safe_bin)
        
        self.log_dir = os.path.join(self.test_dir, "logs")
        os.makedirs(self.log_dir)
        self.mock_log_file = os.path.join(self.log_dir, "mock_calls.jsonl")
        
        # Symlink safe commands
        os.symlink("/usr/bin/sed", os.path.join(self.safe_bin, "sed"))
        os.symlink("/bin/date", os.path.join(self.safe_bin, "date"))
        os.symlink("/bin/bash", os.path.join(self.safe_bin, "bash"))
        os.symlink(sys.executable, os.path.join(self.safe_bin, "python3"))
        
        # Copy scripts and SQLs
        self.script_path = os.path.join(self.test_dir, "script.sh")
        shutil.copy(os.path.join(self.workspace_dir, "script.sh"), self.script_path)
        os.chmod(self.script_path, 0o755)
        
        self.ddl_path = os.path.join(self.test_dir, "ddl.sql")
        self.dml_path = os.path.join(self.test_dir, "dml.sql")
        shutil.copy(os.path.join(self.workspace_dir, "ddl.sql"), self.ddl_path)
        shutil.copy(os.path.join(self.workspace_dir, "dml.sql"), self.dml_path)
        
        self.env = {
            "PATH": f"{self.mock_bin}:{self.safe_bin}",
            "MOCK_LOG_FILE": self.mock_log_file,
            "HOME": os.environ.get("HOME", "")
        }

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def write_mock_binary(self, name, python_code):
        path = os.path.join(self.mock_bin, name)
        with open(path, "w") as f:
            f.write("#!/usr/bin/env python3\n")
            f.write(python_code)
        os.chmod(path, 0o755)

    def setup_standard_mocks(self, include_jq=True):
        self.write_mock_binary("gcloud", gcloud_code)
        self.write_mock_binary("bq", bq_code)
        if include_jq:
            self.write_mock_binary("jq", jq_code)

    def run_script(self, stdin_input, extra_env=None):
        env = self.env.copy()
        if extra_env:
            env.update(extra_env)
            
        stdout_file_path = os.path.join(self.log_dir, "script_stdout.log")
        stderr_file_path = os.path.join(self.log_dir, "script_stderr.log")
        stdout_file = open(stdout_file_path, "w")
        stderr_file = open(stderr_file_path, "w")
        
        process = subprocess.Popen(
            ["bash", self.script_path],
            stdin=subprocess.PIPE,
            stdout=stdout_file,
            stderr=stderr_file,
            cwd=self.test_dir,
            env=env,
            text=True
        )
        process.stdin.write(stdin_input)
        process.stdin.flush()
        process.stdin.close()
        
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout_file.close()
            stderr_file.close()
            with open(stdout_file_path, "r") as f:
                stdout_content = f.read()
            with open(stderr_file_path, "r") as f:
                stderr_content = f.read()
            raise TimeoutError(f"Script hung! Captured stdout:\n{stdout_content}\nCaptured stderr:\n{stderr_content}")
            
        stdout_file.close()
        stderr_file.close()
        
        with open(stdout_file_path, "r") as f:
            stdout = f.read()
        with open(stderr_file_path, "r") as f:
            stderr = f.read()
            
        return process.returncode, stdout, stderr

    def get_mock_calls(self):
        if not os.path.exists(self.mock_log_file):
            return []
        calls = []
        with open(self.mock_log_file, "r") as f:
            for line in f:
                if line.strip():
                    calls.append(json.loads(line))
        return calls

    def test_happy_path(self):
        self.setup_standard_mocks()
        
        inputs = "my-test-project\nUS\nGWS_Audit\nevery day 02:00\n"
        returncode, stdout, stderr = self.run_script(inputs)
        
        self.assertEqual(returncode, 0, f"Script failed with stderr:\n{stderr}")
        self.assertIn("DMS Import ANALYTICS - AUTOMATED DEPLOYMENT", stdout)
        self.assertIn("✅ DEPLOYMENT COMPLETE", stdout)
        self.assertIn("lookerstudio.google.com/reporting/create", stdout)
        
        calls = self.get_mock_calls()
        
        # gcloud
        gcloud_calls = [c for c in calls if c["cmd"] == "gcloud"]
        self.assertEqual(len(gcloud_calls), 2)
        
        # bq calls (DDL, DML, LS, MK)
        bq_calls = [c for c in calls if c["cmd"] == "bq"]
        self.assertEqual(len(bq_calls), 4)
        
        # Check DDL content
        self.assertIn("CREATE TABLE IF NOT EXISTS `my-test-project.GWS_Audit.map_execution_wave`", bq_calls[0]["stdin"])
        
        # Check DML Backfill content (lookback_days=365)
        self.assertIn("INTERVAL 365 DAY", bq_calls[1]["stdin"])
        self.assertIn("`my-test-project.GWS_Audit.activity`", bq_calls[1]["stdin"])
        
        # Check scheduled query config setup
        self.assertIn("mk", bq_calls[3]["args"])
        self.assertIn("--schedule=every day 02:00", bq_calls[3]["args"])
        
        # Check লুকer link replacement
        self.assertIn("projectId=my-test-project", stdout)
        self.assertIn("datasetId=GWS_Audit", stdout)

    def test_empty_project_id_validation(self):
        self.setup_standard_mocks()
        inputs = "\n\nmy-test-project\n\n\n\n"
        returncode, stdout, stderr = self.run_script(inputs)
        
        self.assertEqual(returncode, 0)
        self.assertIn("Project ID cannot be empty.", stdout)
        self.assertEqual(stdout.count("Project ID cannot be empty."), 2)

    def test_missing_sql_files(self):
        self.setup_standard_mocks()
        os.remove(self.ddl_path)
        
        inputs = "my-test-project\n\n\n\ne\n"
        returncode, stdout, stderr = self.run_script(inputs)
        
        self.assertEqual(returncode, 1)
        self.assertIn("ddl.sql' or 'dml.sql' not found", stdout)
        self.assertIn("👋 Exiting.", stdout)

    def test_jq_missing(self):
        self.setup_standard_mocks(include_jq=False)
        
        inputs = "my-test-project\n\n\n\n"
        returncode, stdout, stderr = self.run_script(inputs)
        
        self.assertEqual(returncode, 0)
        self.assertIn("'jq' is not installed. Skipping scheduled query creation", stdout)
        
        calls = self.get_mock_calls()
        bq_calls = [c for c in calls if c["cmd"] == "bq"]
        self.assertEqual(len(bq_calls), 2)  # DDL, DML backfill only

    def test_ddl_failure_exit(self):
        self.setup_standard_mocks()
        
        inputs = "my-test-project\n\n\n\ne\n"
        returncode, stdout, stderr = self.run_script(inputs, extra_env={"MOCK_BQ_DDL_FAIL": "1"})
        
        self.assertEqual(returncode, 1)
        self.assertIn("Error: DDL execution failed.", stdout)
        self.assertIn("👋 Exiting.", stdout)

    def test_dml_failure_exit(self):
        self.setup_standard_mocks()
        
        inputs = "my-test-project\n\n\n\ne\n"
        returncode, stdout, stderr = self.run_script(inputs, extra_env={"MOCK_BQ_DML_FAIL": "1"})
        
        self.assertEqual(returncode, 1)
        self.assertIn("Error: DML execution failed.", stdout)
        self.assertIn("👋 Exiting.", stdout)

    def test_schedule_creation_failure_exit(self):
        self.setup_standard_mocks()
        
        inputs = "my-test-project\n\n\n\ne\n"
        returncode, stdout, stderr = self.run_script(inputs, extra_env={"MOCK_BQ_MK_FAIL": "1"})
        
        self.assertEqual(returncode, 1)
        self.assertIn("Error: Failed to create scheduled query.", stdout)

    def test_ddl_failure_retry_then_success(self):
        self.setup_standard_mocks()
        
        # User answers 'r' to retry, and next attempt succeeds because we only fail the first run
        inputs = "my-test-project\n\n\n\nr\n"
        returncode, stdout, stderr = self.run_script(inputs, extra_env={"MOCK_BQ_DDL_FAIL": "1"})
        
        self.assertEqual(returncode, 0)
        self.assertIn("Restarting deployment...", stdout)
        
        calls = self.get_mock_calls()
        bq_calls = [c for c in calls if c["cmd"] == "bq"]
        
        # Should have 5 calls: DDL (fail), DDL (success), DML, LS, MK
        self.assertEqual(len(bq_calls), 5)
        self.assertIn("CREATE TABLE", bq_calls[0]["stdin"])
        self.assertIn("CREATE TABLE", bq_calls[1]["stdin"])

    def test_existing_schedule_cleanup(self):
        self.setup_standard_mocks()
        
        existing_configs = [
            {
                "name": "projects/my-proj/locations/US/transferConfigs/cfg-111",
                "displayName": "Data Import Dashboard Refresh"
            }
        ]
        
        inputs = "my-test-project\nUS\nGWS_Audit\nevery day 02:00\n"
        returncode, stdout, stderr = self.run_script(
            inputs, 
            extra_env={"MOCK_BQ_LS_OUTPUT": json.dumps(existing_configs)}
        )
        
        self.assertEqual(returncode, 0)
        self.assertIn("🗑️ Found existing scheduled query: projects/my-proj/locations/US/transferConfigs/cfg-111. Deleting...", stdout)
        self.assertIn("✅ Existing scheduled query deleted: projects/my-proj/locations/US/transferConfigs/cfg-111", stdout)
        
        calls = self.get_mock_calls()
        bq_calls = [c for c in calls if c["cmd"] == "bq"]
        rm_calls = [c for c in bq_calls if "rm" in c["args"]]
        self.assertEqual(len(rm_calls), 1)
        self.assertIn("projects/my-proj/locations/US/transferConfigs/cfg-111", rm_calls[0]["args"])

    def test_existing_schedule_cleanup_failure_continues(self):
        self.setup_standard_mocks()
        
        existing_configs = [
            {
                "name": "projects/my-proj/locations/US/transferConfigs/cfg-111",
                "displayName": "Data Import Dashboard Refresh"
            }
        ]
        
        inputs = "my-test-project\nUS\nGWS_Audit\nevery day 02:00\n"
        returncode, stdout, stderr = self.run_script(
            inputs, 
            extra_env={
                "MOCK_BQ_LS_OUTPUT": json.dumps(existing_configs),
                "MOCK_BQ_RM_FAIL": "1"
            }
        )
        
        self.assertEqual(returncode, 0)
        self.assertIn("Warning: Failed to delete existing scheduled query: projects/my-proj/locations/US/transferConfigs/cfg-111. Attempting to proceed...", stdout)
        
        calls = self.get_mock_calls()
        bq_calls = [c for c in calls if c["cmd"] == "bq"]
        
        # mk should still be called even though rm failed
        mk_calls = [c for c in bq_calls if "mk" in c["args"]]
        self.assertEqual(len(mk_calls), 1)

    def test_multiple_existing_schedules_cleanup(self):
        self.setup_standard_mocks()
        
        existing_configs = [
            {
                "name": "projects/my-proj/locations/US/transferConfigs/cfg-111",
                "displayName": "Data Import Dashboard Refresh"
            },
            {
                "name": "projects/my-proj/locations/US/transferConfigs/cfg-222",
                "displayName": "Data Import Dashboard Refresh"
            }
        ]
        
        inputs = "my-test-project\nUS\nGWS_Audit\nevery day 02:00\n"
        returncode, stdout, stderr = self.run_script(
            inputs, 
            extra_env={"MOCK_BQ_LS_OUTPUT": json.dumps(existing_configs)}
        )
        
        self.assertEqual(returncode, 0)
        self.assertIn("projects/my-proj/locations/US/transferConfigs/cfg-111", stdout)
        self.assertIn("projects/my-proj/locations/US/transferConfigs/cfg-222", stdout)
        
        calls = self.get_mock_calls()
        bq_calls = [c for c in calls if c["cmd"] == "bq"]
        rm_calls = [c for c in bq_calls if "rm" in c["args"]]
        self.assertEqual(len(rm_calls), 2)
        
        deleted_configs = [c["args"][-1] for c in rm_calls]
        self.assertIn("projects/my-proj/locations/US/transferConfigs/cfg-111", deleted_configs)
        self.assertIn("projects/my-proj/locations/US/transferConfigs/cfg-222", deleted_configs)

if __name__ == '__main__':
    unittest.main()
