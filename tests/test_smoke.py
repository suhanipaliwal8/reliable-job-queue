import jobqueue
import re
from jobqueue.config import get_db_config

def test_package_imports_and_has_a_version():
    assert re.fullmatch(r"\d+\.\d+\.\d+", jobqueue.__version__)

def test_tests_run_against_a_dedicated_test_database():
    assert get_db_config()["database"].endswith("_test")