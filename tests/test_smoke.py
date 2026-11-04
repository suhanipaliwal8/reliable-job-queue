import jobqueue
from jobqueue.config import get_db_config


def test_package_imports():
    assert jobqueue.__version__ == "0.1.0"

def test_tests_run_against_a_dedicated_test_database():
    assert get_db_config()["database"].endswith("_test")