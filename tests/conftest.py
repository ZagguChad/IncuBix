"""Keep the test suite away from any developer-configured database."""

from __future__ import annotations

import atexit
import os
import shutil
import tempfile
from pathlib import Path

_test_data_dir = Path(tempfile.mkdtemp(prefix="incubrix-test-db-"))
_test_database = _test_data_dir / "test.db"
os.environ["APP_ENV"] = "test"
os.environ["DATABASE_URL"] = f"sqlite:///{_test_database.as_posix()}"
os.environ["SIMULATION_MODE"] = "true"


def _remove_test_database() -> None:
    try:
        from incubrix.db import engine

        engine.dispose()
    except ImportError:
        pass
    shutil.rmtree(_test_data_dir, ignore_errors=True)


atexit.register(_remove_test_database)
