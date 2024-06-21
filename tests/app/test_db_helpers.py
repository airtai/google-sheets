import os
from unittest.mock import patch

from google_sheets.db_helpers import get_wasp_db_url


def test_get_wasp_db_url() -> None:
    root_db_url = "db://user:pass@localhost:5432"  # pragma: allowlist secret
    env_vars = {
        "DATABASE_URL": f"{root_db_url}/dbname",
        "WASP_DB_NAME": "waspdb",
    }
    with patch.dict(os.environ, env_vars, clear=True):
        wasp_db_url = get_wasp_db_url()
        excepted = f"{root_db_url}/waspdb?connect_timeout=60"

        assert wasp_db_url == excepted
