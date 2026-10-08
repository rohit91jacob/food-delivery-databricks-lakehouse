"""Print the FD_* settings that point Airflow / the CLI at a deployed bundle target.

Dev deployments prefix names (schema ``dev_<user>_fooddelivery``, job ``[dev <user>] fooddelivery_daily``),
so read them from the deployed bundle instead of guessing:

    python scripts/bundle_env.py dev >> .env
"""

from __future__ import annotations

import json
import subprocess
import sys


def main(target: str) -> int:
    out = subprocess.run(
        ["databricks", "bundle", "summary", "-t", target, "-o", "json"], check=True, capture_output=True, text=True
    ).stdout
    res = json.loads(out)["resources"]
    schema = res["schemas"]["fooddelivery"]
    print(f"FD_CATALOG={schema['catalog_name']}")
    print(f"FD_SCHEMA={schema['name']}")
    print(f"FD_VOLUME={res['volumes']['landing']['name']}")
    print(f"FD_DATABRICKS_JOB_NAME={res['jobs']['fooddelivery_daily']['name']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "dev"))
