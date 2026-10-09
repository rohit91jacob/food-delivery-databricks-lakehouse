"""Unattended daily refresh, as run by ``.github/workflows/refresh.yml`` (the hosted twin of the Airflow DAG).

Steps, each idempotent per business date:

1. ``check_credentials``: prove the configured Databricks credential works, and fail early when it is
   about to expire, so a quiet failure becomes a dated, actionable GitHub issue.
2. ``next_business_date``: the newest date in gold, plus one day. Gold is the source of truth, so a date
   that was landed but never finished processing is simply processed again.
3. generate and upload: the existing ``fd generate`` / ``fd upload`` commands.
4. ``run_job``: run the deployed Lakeflow job for the date and poll it in a bounded loop.
5. ``reconcile``: run the generator-manifest reconciliation SQL on the SQL warehouse.

Authentication is databricks-sdk unified auth, chosen by ``auth_mode`` from whichever credentials the
environment carries (GitHub OIDC federation, OAuth M2M, or a personal access token).
Everything that talks to Databricks takes a ``WorkspaceClient``, so the decision logic is unit-tested
without one.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
DEFAULT_WARN_DAYS = 14
TERMINAL_LIFE_CYCLE = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
TERMINAL_STATEMENT = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}


class RefreshError(RuntimeError):
    """A refresh step failed; the message is written to the GitHub issue as-is."""


# --------------------------------------------------------------------------------------------- auth


def auth_mode(env: Mapping[str, str]) -> str:
    """Pick the Databricks auth type from the credentials present, most to least preferred.

    * ``github-oidc``: a service-principal client ID and a GitHub OIDC token, with no stored secret
    * ``oauth-m2m``: a service-principal client ID and OAuth secret
    * ``pat``: a personal access token
    """
    explicit = env.get("DATABRICKS_AUTH_TYPE", "").strip()
    client_id = env.get("DATABRICKS_CLIENT_ID", "").strip()
    if explicit:
        mode = explicit
    elif client_id and env.get("DATABRICKS_CLIENT_SECRET", "").strip():
        mode = "oauth-m2m"
    elif client_id and env.get("ACTIONS_ID_TOKEN_REQUEST_URL", "").strip():
        mode = "github-oidc"
    elif env.get("DATABRICKS_TOKEN", "").strip():
        mode = "pat"
    else:
        raise RefreshError(
            "no Databricks credential configured: set DATABRICKS_CLIENT_ID (+ DATABRICKS_CLIENT_SECRET for "
            "OAuth M2M) or DATABRICKS_TOKEN; see docs/databricks_auth.md"
        )
    required = {
        "github-oidc": ("DATABRICKS_CLIENT_ID",),
        "oauth-m2m": ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"),
        "pat": ("DATABRICKS_TOKEN",),
    }
    if mode not in required:
        raise RefreshError(f"unsupported DATABRICKS_AUTH_TYPE {mode!r}; use one of {sorted(required)}")
    missing = [k for k in required[mode] if not env.get(k, "").strip()]
    if missing:
        raise RefreshError(f"DATABRICKS_AUTH_TYPE={mode} needs {', '.join(missing)}")
    return mode


@dataclass(frozen=True)
class Expiry:
    """Expiry verdict for one credential. ``expires_at`` is None when it never expires or is unknown."""

    ok: bool
    expires_at: datetime | None
    message: str

    @property
    def days_left(self) -> float | None:
        if self.expires_at is None:
            return None
        return (self.expires_at - datetime.now(UTC)).total_seconds() / 86400


def _expiry_verdict(name: str, expires_at: datetime | None, now: datetime, warn_days: int) -> Expiry:
    if expires_at is None:
        return Expiry(True, None, f"{name} does not expire")
    left = expires_at - now
    when = expires_at.strftime("%Y-%m-%d %H:%M UTC")
    if left <= timedelta(0):
        return Expiry(False, expires_at, f"{name} EXPIRED on {when}")
    if left < timedelta(days=warn_days):
        return Expiry(False, expires_at, f"{name} expires in {left.days} day(s), on {when}")
    return Expiry(True, expires_at, f"{name} valid until {when} ({left.days} days left)")


def _ms_to_datetime(value) -> datetime | None:
    if value in (None, "", -1, 0, "-1", "0"):
        return None
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


def evaluate_pat_expiry(
    tokens: Sequence[Mapping],
    *,
    now: datetime | None = None,
    warn_days: int = DEFAULT_WARN_DAYS,
    token_id: str | None = None,
) -> Expiry:
    """Decide from ``/api/2.0/token/list`` whether the PAT in use is close to expiring.

    The API can't say which listed token is the one in use. ``token_id`` (repo variable
    ``DATABRICKS_TOKEN_ID``) pins it. Without it, a single listed token is assumed to be the one in
    use. With several, the soonest-expiring valid one is judged, so the check may raise a false alarm
    but never misses a real expiry.
    """
    now = now or datetime.now(UTC)
    if token_id:
        match = [t for t in tokens if t.get("token_id") == token_id]
        if not match:
            return Expiry(False, None, f"DATABRICKS_TOKEN_ID {token_id[:8]}… is not among this user's tokens")
        chosen = match[0]
    elif not tokens:
        return Expiry(False, None, "the token list is empty, so the PAT in use cannot be inspected")
    else:
        valid = [t for t in tokens if (_ms_to_datetime(t.get("expiry_time")) or datetime.max.replace(tzinfo=UTC)) > now]
        pool = valid or list(tokens)
        chosen = min(pool, key=lambda t: _ms_to_datetime(t.get("expiry_time")) or datetime.max.replace(tzinfo=UTC))
    label = f"Databricks PAT '{chosen.get('comment') or chosen.get('token_id', '')[:8]}'"
    if not token_id and len(tokens) > 1:
        label += " (soonest-expiring of several; set the DATABRICKS_TOKEN_ID variable to pin it)"
    return _expiry_verdict(label, _ms_to_datetime(chosen.get("expiry_time")), now, warn_days)


def evaluate_secret_expiry(
    expires_on: str | None, *, now: datetime | None = None, warn_days: int = DEFAULT_WARN_DAYS
) -> Expiry:
    """OAuth secret expiry from the ``DATABRICKS_CLIENT_SECRET_EXPIRES`` variable (YYYY-MM-DD).

    Free Edition has no account-level API to read a secret's expiry, so the date is recorded when the
    secret is generated (at most 730 days ahead).
    """
    now = now or datetime.now(UTC)
    if not (expires_on or "").strip():
        return Expiry(
            True, None, "OAuth secret expiry unknown: set DATABRICKS_CLIENT_SECRET_EXPIRES=YYYY-MM-DD to be warned"
        )
    try:
        day = date.fromisoformat(expires_on.strip())
    except ValueError:
        return Expiry(False, None, f"DATABRICKS_CLIENT_SECRET_EXPIRES={expires_on!r} is not a YYYY-MM-DD date")
    expires_at = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return _expiry_verdict("Databricks OAuth secret", expires_at, now, warn_days)


@dataclass(frozen=True)
class CredentialReport:
    mode: str
    principal: str
    expiry: Expiry

    @property
    def ok(self) -> bool:
        return self.expiry.ok

    def as_dict(self) -> dict:
        return {
            "auth_mode": self.mode,
            "principal": self.principal,
            "ok": self.ok,
            "message": self.expiry.message,
            "expires_at": self.expiry.expires_at.isoformat() if self.expiry.expires_at else None,
        }


def check_credentials(
    w, env: Mapping[str, str] | None = None, *, warn_days: int = DEFAULT_WARN_DAYS
) -> CredentialReport:
    """Authenticate, then judge the credential's remaining lifetime."""
    env = os.environ if env is None else env
    mode = auth_mode(env)
    try:
        me = w.current_user.me()
    except Exception as exc:  # the SDK raises many types; all mean "this credential doesn't work"
        raise RefreshError(f"Databricks authentication with {mode} failed: {type(exc).__name__}: {exc}") from exc
    principal = me.user_name or me.display_name or str(me.id)
    if mode == "pat":
        tokens = [t.as_dict() for t in w.tokens.list()]
        pinned = env.get("DATABRICKS_TOKEN_ID", "").strip() or None
        expiry = evaluate_pat_expiry(tokens, warn_days=warn_days, token_id=pinned)
    elif mode == "oauth-m2m":
        expiry = evaluate_secret_expiry(env.get("DATABRICKS_CLIENT_SECRET_EXPIRES"), warn_days=warn_days)
    else:
        expiry = Expiry(True, None, "GitHub OIDC federation issues a short-lived token per run; nothing expires")
    return CredentialReport(mode, principal, expiry)


# ----------------------------------------------------------------------------------- business dates


def next_business_date(
    latest_in_gold: date | None, start: date, *, override: date | None = None, today: date | None = None
) -> date | None:
    """The date to process, or None when the simulation has caught up with today (IST)."""
    if override is not None:
        if override < start:
            raise RefreshError(f"business_date {override} is before FD_START_DATE {start}")
        return override
    candidate = start if latest_in_gold is None else max(start, latest_in_gold + timedelta(days=1))
    today = today or datetime.now(IST).date()
    return None if candidate > today else candidate


# -------------------------------------------------------------------------------------- SQL + jobs


def warehouse_id(w, name: str) -> str:
    for wh in w.warehouses.list():
        if wh.name == name:
            return wh.id
    raise RefreshError(f"SQL warehouse {name!r} not found")


def execute_sql(w, warehouse: str, sql: str, *, timeout_s: int = 900, poll_s: float = 5.0) -> list[list]:
    """Run one statement on the SQL warehouse and return its rows. A FAILED statement raises RefreshError."""
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

    resp = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse,
        wait_timeout="30s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    deadline = time.monotonic() + timeout_s
    while resp.status.state.value not in TERMINAL_STATEMENT:
        if time.monotonic() > deadline:
            w.statement_execution.cancel_execution(resp.statement_id)
            raise RefreshError(f"SQL statement timed out after {timeout_s}s (warehouse cold start or quota?)")
        time.sleep(poll_s)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state.value != "SUCCEEDED":
        err = resp.status.error.message if resp.status.error else resp.status.state.value
        raise RefreshError(f"SQL statement {resp.status.state.value}: {err}")
    return (resp.result.data_array or []) if resp.result else []


def latest_gold_date(w, warehouse: str, kpis_table: str) -> date | None:
    try:
        rows = execute_sql(w, warehouse, f"SELECT max(business_date) FROM {kpis_table}")
    except RefreshError as exc:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
            return None  # nothing processed yet
        raise
    value = rows[0][0] if rows and rows[0] else None
    return date.fromisoformat(value) if value else None


def run_job(w, job_name: str, run_date: str, *, timeout_s: int = 5400, poll_s: float = 30.0) -> dict:
    """Run the deployed job for one business date and wait in a bounded loop. Raises unless it succeeds."""
    jobs = list(w.jobs.list(name=job_name))
    if len(jobs) != 1:
        raise RefreshError(f"expected exactly one job named {job_name!r}, found {len(jobs)}; is the bundle deployed?")
    waiter = w.jobs.run_now(jobs[0].job_id, job_parameters={"run_date": run_date})
    run_id = waiter.response.run_id if hasattr(waiter, "response") else waiter.run_id
    started = time.monotonic()
    while True:
        run = w.jobs.get_run(run_id)
        life = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
        if life in TERMINAL_LIFE_CYCLE:
            break
        if time.monotonic() - started > timeout_s:
            w.jobs.cancel_run(run_id)
            raise RefreshError(f"job run {run_id} still {life} after {timeout_s}s; cancelled ({run.run_page_url})")
        log.info("job running", extra={"run_id": run_id, "state": life})
        time.sleep(poll_s)
    result = run.state.result_state.value if run.state.result_state else life
    out = {
        "run_id": run_id,
        "run_page_url": run.run_page_url,
        "life_cycle_state": life,
        "result_state": result,
        "state_message": run.state.state_message,
        "minutes": round((time.monotonic() - started) / 60, 1),
    }
    if result != "SUCCESS":
        raise RefreshError(f"job run {result}: {run.state.state_message or ''} ({run.run_page_url})")
    return out
