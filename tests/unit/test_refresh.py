"""Decision logic of the scheduled refresh: auth selection, credential expiry, next date, job polling."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest

from fdtest import REPO
from fooddelivery.config import Paths
from fooddelivery.refresh import (
    RefreshError,
    auth_mode,
    check_credentials,
    evaluate_pat_expiry,
    evaluate_secret_expiry,
    next_business_date,
    run_job,
)
from fooddelivery.seeding import install_committed_seed

NOW = datetime(2026, 10, 9, 6, 0, tzinfo=UTC)
START = date(2026, 9, 1)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# ------------------------------------------------------------------------------------------ auth mode


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({"DATABRICKS_TOKEN": "t"}, "pat"),
        ({"DATABRICKS_CLIENT_ID": "c", "DATABRICKS_CLIENT_SECRET": "s"}, "oauth-m2m"),
        ({"DATABRICKS_CLIENT_ID": "c", "ACTIONS_ID_TOKEN_REQUEST_URL": "https://gh"}, "github-oidc"),
        # a client secret wins over OIDC, and a service principal wins over a PAT
        (
            {"DATABRICKS_CLIENT_ID": "c", "DATABRICKS_CLIENT_SECRET": "s", "ACTIONS_ID_TOKEN_REQUEST_URL": "u"},
            "oauth-m2m",
        ),
        ({"DATABRICKS_CLIENT_ID": "c", "DATABRICKS_CLIENT_SECRET": "s", "DATABRICKS_TOKEN": "t"}, "oauth-m2m"),
        ({"DATABRICKS_AUTH_TYPE": "pat", "DATABRICKS_TOKEN": "t", "DATABRICKS_CLIENT_ID": "c"}, "pat"),
    ],
)
def test_auth_mode_prefers_service_principals(env, expected):
    assert auth_mode(env) == expected


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({}, "no Databricks credential configured"),
        ({"DATABRICKS_AUTH_TYPE": "oauth-m2m", "DATABRICKS_CLIENT_ID": "c"}, "needs DATABRICKS_CLIENT_SECRET"),
        ({"DATABRICKS_AUTH_TYPE": "azure-cli"}, "unsupported DATABRICKS_AUTH_TYPE"),
    ],
)
def test_auth_mode_rejects_incomplete_config(env, message):
    with pytest.raises(RefreshError, match=message):
        auth_mode(env)


# ---------------------------------------------------------------------------------------- PAT expiry


def test_single_pat_inside_the_warning_window_fails():
    tokens = [{"token_id": "a1", "comment": "NewToken", "expiry_time": _ms(NOW + timedelta(days=12))}]
    verdict = evaluate_pat_expiry(tokens, now=NOW)
    assert not verdict.ok
    assert "expires in 12 day(s)" in verdict.message
    assert "NewToken" in verdict.message


def test_pat_far_from_expiry_and_non_expiring_pat_pass():
    far = [{"token_id": "a1", "expiry_time": _ms(NOW + timedelta(days=400))}]
    assert evaluate_pat_expiry(far, now=NOW).ok
    never = [{"token_id": "a1", "expiry_time": -1}]
    verdict = evaluate_pat_expiry(never, now=NOW)
    assert verdict.ok
    assert verdict.expires_at is None


def test_expired_pat_fails():
    tokens = [{"token_id": "a1", "expiry_time": _ms(NOW - timedelta(hours=1))}]
    verdict = evaluate_pat_expiry(tokens, now=NOW)
    assert not verdict.ok
    assert "EXPIRED" in verdict.message


def test_several_pats_judge_the_soonest_valid_one_unless_pinned():
    tokens = [
        {"token_id": "old", "comment": "laptop", "expiry_time": _ms(NOW + timedelta(days=5))},
        {"token_id": "ci", "comment": "github", "expiry_time": _ms(NOW + timedelta(days=700))},
        {"token_id": "dead", "comment": "expired", "expiry_time": _ms(NOW - timedelta(days=3))},
    ]
    unpinned = evaluate_pat_expiry(tokens, now=NOW)
    assert not unpinned.ok  # may be a false alarm, but never a missed expiry
    assert "laptop" in unpinned.message
    assert "DATABRICKS_TOKEN_ID" in unpinned.message
    assert evaluate_pat_expiry(tokens, now=NOW, token_id="ci").ok
    missing = evaluate_pat_expiry(tokens, now=NOW, token_id="gone")
    assert not missing.ok
    assert "not among" in missing.message


def test_warning_window_is_configurable():
    tokens = [{"token_id": "a1", "expiry_time": _ms(NOW + timedelta(days=20))}]
    assert evaluate_pat_expiry(tokens, now=NOW).ok
    assert not evaluate_pat_expiry(tokens, now=NOW, warn_days=30).ok


# ------------------------------------------------------------------------------- OAuth secret expiry


def test_oauth_secret_expiry_from_recorded_date():
    assert evaluate_secret_expiry("2028-10-01", now=NOW).ok
    assert not evaluate_secret_expiry("2026-10-15", now=NOW).ok
    assert not evaluate_secret_expiry("15/10/2026", now=NOW).ok
    unknown = evaluate_secret_expiry("", now=NOW)
    assert unknown.ok
    assert "unknown" in unknown.message


# ------------------------------------------------------------------------------- check_credentials


class _FakeTokens:
    def __init__(self, infos):
        self._infos = infos

    def list(self):
        return [SimpleNamespace(as_dict=lambda i=i: i) for i in self._infos]


def _client(infos=(), fail: Exception | None = None):
    def me():
        if fail:
            raise fail
        return SimpleNamespace(user_name="rohit@example.com", display_name=None, id=1)

    return SimpleNamespace(current_user=SimpleNamespace(me=me), tokens=_FakeTokens(list(infos)))


def test_check_credentials_reports_pat_expiry():
    infos = [{"token_id": "a1", "comment": "NewToken", "expiry_time": _ms(datetime.now(UTC) + timedelta(days=3))}]
    report = check_credentials(_client(infos), {"DATABRICKS_TOKEN": "x"})
    assert report.mode == "pat"
    assert report.principal == "rohit@example.com"
    assert not report.ok
    assert report.as_dict()["expires_at"]


def test_check_credentials_oidc_never_expires_and_auth_failure_raises():
    env = {"DATABRICKS_CLIENT_ID": "c", "ACTIONS_ID_TOKEN_REQUEST_URL": "https://gh"}
    assert check_credentials(_client(), env).ok
    with pytest.raises(RefreshError, match="authentication with pat failed"):
        check_credentials(_client(fail=PermissionError("invalid token")), {"DATABRICKS_TOKEN": "x"})


# ------------------------------------------------------------------------------- next business date


def test_next_business_date():
    today = date(2026, 10, 9)
    assert next_business_date(None, START, today=today) == START
    assert next_business_date(date(2026, 9, 3), START, today=today) == date(2026, 9, 4)
    assert next_business_date(date(2026, 10, 9), START, today=today) is None  # caught up with today
    assert next_business_date(date(2026, 9, 3), START, override=date(2026, 9, 2), today=today) == date(2026, 9, 2)
    with pytest.raises(RefreshError, match="before FD_START_DATE"):
        next_business_date(None, START, override=date(2026, 8, 31))


# ------------------------------------------------------------------------------------------- run_job


def _jobs(states, job_count=1):
    calls = {"cancelled": False, "params": None}
    seq = iter(states)

    def get_run(run_id):
        life, result, msg = next(seq)
        return SimpleNamespace(
            run_page_url="https://ws/run/7",
            state=SimpleNamespace(
                life_cycle_state=SimpleNamespace(value=life),
                result_state=SimpleNamespace(value=result) if result else None,
                state_message=msg,
            ),
        )

    def run_now(job_id, job_parameters):
        calls["params"] = job_parameters
        return SimpleNamespace(response=SimpleNamespace(run_id=7))

    def cancel_run(run_id):
        calls["cancelled"] = True

    jobs = SimpleNamespace(
        list=lambda name: [SimpleNamespace(job_id=1)] * job_count,
        run_now=run_now,
        get_run=get_run,
        cancel_run=cancel_run,
    )
    return SimpleNamespace(jobs=jobs), calls


def test_run_job_polls_until_success():
    w, calls = _jobs([("PENDING", None, ""), ("RUNNING", None, ""), ("TERMINATED", "SUCCESS", "")])
    out = run_job(w, "job", "2026-09-04", poll_s=0)
    assert out["result_state"] == "SUCCESS"
    assert calls["params"] == {"run_date": "2026-09-04"}


def test_run_job_failure_and_missing_job_raise():
    w, _ = _jobs([("TERMINATED", "FAILED", "quota exceeded")])
    with pytest.raises(RefreshError, match="quota exceeded"):
        run_job(w, "job", "2026-09-04", poll_s=0)
    w, _ = _jobs([], job_count=0)
    with pytest.raises(RefreshError, match="is the bundle deployed"):
        run_job(w, "job", "2026-09-04", poll_s=0)


def test_run_job_is_bounded_and_cancels_on_timeout():
    w, calls = _jobs([("RUNNING", None, "")] * 5)
    with pytest.raises(RefreshError, match="cancelled"):
        run_job(w, "job", "2026-09-04", poll_s=0, timeout_s=-1)
    assert calls["cancelled"]


# ------------------------------------------------------------------------------------ committed seed


def test_committed_seed_installs_and_matches_its_checksum(tmp_path):
    meta = install_committed_seed(Paths(tmp_path))
    assert meta["license"] == "CC0-1.0"
    assert meta["restaurants"] == 8673
    assert (tmp_path / "seed" / "restaurants.jsonl").stat().st_size > 1_000_000


def test_tampered_committed_seed_is_rejected(tmp_path):
    src = REPO / "seed"
    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "restaurants.meta.json").write_text((src / "restaurants.meta.json").read_text())
    payload = gzip.decompress((src / "restaurants.jsonl.gz").read_bytes()) + json.dumps({"x": 1}).encode()
    (bad / "restaurants.jsonl.gz").write_bytes(gzip.compress(payload))
    with pytest.raises(ValueError, match="checksum mismatch"):
        install_committed_seed(Paths(tmp_path / "out"), bad / "restaurants.jsonl.gz")
