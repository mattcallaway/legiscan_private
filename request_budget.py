"""Enforce per-job and shared monthly limits before LegiScan requests."""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache
import logging
import os
from pathlib import Path
import sqlite3

from legiscan_quota import LEDGER_UNAVAILABLE, LegiScanQuota


JOB_LIMIT_REACHED = "Per-job LegiScan request budget reached"
MISSING_KEY = "No LegiScan API key is configured"
logger = logging.getLogger(__name__)


@dataclass
class RequestBudget:
    limit: int | None
    used: int = 0
    exhausted: bool = False
    purpose: str = "Bill refresh"
    blocked_reason: str | None = None

    @property
    def stop_reason(self):
        return self.blocked_reason or (JOB_LIMIT_REACHED if self.exhausted else None)


_active = ContextVar("legiscan_request_budget", default=None)


@contextmanager
def job_request_budget(purpose="Bill refresh"):
    raw_limit = os.environ.get("LEGISCAN_JOB_REQUEST_LIMIT", "").strip()
    try:
        limit = int(raw_limit) if raw_limit else None
    except ValueError as error:
        raise ValueError("LEGISCAN_JOB_REQUEST_LIMIT must be a positive integer.") from error
    if limit is not None and limit < 1:
        raise ValueError("LEGISCAN_JOB_REQUEST_LIMIT must be a positive integer.")
    budget = RequestBudget(limit, purpose=purpose)
    token = _active.set(budget)
    try:
        yield budget
    finally:
        _active.reset(token)


def allow_request():
    budget = _active.get()
    if budget is None:
        return True
    if budget.limit is not None and budget.used >= budget.limit:
        budget.exhausted = True
        return False
    budget.used += 1
    return True


def job_stop_reason():
    budget = _active.get()
    return budget.stop_reason if budget else None


@lru_cache(maxsize=32)
def _open_quota(data_dir):
    return LegiScanQuota(data_dir)


def quota_for_directory(data_dir):
    return _open_quota(Path(data_dir).resolve())


def admit_legiscan_request(data_dir, key, operation, purpose="Bill research"):
    budget = _active.get()
    if budget and budget.stop_reason:
        return budget.stop_reason
    if budget and budget.limit is not None and budget.used >= budget.limit:
        budget.exhausted = True
        return JOB_LIMIT_REACHED
    if not key.strip():
        reason = MISSING_KEY
    else:
        try:
            reason = quota_for_directory(data_dir).reserve_request(key, operation, budget.purpose if budget else purpose)
        except (OSError, sqlite3.Error):
            logger.warning("Could not reserve a LegiScan request in the shared budget.")
            reason = LEDGER_UNAVAILABLE
    if reason:
        if budget:
            budget.blocked_reason = reason
        return reason
    allow_request()
    return None


def observe_legiscan_response(data_dir, key, response, payload=None):
    """Pause repeated requests after provider denials, without retaining raw alerts."""
    alert = payload.get("alert") if isinstance(payload, dict) and payload.get("status") == "ERROR" else None
    message = str(alert.get("message", "")).casefold() if isinstance(alert, dict) else ""
    status = response.status_code
    if status == 429 or any(term in message for term in ("quota", "query limit", "request limit", "rate limit", "usage limit")):
        reason, minutes = "LegiScan reported a request limit. Requests are paused temporarily", 60
    elif status == 401 or "api key" in message or "apikey" in message:
        reason, minutes = "LegiScan reported an API key problem", 60
    elif status == 403:
        reason, minutes = "LegiScan returned HTTP 403", 15
    else:
        return None
    retry_after = response.headers.get("Retry-After", "")
    if isinstance(retry_after, str) and retry_after.isdecimal() and len(retry_after) <= 9:
        minutes = min(1440, max(minutes, (int(retry_after) + 59) // 60))
    try:
        quota_for_directory(data_dir).pause_key(key, reason, minutes)
    except (OSError, sqlite3.Error):
        logger.warning("Could not record the LegiScan request pause.")
    if budget := _active.get():
        budget.blocked_reason = reason
    return reason
