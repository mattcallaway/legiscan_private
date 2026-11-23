"""Optional per-job request budgets shared by both ingestion paths."""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import os


@dataclass
class RequestBudget:
    limit: int | None
    used: int = 0
    exhausted: bool = False


_active = ContextVar("legiscan_request_budget", default=None)


@contextmanager
def job_request_budget():
    raw_limit = os.environ.get("LEGISCAN_JOB_REQUEST_LIMIT", "").strip()
    try:
        limit = int(raw_limit) if raw_limit else None
    except ValueError as error:
        raise ValueError("LEGISCAN_JOB_REQUEST_LIMIT must be a positive integer.") from error
    if limit is not None and limit < 1:
        raise ValueError("LEGISCAN_JOB_REQUEST_LIMIT must be a positive integer.")
    budget = RequestBudget(limit)
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
