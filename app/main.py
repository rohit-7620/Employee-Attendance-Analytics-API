"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB   (real environment variables win over a local .env)

Layout of this file
  1. configuration, constants, indexes
  2. time / rounding helpers (R1 - R5, R8)
  3. (de)serialisation helpers
  4. request models
  5. endpoints: system, employees, attendance
  6. pipeline builders + analytics endpoints
  7. explain endpoint (re-uses the builders, so it explains the real queries)
"""
import calendar
import json
import logging
import os
import re
import time as _time
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, StrictInt, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# load_dotenv() does NOT override variables that are already set, so real env vars win.
load_dotenv()
log = logging.getLogger("attendance")

# tz_aware=True: PyMongo hands back aware UTC datetimes instead of naive ones, so
# arithmetic and comparison with our own aware datetimes is always safe.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

# --------------------------------------------------------------------------- #
# 1. Constants and indexes
# --------------------------------------------------------------------------- #
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]  # R6
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

GRACE_SECONDS = 10 * 60      # R2
MIN_OVERTIME_MIN = 30        # R3
HALF_DAY_BELOW = Decimal("4.50")  # R5
MAX_SHIFT_SECONDS = 24 * 3600
MS_MIN, MS_MAX = 100_000_000_000, 4_102_444_800_000

ATT_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


def ensure_indexes() -> None:
    """Idempotent: create_index is a no-op when the index already exists."""
    # employees
    employees.create_index([("emp_code", ASCENDING)], unique=True, name="emp_code_unique")
    employees.create_index([("department", ASCENDING), ("emp_code", ASCENDING)], name="dept_emp_code")
    employees.create_index([("joined_on", ASCENDING)], name="joined_on")
    employees.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="dept_joined_on")
    # attendance_logs
    # one record per employee per day: the guarantee behind the punch-in race, and the
    # index for per-employee queries (monthly summary, list by emp_code + date range)
    logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="emp_date_unique")
    # default list order (date desc, emp_code asc) and every "all employees in a date range" query
    logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_emp")
    # list filtered by status, same sort
    logs.create_index(
        [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], name="status_date_emp"
    )
    # punch-out: "latest record of this employee whose punch_in <= t"
    logs.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name="emp_punch_in")


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Retry briefly so the app still comes up if MongoDB is a few seconds late.
    deadline = _time.monotonic() + 12
    while True:
        try:
            ensure_indexes()
            break
        except PyMongoError as exc:
            if _time.monotonic() > deadline:
                log.error("could not create indexes: %s", exc)
                break
            _time.sleep(1)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Time and rounding helpers
# --------------------------------------------------------------------------- #
def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def from_ms(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: treat naive values as UTC
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def secs_between(later: datetime, earlier: datetime) -> int:
    d = later - earlier
    return d.days * 86400 + d.seconds


def hm(s: str) -> tuple[int, int]:
    h, m = s.split(":")
    return int(h), int(m)


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return hm(shift_end) <= hm(shift_start)


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: IST calendar date of the punch-in; an overnight-shift punch-in earlier than
    shift_end belongs to the shift that started the previous day."""
    ist = punch_in.astimezone(IST)
    d = ist.date()
    if is_overnight(emp["shift_start"], emp["shift_end"]):
        eh, em = hm(emp["shift_end"])
        sec_of_day = ist.hour * 3600 + ist.minute * 60 + ist.second
        if sec_of_day < eh * 3600 + em * 60:
            d -= timedelta(days=1)
    return d


def shift_bounds(att_date: date, emp: dict) -> tuple[datetime, datetime]:
    sh, sm = hm(emp["shift_start"])
    eh, em = hm(emp["shift_end"])
    start = datetime.combine(att_date, time(sh, sm), tzinfo=IST)
    end = datetime.combine(att_date, time(eh, em), tzinfo=IST)
    if is_overnight(emp["shift_start"], emp["shift_end"]):
        end += timedelta(days=1)
    return start, end


def round_half_up(value: Decimal, places: int) -> Decimal:
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def compute_late_minutes(punch_in: datetime, shift_start_dt: datetime) -> int:
    """R2: late only when strictly more than 10:00 after shift start; then floor minutes
    since shift start (not since the end of the grace period)."""
    secs = secs_between(punch_in, shift_start_dt)
    return secs // 60 if secs > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals, half-up (Decimal, not float round())."""
    hours = Decimal(secs_between(punch_out, punch_in)) / Decimal(3600)
    return float(round_half_up(hours, 2))


def compute_overtime(punch_out: datetime, shift_end_dt: datetime) -> int:
    """R3: floor minutes after shift end, only when >= 30."""
    mins = secs_between(punch_out, shift_end_dt) // 60
    return mins if mins >= MIN_OVERTIME_MIN else 0


def derive(status: str, p_in: Optional[datetime], p_out: Optional[datetime], emp: dict, att_date: date) -> dict:
    """All four derived fields from the final status and times (R2 - R5)."""
    out = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE or p_in is None:
        return out
    start, end = shift_bounds(att_date, emp)
    out["late_minutes"] = compute_late_minutes(p_in, start)
    if p_out is not None:
        wh = compute_work_hours(p_in, p_out)
        out["work_hours"] = wh
        out["overtime_minutes"] = compute_overtime(p_out, end)
        out["half_day"] = Decimal(str(wh)) < HALF_DAY_BELOW
    return out


# --------------------------------------------------------------------------- #
# 3. Validation / serialisation helpers
# --------------------------------------------------------------------------- #
_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"


def bad(loc: list, msg: str):
    """Raise a 422 in FastAPI's own error shape ({"detail": [...]})."""
    raise RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": None}])


def parse_date(value: str, loc: list) -> date:
    if not _DATE_RE.match(value):
        bad(loc, "must be a date in YYYY-MM-DD format")
    try:
        return date.fromisoformat(value)
    except ValueError:
        bad(loc, "not a valid calendar date")


def month_bounds(month: str) -> tuple[date, date]:
    y, m = int(month[:4]), int(month[5:7])
    if y < 1:
        bad(["query", "month"], "invalid year")
    return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])


def count_weekdays(a: date, b: date) -> int:
    """Mon-Fri days in [a, b] inclusive (R7), O(1)."""
    if b < a:
        return 0
    n = (b - a).days + 1
    full, rem = divmod(n, 7)
    count = full * 5
    for i in range(rem):
        if (a + timedelta(days=full * 7 + i)).weekday() < 5:
            count += 1
    return count


def ms_if_dt(v: Any) -> Any:
    return to_ms(v) if isinstance(v, datetime) else v


def fmt_record(doc: dict) -> dict:
    """Stored attendance document -> API shape (epoch ms, defaults for legacy gaps, no _id)."""
    history = []
    for h in doc.get("history") or []:
        history.append(
            {
                "at": to_ms(h.get("at")),
                "by": h.get("by"),
                "reason": h.get("reason"),
                "changes": {
                    k: {"from": ms_if_dt(v.get("from")), "to": ms_if_dt(v.get("to"))}
                    for k, v in (h.get("changes") or {}).items()
                },
            }
        )
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": to_ms(doc.get("punch_in")),
        "punch_out": to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": history,
    }


def fmt_employee(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": to_ms(doc.get("created_at")),
    }


def get_employee(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 4. Request models
# --------------------------------------------------------------------------- #
_TIME_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=_TIME_PATTERN)
    shift_end: str = Field(default="18:30", pattern=_TIME_PATTERN)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        if not _DATE_RE.match(v):
            raise ValueError("must be YYYY-MM-DD")
        date.fromisoformat(v)  # ValueError -> 422
        return v

    @model_validator(mode="after")
    def _shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[StrictInt] = Field(default=None, ge=MS_MIN, le=MS_MAX)
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[StrictInt] = Field(default=None, ge=MS_MIN, le=MS_MAX)


class RegularizeIn(BaseModel):
    # Derived fields are not declared, so anything a client sends for them is ignored.
    status: Optional[Status] = None
    punch_in: Optional[StrictInt] = Field(default=None, ge=MS_MIN, le=MS_MAX)
    punch_out: Optional[StrictInt] = Field(default=None, ge=MS_MIN, le=MS_MAX)
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


# --------------------------------------------------------------------------- #
# 5. Endpoints: system, employees, attendance
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "mongodb unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    ms = _time.time_ns() // 1_000_000
    doc = body.model_dump()
    doc["created_at"] = EPOCH + timedelta(milliseconds=ms)
    try:
        # The unique index on emp_code decides races; no check-then-insert.
        employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return fmt_employee(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q: dict = {"department": department} if department is not None else {}
    total = employees.count_documents(q)
    cursor = (
        employees.find(q, {"_id": 0})
        .sort("emp_code", ASCENDING)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    return {"items": [fmt_employee(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee(body.emp_code)
    ts = from_ms(body.punched_at) if body.punched_at is not None else now_utc()
    att_date = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": att_date.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive(body.status, ts, None, emp, att_date),
        "history": [],
    }
    try:
        # Atomic: the unique (emp_code, date) index lets exactly one concurrent insert win.
        logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return fmt_record(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee(body.emp_code)
    ts = from_ms(body.punched_at) if body.punched_at is not None else now_utc()
    # most recent record opened at or before the punch-out (may be yesterday's, for overnight shifts)
    rec = logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", DESCENDING)]
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    elapsed = secs_between(ts, rec["punch_in"])
    if elapsed <= 0 or elapsed > MAX_SHIFT_SECONDS:
        bad(["body", "punched_at"], "must be after punch_in and at most 24 hours later")
    d = derive(rec["status"], rec["punch_in"], ts, emp, date.fromisoformat(rec["date"]))
    # Conditional update: succeeds only if the record is still open and its punch_in is
    # unchanged (a concurrent PATCH would invalidate our computation). Else 409.
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None, "punch_in": rec["punch_in"]},
        {
            "$set": {
                "punch_out": ts,
                "work_hours": d["work_hours"],
                "overtime_minutes": d["overtime_minutes"],
                "half_day": d["half_day"],
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out or record changed concurrently")
    return fmt_record(updated)


def build_attendance_filter(
    emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str], status: Optional[str]
) -> dict:
    q: dict = {}
    d_from = parse_date(date_from, ["query", "date_from"]) if date_from else None
    d_to = parse_date(date_to, ["query", "date_to"]) if date_to else None
    if d_from and d_to and d_from > d_to:
        bad(["query", "date_from"], "date_from must not be after date_to")
    if emp_code:
        q["emp_code"] = emp_code
    if d_from or d_to:
        q["date"] = {}
        if d_from:
            q["date"]["$gte"] = d_from.isoformat()
        if d_to:
            q["date"]["$lte"] = d_to.isoformat()
    if status:
        q["status"] = status
    return q


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = build_attendance_filter(emp_code, date_from, date_to, status)
    total = logs.count_documents(q) if q else logs.estimated_document_count()
    cursor = logs.find(q, {"_id": 0}).sort(ATT_SORT).skip((page - 1) * page_size).limit(page_size)
    return {"items": [fmt_record(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    rec_date = parse_date(date, ["path", "date"])
    emp = get_employee(emp_code)
    rec = logs.find_one({"emp_code": emp_code, "date": rec_date.isoformat()})
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")

    old = {
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day", False)),
    }

    status = body.status or old["status"]
    if status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            bad(["body"], "punch_in / punch_out cannot be supplied with status ABSENT or LEAVE")
        new_in = new_out = None
    else:
        new_in = from_ms(body.punch_in) if body.punch_in is not None else old["punch_in"]
        new_out = from_ms(body.punch_out) if body.punch_out is not None else old["punch_out"]
        if new_in is None:
            bad(["body", "punch_in"], "a presence status requires a punch_in")
        if attendance_date(new_in, emp) != rec_date:
            bad(["body", "punch_in"], "punch_in must fall on the record's attendance date")
        if new_out is not None:
            gap = secs_between(new_out, new_in)
            if gap <= 0 or gap > MAX_SHIFT_SECONDS:
                bad(["body", "punch_out"], "punch_out must be after punch_in and within 24 hours")

    new = {"status": status, "punch_in": new_in, "punch_out": new_out,
           **derive(status, new_in, new_out, emp, rec_date)}
    changes = {k: {"from": old[k], "to": new[k]} for k in old if old[k] != new[k]}
    if not changes:
        bad(["body"], "request changes nothing")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency: the update applies only if the record is exactly as we read
    # it (same status/times and same history length). Otherwise someone else wrote first
    # and we answer 409 instead of silently losing their history entry.
    n = len(rec.get("history") or [])
    history_guard = (
        {"history": {"$size": n}}
        if n
        else {"$or": [{"history": {"$exists": False}}, {"history": {"$size": 0}}]}
    )
    guard = {
        "_id": rec["_id"],
        "status": old["status"],
        "punch_in": old["punch_in"],
        "punch_out": old["punch_out"],
        **history_guard,
    }
    updated = logs.find_one_and_update(
        guard,
        {"$set": new, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "record was modified concurrently; retry")
    return fmt_record(updated)


# --------------------------------------------------------------------------- #
# 6. Pipeline builders and analytics endpoints
# --------------------------------------------------------------------------- #
def _is_present():
    return {"$in": ["$status", PRESENCE]}


def _late_of():
    return {"$ifNull": ["$late_minutes", 0]}


def _is_late():
    return {"$gt": [_late_of(), 0]}


def _present_x2():
    """Presence counted in half-days (integers): 2 = full day, 1 = half day, 0 = not
    present or weekend. Integers keep every sum exact; divide by 2 at the end."""
    weekday = {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}, 5]}
    half = {"$eq": [{"$ifNull": ["$half_day", False]}, True]}
    return {"$cond": [{"$and": [_is_present(), weekday]}, {"$cond": [half, 1, 2]}, 0]}


def _count_if(cond):
    return {"$sum": {"$cond": [cond, 1, 0]}}


def employee_monthly_pipeline(emp_code: str, start: date, end: date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present_x2": {"$sum": _present_x2()},
                "leave_days": _count_if({"$eq": ["$status", "LEAVE"]}),
                "late_count": _count_if(_is_late()),
                "total_late_minutes": {"$sum": _late_of()},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]


def department_summary_pipeline(start: date, end: date, department: Optional[str]) -> list:
    # Start from EMPLOYEES so people with zero logs are still counted (R9), then pull each
    # person's monthly aggregates from the logs (index-backed join on emp_code, date).
    emp_match: dict = {"joined_on": {"$lte": end.isoformat()}}
    if department is not None:
        emp_match["department"] = department
    wh_present = {"$and": [_is_present(), {"$isNumber": "$work_hours"}]}
    return [
        {"$match": emp_match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}},
                    {
                        "$group": {
                            "_id": None,
                            "present_x2": {"$sum": _present_x2()},
                            # work_hours in integer hundredths -> exact sums, exact half-up later
                            "wh_cents": {
                                "$sum": {"$cond": [wh_present, {"$toLong": {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}}, 0]}
                            },
                            "wh_n": _count_if(wh_present),
                            "late_count": _count_if(_is_late()),
                            "late_total": {"$sum": _late_of()},
                            "leave_count": _count_if({"$eq": ["$status", "LEAVE"]}),
                            "on_duty_count": _count_if({"$eq": ["$status", "ON_DUTY"]}),
                        }
                    },
                ],
                "as": "s",
            }
        },
        {"$set": {"s": {"$ifNull": [{"$arrayElemAt": ["$s", 0]}, {}]}}},
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_x2": {"$sum": {"$ifNull": ["$s.present_x2", 0]}},
                "wh_cents": {"$sum": {"$ifNull": ["$s.wh_cents", 0]}},
                "wh_n": {"$sum": {"$ifNull": ["$s.wh_n", 0]}},
                "late_count": {"$sum": {"$ifNull": ["$s.late_count", 0]}},
                "late_total": {"$sum": {"$ifNull": ["$s.late_total", 0]}},
                "leave_count": {"$sum": {"$ifNull": ["$s.leave_count", 0]}},
                "on_duty_count": {"$sum": {"$ifNull": ["$s.on_duty_count", 0]}},
            }
        },
        {"$sort": {"_id": 1}},
    ]


def late_leaderboard_pipeline(start: date, end: date, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": {"date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}},
        {"$group": {"_id": "$emp_code", "total": {"$sum": _late_of()}, "late_count": _count_if(_is_late())}},
        {"$match": {"total": {"$gt": 0}}},
        # inner join: logs of unknown employees disappear at $unwind
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "e"}},
        {"$unwind": "$e"},
    ]
    if department is not None:
        # filter BEFORE ranking so ranks are computed inside the department
        pipeline.append({"$match": {"e.department": department}})
    pipeline += [
        # $rank = standard competition ranking (1, 2, 2, 4); ties are decided on `total` only
        {"$setWindowFields": {"sortBy": {"total": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit AFTER ranking
        {"$sort": {"total": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$e.name",
                "department": "$e.department",
                "total_late_minutes": "$total",
                "late_count": 1,
            }
        },
    ]
    return pipeline


def department_trend_pipeline(codes: list[str], joined: list[str], d_from: date, d_to: date) -> list:
    day0 = datetime.combine(d_from, time.min, tzinfo=UTC)
    day_after_last = datetime.combine(d_to + timedelta(days=1), time.min, tzinfo=UTC)
    last = datetime.combine(d_to, time.min, tzinfo=UTC)
    half = {"$eq": [{"$ifNull": ["$half_day", False]}, True]}
    return [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": d_from.isoformat(), "$lte": d_to.isoformat()}}},
        {
            "$group": {
                "_id": "$date",
                "present_x2": {"$sum": {"$cond": [_is_present(), {"$cond": [half, 1, 2]}, 0]}},
                "late_count": _count_if(_is_late()),
            }
        },
        {"$set": {"d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        {"$sort": {"d": 1}},  # explicit: $densify wants its input ordered by the densified field
        # gap filling: one document per calendar day. The upper bound is padded by one day
        # and trimmed below, so the result is right whether the bound is inclusive or not.
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [day0, day_after_last]}}},
        {"$match": {"d": {"$gte": day0, "$lte": last}}},
        {
            "$set": {
                "date": {"$dateToString": {"date": "$d", "format": "%Y-%m-%d"}},
                "is_working_day": {"$lte": [{"$isoDayOfWeek": "$d"}, 5]},
                "present_x2": {"$ifNull": ["$present_x2", 0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
            }
        },
        {
            "$set": {
                "headcount": {
                    "$size": {"$filter": {"input": {"$literal": joined}, "as": "j", "cond": {"$lte": ["$$j", "$date"]}}}
                }
            }
        },
        {
            # attendance_rate in integer ten-thousandths, rounded half-up with integer maths:
            #   rate*10000 = 5000*present_x2/headcount  ->  floor((10000*present_x2 + headcount) / (2*headcount))
            "$set": {
                "rate_int": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": ["$present_x2", 10000]}, "$headcount"]},
                                    {"$multiply": [2, "$headcount"]},
                                ]
                            }
                        },
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"d": 1},
                "output": {
                    "win_sum": {"$sum": "$rate_int", "window": {"documents": [-6, 0]}},
                    "win_cnt": {
                        "$sum": {"$cond": [{"$eq": ["$rate_int", None]}, 0, 1]},
                        "window": {"documents": [-6, 0]},
                    },
                },
            }
        },
        {
            # mean of the reported (rounded) rates, half-up to 4 decimals, again in integers
            "$set": {
                "moving_int": {
                    "$cond": [
                        {"$gt": ["$win_cnt", 0]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": [2, "$win_sum"]}, "$win_cnt"]},
                                    {"$multiply": [2, "$win_cnt"]},
                                ]
                            }
                        },
                        None,
                    ]
                }
            }
        },
        {"$sort": {"d": 1}},
        {
            "$project": {
                "_id": 0, "date": 1, "is_working_day": 1, "headcount": 1,
                "present_x2": 1, "late_count": 1, "rate_int": 1, "moving_int": 1,
            }
        },
    ]


def _ten_thousandths(v: Optional[int]) -> Optional[float]:
    return None if v is None else float(Decimal(int(v)) / Decimal(10000))


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=_MONTH_PATTERN)):
    emp = get_employee(emp_code)
    m_start, m_end = month_bounds(month)
    joined = date.fromisoformat(emp["joined_on"])
    working_days = count_weekdays(max(m_start, joined), m_end)  # R7: counted from joined_on

    agg = next(logs.aggregate(employee_monthly_pipeline(emp_code, m_start, m_end)), None) or {}
    present_x2 = int(agg.get("present_x2", 0))
    pct = None
    if working_days > 0:
        pct = float(round_half_up(Decimal(present_x2) / Decimal(2 * working_days) * 100, 2))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": float(Decimal(present_x2) / 2),
        "leave_days": int(agg.get("leave_days", 0)),
        "late_count": int(agg.get("late_count", 0)),
        "total_late_minutes": int(agg.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(agg.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=_MONTH_PATTERN), department: Optional[str] = None):
    m_start, m_end = month_bounds(month)
    items = []
    for r in employees.aggregate(department_summary_pipeline(m_start, m_end, department)):
        avg = None
        if r["wh_n"] > 0:
            avg = float(round_half_up(Decimal(int(r["wh_cents"])) / Decimal(int(r["wh_n"])) / 100, 2))
        items.append(
            {
                "department": r["_id"],
                "headcount": int(r["headcount"]),
                "present_days": float(Decimal(int(r["present_x2"])) / 2),
                "avg_work_hours": avg,
                "late_count": int(r["late_count"]),
                "total_late_minutes": int(r["late_total"]),
                "leave_count": int(r["leave_count"]),
                "on_duty_count": int(r["on_duty_count"]),
            }
        )
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=_MONTH_PATTERN),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    m_start, m_end = month_bounds(month)
    rows = list(logs.aggregate(late_leaderboard_pipeline(m_start, m_end, limit, department)))
    return {
        "month": month,
        "items": [
            {
                "rank": int(r["rank"]),
                "emp_code": r["emp_code"],
                "name": r["name"],
                "department": r["department"],
                "total_late_minutes": int(r["total_late_minutes"]),
                "late_count": int(r["late_count"]),
            }
            for r in rows
        ],
    }


def validate_trend_range(from_: str, to: str) -> tuple[date, date]:
    d_from = parse_date(from_, ["query", "from"])
    d_to = parse_date(to, ["query", "to"])
    if d_to < d_from:
        bad(["query", "to"], "to must not be before from")
    if (d_to - d_from).days + 1 > 92:
        bad(["query", "to"], "range must not exceed 92 days")
    return d_from, d_to


def department_roster(department: str) -> tuple[list[str], list[str]]:
    """emp_codes and joined_on dates of a department (index: department, emp_code)."""
    rows = list(employees.find({"department": department}, {"_id": 0, "emp_code": 1, "joined_on": 1}))
    if not rows:
        raise HTTPException(404, "department not found")
    return [r["emp_code"] for r in rows], [r["joined_on"] for r in rows]


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
):
    d_from, d_to = validate_trend_range(from_, to)
    codes, joined = department_roster(department)
    rows = list(logs.aggregate(department_trend_pipeline(codes, joined, d_from, d_to)))
    return {
        "department": department,
        "items": [
            {
                "date": r["date"],
                "is_working_day": bool(r["is_working_day"]),
                "headcount": int(r["headcount"]),
                "present_count": float(Decimal(int(r["present_x2"])) / 2),
                "late_count": int(r["late_count"]),
                "attendance_rate": _ten_thousandths(r.get("rate_int")),
                "moving_avg_7d": _ten_thousandths(r.get("moving_int")),
            }
            for r in rows
        ],
    }


# --------------------------------------------------------------------------- #
# 7. Explain endpoint
# --------------------------------------------------------------------------- #
ExplainTarget = Literal[
    "attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"
]


def _need(value: Any, name: str):
    if value is None or value == "":
        bad(["query", name], f"{name} is required for this endpoint")
    return value


def _month_param(month: Optional[str]) -> tuple[date, date]:
    _need(month, "month")
    if not re.match(_MONTH_PATTERN, month):
        bad(["query", "month"], "must be YYYY-MM")
    return month_bounds(month)


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    # Each branch builds the operation with the SAME functions the real endpoint uses.
    if endpoint == "attendance_list":
        q = build_attendance_filter(emp_code, date_from, date_to, status)
        coll = logs.name
        command: dict = {
            "find": coll, "filter": q, "sort": dict(ATT_SORT),
            "skip": (page - 1) * page_size, "limit": page_size,
        }
    elif endpoint == "employee_monthly":
        _need(emp_code, "emp_code")
        m_start, m_end = _month_param(month)
        coll = logs.name
        command = {"aggregate": coll, "pipeline": employee_monthly_pipeline(emp_code, m_start, m_end), "cursor": {}}
    elif endpoint == "department_summary":
        m_start, m_end = _month_param(month)
        coll = employees.name
        command = {"aggregate": coll, "pipeline": department_summary_pipeline(m_start, m_end, department), "cursor": {}}
    elif endpoint == "late_leaderboard":
        m_start, m_end = _month_param(month)
        coll = logs.name
        command = {"aggregate": coll, "pipeline": late_leaderboard_pipeline(m_start, m_end, limit, department), "cursor": {}}
    else:  # department_trend
        _need(department, "department")
        _need(from_, "from")
        _need(to, "to")
        d_from, d_to = validate_trend_range(from_, to)
        codes, joined = department_roster(department)
        coll = logs.name
        command = {"aggregate": coll, "pipeline": department_trend_pipeline(codes, joined, d_from, d_to), "cursor": {}}

    result = db.command("explain", command, verbosity="executionStats")
    return {"endpoint": endpoint, "collection": coll, "explain": json.loads(json_util.dumps(result))}
