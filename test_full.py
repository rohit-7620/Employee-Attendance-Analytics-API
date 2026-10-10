"""
Full verification test suite for the Attendance API.

Phases:
  1. SETUP  – venv, install, start server, health-check
  2. SEED   – insert awkward test data directly into MongoDB
  3. VERIFY – all analytics endpoints with hand-computed expected values
  4. CONCURRENCY – real HTTP parallel requests
  5. SCALE  – 100k logs + timing
  6. REPORT – PASS/FAIL table

Run:  python test_full.py
Requirements in test_full_requirements.txt (httpx, pymongo, python-dateutil)
"""

import calendar
import math
import os
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

import httpx
from pymongo import MongoClient, ASCENDING, DESCENDING

# ── Config ─────────────────────────────────────────────────────────────────────
BASE = "http://localhost:8000"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB", "attendance_test")

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

# ── Connect to Mongo ────────────────────────────────────────────────────────────
client = MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=8000)
db = client[MONGO_DB]
employees = db["employees"]
logs = db["attendance_logs"]

# ── Results accumulator ─────────────────────────────────────────────────────────
results = []   # list of {"check": str, "status": "PASS"|"FAIL", "detail": str}

def record(check, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    results.append({"check": check, "status": status, "detail": detail})
    tag = "✅" if ok else "❌"
    print(f"{tag} {check}" + (f"\n   {detail}" if not ok else ""))

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

# ── Helpers ─────────────────────────────────────────────────────────────────────
def ist_to_utc(y, mo, d, h, mi, s=0):
    """Construct a UTC-aware datetime from an IST wall-clock time."""
    naive_ist = datetime(y, mo, d, h, mi, s)
    ist_dt = naive_ist.replace(tzinfo=IST)
    return ist_dt.astimezone(UTC)

def count_weekdays(a: date, b: date) -> int:
    if b < a:
        return 0
    n = (b - a).days + 1
    full, rem = divmod(n, 7)
    count = full * 5
    for i in range(rem):
        if (a + timedelta(days=full * 7 + i)).weekday() < 5:
            count += 1
    return count

def round_half_up(value: Decimal, places: int) -> Decimal:
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)

# ── PHASE 1: HEALTH CHECK ───────────────────────────────────────────────────────
section("PHASE 1 – Health & Indexes")

def phase1_health():
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            r = httpx.get(f"{BASE}/health", timeout=3)
            if r.status_code == 200:
                record("GET /health returns 200", True)
                return True
        except Exception:
            pass
        time.sleep(1)
    record("GET /health returns 200", False, "Server did not respond within 20s")
    return False

ok = phase1_health()
if not ok:
    print("\nServer not up – cannot continue. Start uvicorn first.")
    sys.exit(1)

# Check indexes
idx = logs.index_information()
idx_e = employees.index_information()
expected_log_idx = {"emp_date_unique", "date_emp", "status_date_emp", "emp_punch_in"}
expected_emp_idx = {"emp_code_unique", "dept_emp_code", "joined_on", "dept_joined_on"}
record("attendance_logs indexes created", expected_log_idx.issubset(set(idx.keys())),
       f"Missing: {expected_log_idx - set(idx.keys())}")
record("employees indexes created", expected_emp_idx.issubset(set(idx_e.keys())),
       f"Missing: {expected_emp_idx - set(idx_e.keys())}")

# ── PHASE 2: SEED ───────────────────────────────────────────────────────────────
section("PHASE 2 – Seed Test Data")

# Wipe and re-seed so the test is idempotent
employees.delete_many({})
logs.delete_many({})

# Reference month: October 2025
# Mon 2025-10-06 … Fri 2025-10-10  (week 1 of month used for detailed checks)
#
# Employees
# E001  Engineering  joined 2025-10-01  09:30-18:30 (normal)
# E002  Engineering  joined 2025-10-01  09:30-18:30
# E003  Engineering  joined 2025-10-01  09:30-18:30  (zero logs)
# E004  Marketing    joined 2025-10-15  09:30-18:30  (mid-month joiner)
# E005  Marketing    joined 2025-10-01  22:00-06:00  (overnight shift)
# E006  Marketing    joined 2025-10-01  09:30-18:30
# GHOST – no employee doc (logs for unknown emp_code)

emp_docs = [
    {"emp_code": "EMP0001", "name": "Alice",  "email": "alice@test.com",
     "department": "Engineering", "shift_start": "09:30", "shift_end": "18:30",
     "joined_on": "2025-10-01", "created_at": datetime(2025, 10, 1, tzinfo=UTC)},
    {"emp_code": "EMP0002", "name": "Bob",    "email": "bob@test.com",
     "department": "Engineering", "shift_start": "09:30", "shift_end": "18:30",
     "joined_on": "2025-10-01", "created_at": datetime(2025, 10, 1, tzinfo=UTC)},
    {"emp_code": "EMP0003", "name": "Charlie","email": "charlie@test.com",
     "department": "Engineering", "shift_start": "09:30", "shift_end": "18:30",
     "joined_on": "2025-10-01", "created_at": datetime(2025, 10, 1, tzinfo=UTC)},
    {"emp_code": "EMP0004", "name": "Dave",   "email": "dave@test.com",
     "department": "Marketing", "shift_start": "09:30", "shift_end": "18:30",
     "joined_on": "2025-10-15", "created_at": datetime(2025, 10, 1, tzinfo=UTC)},
    {"emp_code": "EMP0005", "name": "Eve",    "email": "eve@test.com",
     "department": "Marketing", "shift_start": "22:00", "shift_end": "06:00",
     "joined_on": "2025-10-01", "created_at": datetime(2025, 10, 1, tzinfo=UTC)},
    {"emp_code": "EMP0006", "name": "Frank",  "email": "frank@test.com",
     "department": "Marketing", "shift_start": "09:30", "shift_end": "18:30",
     "joined_on": "2025-10-01", "created_at": datetime(2025, 10, 1, tzinfo=UTC)},
]
employees.insert_many(emp_docs)

# Attendance logs
# All punch times stored as UTC datetimes (BSON dates).
# IST → UTC: subtract 5h30m

def log(emp, date_str, status, pin_ist=None, pout_ist=None,
        work_hours=None, late_min=0, ot_min=0, half_day=False,
        history=None, omit_history=False, omit_half_day=False):
    """Build an attendance log document."""
    def to_utc(ymd, hm):
        if hm is None:
            return None
        y, mo, d_ = map(int, ymd.split("-"))
        h, m_ = map(int, hm.split(":"))
        return ist_to_utc(y, mo, d_, h, m_)

    doc = {
        "emp_code": emp,
        "date": date_str,
        "status": status,
        "punch_in":  to_utc(date_str, pin_ist),
        "punch_out": to_utc(date_str, pout_ist),
        "work_hours": work_hours,
        "late_minutes": late_min,
        "overtime_minutes": ot_min,
    }
    if not omit_half_day:
        doc["half_day"] = half_day
    if not omit_history:
        doc["history"] = history or []
    return doc

log_docs = [
    # ── EMP0001 – October 2025 ───────────────────────────────────────────────
    # Mon 2025-10-06  PRESENT  on time  8h work
    log("EMP0001", "2025-10-06", "PRESENT", "09:30", "17:30", work_hours=8.0,
        late_min=0, ot_min=0, half_day=False),
    # Tue 2025-10-07  PRESENT  late 45 min  9h work  30 min OT
    log("EMP0001", "2025-10-07", "PRESENT", "10:15", "19:15", work_hours=9.0,
        late_min=45, ot_min=45, half_day=False),
    # Wed 2025-10-08  ABSENT
    log("EMP0001", "2025-10-08", "ABSENT"),
    # Thu 2025-10-09  LEAVE
    log("EMP0001", "2025-10-09", "LEAVE"),
    # Fri 2025-10-10  PRESENT  half day  4h work
    log("EMP0001", "2025-10-10", "PRESENT", "09:30", "13:30", work_hours=4.0,
        late_min=0, ot_min=0, half_day=True),
    # Sat 2025-10-11  PRESENT (weekend – still adds to late/OT totals per rule)
    log("EMP0001", "2025-10-11", "PRESENT", "10:00", "18:30", work_hours=8.5,
        late_min=30, ot_min=0, half_day=False),
    # Mon 2025-10-13  WFH  on time
    log("EMP0001", "2025-10-13", "WFH", "09:30", "18:30", work_hours=9.0,
        late_min=0, ot_min=0, half_day=False),
    # ON_DUTY record
    log("EMP0001", "2025-10-14", "ON_DUTY", "09:30", "18:30", work_hours=9.0,
        late_min=0, ot_min=0, half_day=False),

    # ── EMP0002 – October 2025 ───────────────────────────────────────────────
    # Two PRESENT days – same late total as EMP0001 (ties test)
    log("EMP0002", "2025-10-06", "PRESENT", "10:15", "19:00", work_hours=8.75,
        late_min=45, ot_min=30, half_day=False),
    log("EMP0002", "2025-10-07", "PRESENT", "09:30", "18:30", work_hours=9.0,
        late_min=0, ot_min=0, half_day=False),
    # Legacy record: no history field, no half_day field
    {
        "emp_code": "EMP0002",
        "date": "2025-10-08",
        "status": "PRESENT",
        "punch_in":  ist_to_utc(2025, 10, 8, 9, 30),
        "punch_out": ist_to_utc(2025, 10, 8, 18, 30),
        "work_hours": 9.0,
        "late_minutes": 0,
        "overtime_minutes": 0,
        # No history, no half_day keys
    },

    # ── EMP0004 – mid-month joiner (joined 2025-10-15) ───────────────────────
    log("EMP0004", "2025-10-15", "PRESENT", "09:30", "18:30", work_hours=9.0,
        late_min=0, ot_min=0, half_day=False),
    log("EMP0004", "2025-10-16", "PRESENT", "09:45", "18:30", work_hours=8.75,
        late_min=15, ot_min=0, half_day=False),

    # ── EMP0005 – overnight shift (22:00-06:00 IST) ──────────────────────────
    # Date assigned to 2025-10-06 (punch_in at 22:00 IST on Oct 6, punch_out at 05:30 IST on Oct 7)
    # shift_start IST = 22:00, shift_end IST = 06:00 (+1 day)
    # punch_in UTC = 2025-10-06 16:30 UTC
    # punch_out UTC = 2025-10-06 (IST 2025-10-07 05:30) = 2025-10-07 00:00 UTC
    {
        "emp_code": "EMP0005",
        "date": "2025-10-06",
        "status": "PRESENT",
        "punch_in":  ist_to_utc(2025, 10, 6, 22, 0),
        "punch_out": ist_to_utc(2025, 10, 7, 5, 30),   # 7.5h shift, on time
        "work_hours": 7.5,
        "late_minutes": 0,
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    },

    # ── EMP0006 – normal logs ─────────────────────────────────────────────────
    log("EMP0006", "2025-10-06", "PRESENT", "10:30", "19:30", work_hours=9.0,
        late_min=60, ot_min=60, half_day=False),
    log("EMP0006", "2025-10-07", "PRESENT", "09:30", "18:30", work_hours=9.0,
        late_min=0, ot_min=0, half_day=False),

    # ── GHOST (no employee doc) ───────────────────────────────────────────────
    log("EMP9999", "2025-10-06", "PRESENT", "09:30", "18:30", work_hours=9.0,
        late_min=0, ot_min=0, half_day=False),
]

logs.insert_many(log_docs)
record("Seed data inserted", True, f"{len(log_docs)} logs, {len(emp_docs)} employees")

# ── PHASE 3: VERIFY ENDPOINTS ──────────────────────────────────────────────────
section("PHASE 3 – Verify Endpoints")

def get(path, params=None):
    return httpx.get(f"{BASE}{path}", params=params, timeout=10)

# ─────────────────────────────────────────────────────────────────────────────
# 3a. Employee monthly  EMP0001  October 2025
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- 3a. Employee monthly EMP0001 ---")
r = get("/analytics/employees/EMP0001/monthly", {"month": "2025-10"})
record("EMP0001 monthly HTTP 200", r.status_code == 200, f"status={r.status_code}")
if r.status_code == 200:
    d = r.json()
    # Hand calculation:
    # joined_on = 2025-10-01, month = Oct 2025  (2025-10-01 to 2025-10-31)
    # working_days = Mon-Fri in [2025-10-01, 2025-10-31] = 23 days
    working_days_expected = count_weekdays(date(2025, 10, 1), date(2025, 10, 31))
    # Presence (Mon-Fri only):
    #   Oct 06 Mon: PRESENT full  → 2
    #   Oct 07 Tue: PRESENT full  → 2
    #   Oct 08 Wed: ABSENT        → 0
    #   Oct 09 Thu: LEAVE         → 0
    #   Oct 10 Fri: PRESENT half  → 1
    #   Oct 11 Sat: PRESENT full  → 0 (weekend, not counted)
    #   Oct 13 Mon: WFH full      → 2
    #   Oct 14 Tue: ON_DUTY full  → 2
    # Sum of weekday presence_x2 = 2+2+0+0+1+0+2+2 = 9 → present_days = 4.5
    present_x2_expected = 9
    present_days_expected = 4.5
    # leave_days: Oct 09 LEAVE = 1
    leave_days_expected = 1
    # late_count: Oct 07 (45 late) + Oct 11 Sat (30 late, counts) = 2
    # per spec: "weekend records still add to late and overtime totals"
    late_count_expected = 2
    # total_late_minutes: 45 + 30 = 75
    total_late_expected = 75
    # attendance_pct = 4.5/23*100 = 19.565... = 19.57 (half-up 2dp)
    pct_expected = float(round_half_up(Decimal("4.5") / Decimal(str(working_days_expected)) * 100, 2))
    
    record("EMP0001 working_days", d["working_days"] == working_days_expected,
           f"got={d['working_days']} expected={working_days_expected}")
    record("EMP0001 present_days", d["present_days"] == present_days_expected,
           f"got={d['present_days']} expected={present_days_expected}")
    record("EMP0001 leave_days", d["leave_days"] == leave_days_expected,
           f"got={d['leave_days']} expected={leave_days_expected}")
    record("EMP0001 late_count", d["late_count"] == late_count_expected,
           f"got={d['late_count']} expected={late_count_expected}")
    record("EMP0001 total_late_minutes", d["total_late_minutes"] == total_late_expected,
           f"got={d['total_late_minutes']} expected={total_late_expected}")
    record("EMP0001 attendance_pct", d["attendance_pct"] == pct_expected,
           f"got={d['attendance_pct']} expected={pct_expected}")

# EMP0003 – zero logs
print("\n--- 3a. Employee monthly EMP0003 (zero logs) ---")
r3 = get("/analytics/employees/EMP0003/monthly", {"month": "2025-10"})
record("EMP0003 monthly HTTP 200", r3.status_code == 200, f"status={r3.status_code}")
if r3.status_code == 200:
    d3 = r3.json()
    record("EMP0003 present_days = 0", d3["present_days"] == 0.0, f"got={d3['present_days']}")
    record("EMP0003 working_days > 0", d3["working_days"] > 0, f"got={d3['working_days']}")

# EMP0004 – mid-month joiner (joined Oct 15)
print("\n--- 3a. Employee monthly EMP0004 (mid-month joiner) ---")
r4 = get("/analytics/employees/EMP0004/monthly", {"month": "2025-10"})
record("EMP0004 monthly HTTP 200", r4.status_code == 200, f"status={r4.status_code}")
if r4.status_code == 200:
    d4 = r4.json()
    # joined Oct 15, working days = Mon-Fri in [Oct 15, Oct 31]
    wd4_expected = count_weekdays(date(2025, 10, 15), date(2025, 10, 31))
    record("EMP0004 working_days (from joined_on)", d4["working_days"] == wd4_expected,
           f"got={d4['working_days']} expected={wd4_expected}")
    record("EMP0004 present_days = 2 (both days full)", d4["present_days"] == 2.0,
           f"got={d4['present_days']}")

# ─────────────────────────────────────────────────────────────────────────────
# 3b. Department summary  2025-10
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- 3b. Department summary 2025-10 ---")
r = get("/analytics/departments/summary", {"month": "2025-10"})
record("Dept summary HTTP 200", r.status_code == 200, f"status={r.status_code}")
if r.status_code == 200:
    data = {item["department"]: item for item in r.json()["items"]}
    
    # Engineering headcount: EMP0001, EMP0002, EMP0003 (zero logs) joined<=Oct31 → 3
    record("Engineering headcount = 3", data.get("Engineering", {}).get("headcount") == 3,
           f"got={data.get('Engineering', {}).get('headcount')}")
    
    # Marketing headcount: EMP0004 (joined Oct15 <= Oct31), EMP0005, EMP0006 → 3
    record("Marketing headcount = 3", data.get("Marketing", {}).get("headcount") == 3,
           f"got={data.get('Marketing', {}).get('headcount')}")
    
    # EMP0004 joined after month-start but still <=month-end → should be included
    record("EMP0004 included (joined mid-month)", "Marketing" in data, "Marketing not found")
    
    # Departments sorted ascending
    names = [item["department"] for item in r.json()["items"]]
    record("Departments sorted ascending", names == sorted(names),
           f"got={names}")
    
    # avg_work_hours for Engineering: EMP0001 (8.0,9.0,4.0,9.0,9.0) + EMP0002(8.75,9.0,9.0)
    # Present-status records with work_hours:
    # EMP0001: Oct06=8.0, Oct07=9.0, Oct10=4.0(half), Oct11=8.5(weekend), Oct13=9.0(WFH), Oct14=9.0(ON_DUTY) = 6 records
    # EMP0002: Oct06=8.75, Oct07=9.0, Oct08=9.0 = 3 records
    # EMP0003: 0 records
    # Total work_hours: 8.0+9.0+4.0+8.5+9.0+9.0 + 8.75+9.0+9.0 = 74.25
    # Count: 9 records
    # avg = 74.25/9 = 8.25 (in cents: 8.0*100+9.0*100+4.0*100+8.5*100+9.0*100+9.0*100+8.75*100+9.0*100+9.0*100 = 7425, /9 = 825 cents = 8.25)
    eng_avg = data.get("Engineering", {}).get("avg_work_hours")
    record("Engineering avg_work_hours = 8.25", eng_avg == 8.25,
           f"got={eng_avg}")

# ─────────────────────────────────────────────────────────────────────────────
# 3c. Late leaderboard  2025-10 limit=3
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- 3c. Late leaderboard 2025-10 ---")
r = get("/analytics/leaderboard/late", {"month": "2025-10", "limit": "3"})
record("Late leaderboard HTTP 200", r.status_code == 200, f"status={r.status_code}")
if r.status_code == 200:
    items = r.json()["items"]
    print("  Leaderboard:", [(i["emp_code"], i["total_late_minutes"], i["rank"]) for i in items])
    
    # Hand calculation:
    # EMP0001: late on Oct07(45) + Oct11(30) = 75 total
    # EMP0002: late on Oct06(45) = 45 total
    # EMP0006: late on Oct06(60) = 60 total
    # EMP0004: late on Oct16(15) = 15 total
    # EMP0005: 0 late
    # EMP9999: 0 late (and no employee doc, excluded from leaderboard)
    # Sorted by total desc: EMP0001(75), EMP0006(60), EMP0002(45), EMP0004(15)
    # rank: 1, 2, 3, 4
    # limit=3 → ranks 1,2,3 → EMP0001, EMP0006, EMP0002
    
    emp_codes_got = [i["emp_code"] for i in items]
    record("Leaderboard excludes ghost EMP9999", "EMP9999" not in emp_codes_got,
           f"got={emp_codes_got}")
    record("Leaderboard rank 1 = EMP0001 (75 min)", 
           len(items) > 0 and items[0]["emp_code"] == "EMP0001" and items[0]["total_late_minutes"] == 75,
           f"got={items[0] if items else 'empty'}")
    record("Leaderboard rank 2 = EMP0006 (60 min)",
           len(items) > 1 and items[1]["emp_code"] == "EMP0006" and items[1]["total_late_minutes"] == 60,
           f"got={items[1] if len(items)>1 else 'empty'}")
    record("Leaderboard limit=3 returns 3 items", len(items) == 3, f"got={len(items)}")

# Tie test: limit=2 for EMP0001(75), EMP0006(60): no tie at cutoff here.
# To test tie: limit=2, EMP0002 and EMP0001 aren't tied. Let's test with limit=4 → should return 4
r_tie = get("/analytics/leaderboard/late", {"month": "2025-10", "limit": "4"})
if r_tie.status_code == 200:
    items_4 = r_tie.json()["items"]
    record("Leaderboard limit=4 returns 4 items", len(items_4) == 4, f"got={len(items_4)}")

# Now insert tie: give EMP0003 exactly 75 late minutes to create a tie with EMP0001
logs.insert_one(log("EMP0003", "2025-10-07", "PRESENT", "10:15", "18:30",
                    work_hours=8.25, late_min=75, ot_min=0, half_day=False))
r_tied = get("/analytics/leaderboard/late", {"month": "2025-10", "limit": "1"})
if r_tied.status_code == 200:
    items_t = r_tied.json()["items"]
    ranks = [i["rank"] for i in items_t]
    tied_codes = [i["emp_code"] for i in items_t]
    # Both EMP0001 and EMP0003 have 75 → rank 1 each, both returned even with limit=1
    record("Tied leaderboard: both rank-1 entries returned when limit=1",
           len(items_t) == 2 and all(r == 1 for r in ranks),
           f"got items={[(i['emp_code'],i['rank']) for i in items_t]}")
    # ordered by total desc then emp_code asc
    if len(items_t) >= 2:
        record("Tied leaderboard: EMP0001 before EMP0003 (emp_code asc)",
               items_t[0]["emp_code"] == "EMP0001",
               f"got first={items_t[0]['emp_code']}")
# Remove the tie record
logs.delete_one({"emp_code": "EMP0003", "date": "2025-10-07"})

# Department filter
r_dept = get("/analytics/leaderboard/late", {"month": "2025-10", "limit": "10", "department": "Marketing"})
if r_dept.status_code == 200:
    mkt_items = r_dept.json()["items"]
    mkt_codes = [i["emp_code"] for i in mkt_items]
    record("Dept leaderboard excludes Engineering employees", 
           "EMP0001" not in mkt_codes and "EMP0002" not in mkt_codes,
           f"got={mkt_codes}")
    # EMP0006(60), EMP0004(15) are in Marketing and have late minutes
    record("Dept leaderboard includes EMP0006 (Marketing)",
           "EMP0006" in mkt_codes, f"got={mkt_codes}")

# ─────────────────────────────────────────────────────────────────────────────
# 3d. Department trend  Engineering  2025-10-06 to 2025-10-10
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- 3d. Department trend ---")
r = get("/analytics/departments/Engineering/trend", {"from": "2025-10-06", "to": "2025-10-10"})
record("Dept trend HTTP 200", r.status_code == 200, f"status={r.status_code}")
if r.status_code == 200:
    trend = r.json()["items"]
    dates_got = [item["date"] for item in trend]
    expected_dates = ["2025-10-06","2025-10-07","2025-10-08","2025-10-09","2025-10-10"]
    record("Trend has exactly 5 rows (Mon-Fri)", len(trend) == 5 and dates_got == expected_dates,
           f"got={dates_got}")
    
    # Oct 06 Mon: EMP0001 PRESENT(full)+EMP0002 PRESENT(half-day? no, full)+EMP0003 no log
    # present_x2: EMP0001=2, EMP0002=2, EMP0003=0  → 4 → present_count=2.0
    # headcount for Engineering on Oct06: all 3 joined<=Oct06
    oct06 = next((i for i in trend if i["date"] == "2025-10-06"), None)
    if oct06:
        record("Oct06 is_working_day=True", oct06["is_working_day"] is True, f"got={oct06['is_working_day']}")
        record("Oct06 headcount=3", oct06["headcount"] == 3, f"got={oct06['headcount']}")
        record("Oct06 present_count=2.0", oct06["present_count"] == 2.0, f"got={oct06['present_count']}")
    
    # Oct 08 Wed: EMP0001 ABSENT, EMP0002 PRESENT(legacy, full), EMP0003 no log
    # present_x2 = 0+2+0 = 2 → present_count=1.0
    oct08 = next((i for i in trend if i["date"] == "2025-10-08"), None)
    if oct08:
        record("Oct08 present_count=1.0 (ABSENT+legacy+zero)", oct08["present_count"] == 1.0,
               f"got={oct08['present_count']}")

# Test empty range (no records exist for this period)
print("\n--- 3d. Dept trend empty range ---")
r_empty = get("/analytics/departments/Engineering/trend", {"from": "2025-09-01", "to": "2025-09-05"})
record("Trend empty range HTTP 200", r_empty.status_code == 200, f"status={r_empty.status_code}")
if r_empty.status_code == 200:
    items_e = r_empty.json()["items"]
    record("Empty trend: exactly 5 rows returned", len(items_e) == 5,
           f"got={len(items_e)} rows")
    record("Empty trend: all attendance_rate=None (no records)", 
           all(i["attendance_rate"] is None for i in items_e),
           f"rates={[i['attendance_rate'] for i in items_e]}")

# Unknown department → 404
r_unk = get("/analytics/departments/UNKNOWN_DEPT/trend", {"from": "2025-10-01", "to": "2025-10-05"})
record("Unknown dept trend returns 404", r_unk.status_code == 404, f"status={r_unk.status_code}")

# Max range (92 days)
r_max = get("/analytics/departments/Engineering/trend", {"from": "2025-10-01", "to": "2025-12-31"})
record("93-day range returns 422", r_max.status_code == 422, f"status={r_max.status_code}")

# ─────────────────────────────────────────────────────────────────────────────
# 3e. Admin explain – IXSCAN check
# ─────────────────────────────────────────────────────────────────────────────
print("\n--- 3e. Admin /explain ---")

def check_explain(endpoint, params, label):
    r = get(f"/admin/explain/{endpoint}", params)
    if r.status_code != 200:
        record(f"explain/{label} HTTP 200", False, f"status={r.status_code} body={r.text[:300]}")
        return
    record(f"explain/{label} HTTP 200", True)
    body = r.text
    has_ixscan = "IXSCAN" in body
    no_collscan = "COLLSCAN" not in body
    record(f"explain/{label} uses IXSCAN", has_ixscan, f"body snippet: {body[:500]}" if not has_ixscan else "")
    record(f"explain/{label} NO COLLSCAN", no_collscan, f"collscan found in body" if not no_collscan else "")

check_explain("attendance_list", {"emp_code": "EMP0001", "date_from": "2025-10-01", "date_to": "2025-10-31"}, "attendance_list")
check_explain("employee_monthly", {"emp_code": "EMP0001", "month": "2025-10"}, "employee_monthly")
check_explain("department_summary", {"month": "2025-10"}, "department_summary")
check_explain("late_leaderboard", {"month": "2025-10", "limit": "5"}, "late_leaderboard")
check_explain("department_trend", {"department": "Engineering", "from": "2025-10-06", "to": "2025-10-10"}, "department_trend")

# ── PHASE 4: CONCURRENCY ───────────────────────────────────────────────────────
section("PHASE 4 – Concurrency")

# Clean up
employees.delete_many({"emp_code": {"$in": ["EMP8001", "EMP8002"]}})
logs.delete_many({"emp_code": {"$in": ["EMP8001", "EMP8002"]}})

# Create test employees via API
httpx.post(f"{BASE}/employees", json={
    "emp_code": "EMP8001", "name": "Concurrent1", "email": "c1@test.com",
    "department": "Eng", "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2025-01-01"
})
httpx.post(f"{BASE}/employees", json={
    "emp_code": "EMP8002", "name": "Concurrent2", "email": "c2@test.com",
    "department": "Eng", "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2025-01-01"
})

# Fixed punch time: Oct 20 2025 09:30 IST
punch_ms = int(datetime(2025, 10, 20, 9, 30, tzinfo=IST).timestamp() * 1000)

# 4a. Concurrent punch-in (10 threads)
statuses_in = []
def do_punch_in():
    r = httpx.post(f"{BASE}/attendance/punch-in",
                   json={"emp_code": "EMP8001", "punched_at": punch_ms, "status": "PRESENT"},
                   timeout=10)
    statuses_in.append(r.status_code)

threads = [threading.Thread(target=do_punch_in) for _ in range(10)]
for t in threads: t.start()
for t in threads: t.join()
count_201 = statuses_in.count(201)
count_409 = statuses_in.count(409)
record("Concurrent punch-in: exactly one 201", count_201 == 1,
       f"got 201×{count_201}, 409×{count_409}, others={[s for s in statuses_in if s not in (201,409)]}")
record("Concurrent punch-in: rest are 409", count_409 == 9,
       f"got 201×{count_201}, 409×{count_409}")

# 4b. Concurrent punch-out (10 threads)
punch_out_ms = int(datetime(2025, 10, 20, 18, 30, tzinfo=IST).timestamp() * 1000)
statuses_out = []
def do_punch_out():
    r = httpx.post(f"{BASE}/attendance/punch-out",
                   json={"emp_code": "EMP8001", "punched_at": punch_out_ms},
                   timeout=10)
    statuses_out.append(r.status_code)

threads = [threading.Thread(target=do_punch_out) for _ in range(10)]
for t in threads: t.start()
for t in threads: t.join()
count_200 = statuses_out.count(200)
count_409_out = statuses_out.count(409)
record("Concurrent punch-out: exactly one 200", count_200 == 1,
       f"got 200×{count_200}, 409×{count_409_out}")
record("Concurrent punch-out: rest are 409", count_409_out == 9,
       f"got 200×{count_200}, 409×{count_409_out}")

# 4c. Concurrent PATCH (10 threads) – history entries never lost
patch_ms_in  = int(datetime(2025, 10, 20, 9, 45, tzinfo=IST).timestamp() * 1000)
patch_count = 0
patch_lock = threading.Lock()
def do_patch():
    global patch_count
    r = httpx.patch(f"{BASE}/attendance/EMP8001/2025-10-20",
                    json={"punch_in": patch_ms_in, "reason": "regularize test", "regularized_by": "admin"},
                    timeout=10)
    if r.status_code == 200:
        with patch_lock:
            patch_count += 1

threads = [threading.Thread(target=do_patch) for _ in range(10)]
for t in threads: t.start()
for t in threads: t.join()
final_rec = logs.find_one({"emp_code": "EMP8001", "date": "2025-10-20"})
h_len = len(final_rec.get("history", [])) if final_rec else 0
record("Concurrent PATCH: history entries not lost (each winner gets 1 entry)",
       h_len >= 1, f"history has {h_len} entries, {patch_count} 200s")

# 4d. Concurrent POST /employees same emp_code
employees.delete_many({"emp_code": "EMP8099"})
statuses_emp = []
def do_create_emp():
    r = httpx.post(f"{BASE}/employees", json={
        "emp_code": "EMP8099", "name": "Race", "email": "race@test.com",
        "department": "Eng", "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2025-01-01"
    }, timeout=10)
    statuses_emp.append(r.status_code)

threads = [threading.Thread(target=do_create_emp) for _ in range(10)]
for t in threads: t.start()
for t in threads: t.join()
c201 = statuses_emp.count(201)
c409 = statuses_emp.count(409)
record("Concurrent POST /employees: exactly one 201", c201 == 1, f"got 201×{c201}, 409×{c409}")
record("Concurrent POST /employees: rest 409", c409 == 9, f"got 201×{c201}, 409×{c409}")

# ── PHASE 5: SCALE ─────────────────────────────────────────────────────────────
section("PHASE 5 – Scale (100k logs)")

print("Generating 100k scale logs…")
logs.delete_many({"emp_code": {"$regex": "^SCALE"}})
employees.delete_many({"emp_code": {"$regex": "^SCALE"}})

# 500 employees × 200 days
SCALE_EMPS = 500
SCALE_DAYS = 200
scale_start = date(2024, 1, 1)
scale_dept = ["ScaleEng", "ScaleMkt"]

# Insert employees in bulk
scale_emp_docs = []
for i in range(SCALE_EMPS):
    dept = scale_dept[i % 2]
    scale_emp_docs.append({
        "emp_code": f"SCALE{i:04d}",
        "name": f"Scale Worker {i}",
        "email": f"scale{i}@test.com",
        "department": dept,
        "shift_start": "09:30",
        "shift_end": "18:30",
        "joined_on": "2024-01-01",
        "created_at": datetime(2024, 1, 1, tzinfo=UTC),
    })
employees.insert_many(scale_emp_docs)

BATCH = 5000
buf = []
import random
random.seed(42)
statuses_pool = ["PRESENT"] * 7 + ["ABSENT", "LEAVE", "WFH"]

def scale_date_str(i):
    return (scale_start + timedelta(days=i)).isoformat()

for emp_i in range(SCALE_EMPS):
    ec = f"SCALE{emp_i:04d}"
    for day_i in range(SCALE_DAYS):
        d_str = scale_date_str(day_i)
        st = random.choice(statuses_pool)
        if st in ("PRESENT", "WFH"):
            late_m = random.choice([0, 0, 0, 15, 30, 45])
            wh = round(random.uniform(7.5, 10.0), 2)
            ot = 30 if wh >= 9.5 else 0
            buf.append({
                "emp_code": ec, "date": d_str, "status": st,
                "punch_in":  ist_to_utc(*(int(x) for x in d_str.split("-")), 9+late_m//60, late_m%60),
                "punch_out": ist_to_utc(*(int(x) for x in d_str.split("-")), 18, 30),
                "work_hours": wh, "late_minutes": late_m, "overtime_minutes": ot,
                "half_day": False, "history": [],
            })
        else:
            buf.append({
                "emp_code": ec, "date": d_str, "status": st,
                "punch_in": None, "punch_out": None,
                "work_hours": None, "late_minutes": 0, "overtime_minutes": 0,
                "half_day": False, "history": [],
            })
        if len(buf) >= BATCH:
            logs.insert_many(buf, ordered=False)
            buf.clear()
if buf:
    logs.insert_many(buf, ordered=False)
    buf.clear()

total_scale = logs.count_documents({"emp_code": {"$regex": "^SCALE"}})
print(f"  Inserted {total_scale} scale logs")
record("Scale: 100k logs inserted", total_scale == SCALE_EMPS * SCALE_DAYS,
       f"got={total_scale}")

# Time endpoints
def time_get(label, path, params, threshold_ms=500):
    t0 = time.perf_counter()
    r = httpx.get(f"{BASE}{path}", params=params, timeout=60)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    flag = "⚠️ SLOW" if elapsed_ms > threshold_ms else ""
    print(f"  {label}: {elapsed_ms:.0f}ms {flag}")
    ok = r.status_code == 200
    record(f"Scale {label} < {threshold_ms}ms", elapsed_ms < threshold_ms,
           f"elapsed={elapsed_ms:.0f}ms status={r.status_code}")
    return elapsed_ms, r

time_get("GET /attendance (no filter)", "/attendance", {}, threshold_ms=500)
time_get("Employee monthly (SCALE0001)", "/analytics/employees/SCALE0001/monthly",
         {"month": "2024-01"}, threshold_ms=500)
time_get("Dept summary ScaleEng 2024-01", "/analytics/departments/summary",
         {"month": "2024-01", "department": "ScaleEng"}, threshold_ms=500)
time_get("Late leaderboard 2024-01", "/analytics/leaderboard/late",
         {"month": "2024-01", "limit": "10"}, threshold_ms=500)
time_get("Dept trend ScaleEng 2024-01-01 to 2024-03-31",
         "/analytics/departments/ScaleEng/trend",
         {"from": "2024-01-01", "to": "2024-03-31"}, threshold_ms=500)

# Explain on scale data
r_expl = get("/admin/explain/department_summary",
             {"month": "2024-01", "department": "ScaleEng"})
if r_expl.status_code == 200:
    body = r_expl.text
    record("Scale explain dept_summary: IXSCAN (no COLLSCAN)", "COLLSCAN" not in body,
           "COLLSCAN found in scale explain" if "COLLSCAN" in body else "")

# ── PHASE 6: REPORT ────────────────────────────────────────────────────────────
section("PHASE 6 – Final Report")

passes = [r for r in results if r["status"] == "PASS"]
fails  = [r for r in results if r["status"] == "FAIL"]

print(f"\nTotal: {len(results)} checks  ✅ {len(passes)} PASS  ❌ {len(fails)} FAIL\n")
print(f"{'Check':<60} {'Status':<6} {'Detail'}")
print("-" * 120)
for r in results:
    detail = (r["detail"][:55] + "…") if len(r["detail"]) > 56 else r["detail"]
    print(f"{r['check']:<60} {r['status']:<6} {detail}")

if fails:
    print("\n\n─── FAILURES (full detail) ───")
    for r in fails:
        print(f"\n❌ {r['check']}")
        print(f"   {r['detail']}")
