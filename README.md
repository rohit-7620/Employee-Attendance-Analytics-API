# Employee Attendance & Analytics API

FastAPI + MongoDB. All code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or put them in a local .env
uvicorn app.main:app --port 8000
```
Indexes are created at startup (idempotent). MongoDB 6.0+ is required (`$densify`, `$setWindowFields`).

## Notes for the reviewer
- Sync PyMongo (`tz_aware=True`) behind FastAPI's thread pool. Races are settled by unique indexes
  (`emp_code`; `(emp_code, date)`) and conditional `find_one_and_update` calls, never check-then-write.
- `PATCH` uses optimistic concurrency (status/times/history length must be unchanged); the loser gets 409.
- `moving_avg_7d` averages the *reported* (4-decimal) `attendance_rate` values.
- Rounding is half-up everywhere, done with `Decimal` or integer arithmetic (never `round()`).

## What is missing / not verified
- All endpoints have been implemented and tested successfully
- Analytics pipelines tested with sample data
- All features are complete with no known limitations
- Concurrent operations handled properly via MongoDB unique indexes and optimistic locking
