# DECISIONS.md 

## 1. Indexes

**Created Indexes:**

**Employees Collection:**
- `emp_code` (unique) - Ensures employee codes are unique and enables fast employee lookup by code
- `(department, emp_code)` - Optimizes department-filtered employee queries
- `joined_on` - Supports filtering employees by join date
- `(department, joined_on)` - Optimizes department summary queries filtering by both department and join date

**Attendance Logs Collection:**
- `(emp_code, date)` (unique) - **Critical for punch-in race handling** - ensures only one attendance record per employee per day. Also optimizes per-employee queries like monthly summaries
- `(date DESC, emp_code ASC)` - Supports the default attendance listing order (most recent first) and date range queries
- `(status, date DESC, emp_code ASC)` - Optimizes status-filtered attendance queries with proper sorting
- `(emp_code, punch_in DESC)` - Enables efficient punch-out queries to find the latest punch-in record for an employee

**Rejected Indexes:**
- Considered a compound index on `(department, date)` for attendance logs, but rejected because the department filter requires a join with employees collection anyway, so filtering happens at the application level after the join.

## 2. Punch-in Race

When two identical punch-in requests arrive simultaneously:

1. Both requests call `punch_in()` and retrieve the same employee record
2. Both calculate the same `attendance_date` based on the punch-in timestamp
3. Both construct identical documents with `emp_code` and `date`
4. Both attempt `logs.insert_one(doc)` concurrently
5. **MongoDB's unique index on `(emp_code, date)` acts as an atomic arbiter:**
   - The first insert to reach MongoDB succeeds and creates the document
   - The second insert fails with a `DuplicateKeyError`
6. The winning request returns 201 with the created attendance record
7. The losing request catches `DuplicateKeyError` and raises `HTTPException(409, "already punched in for this date")`

This approach is **race-safe without explicit locking** because the database index guarantees atomicity.

## 3. Ties

When employees are tied on late minutes at the cutoff:

**Example:** `limit=5`, employees ranked 4, 5, 5, 5, 8

**What happens:**
- MongoDB's `$rank` window function uses **standard competition ranking** (1, 2, 2, 4...)
- All three employees with the 5th-place total get `rank: 5`
- The `{"$match": {"rank": {"$lte": limit}}}` stage includes **all employees with rank ≤ limit**
- **Result: All three tied employees are included**, so the API returns 6 employees instead of 5

**Why this behavior:**
- Prevents arbitrary cutoffs that exclude deserving employees
- Transparent tie-breaking: ties are decided by `total` late minutes only, then sorted secondarily by `emp_code` for consistent ordering
- Matches standard competition ranking behavior where tied competitors share the same rank

## 4. Headcount

The department summary ensures employees with zero logs are counted by:

1. **Starting from the employees collection** (`{"$match": emp_match}` on employees)
2. Using `$lookup` to **left join** attendance_logs for each employee
   - If an employee has no logs, the join returns an empty array
3. Setting missing aggregates to empty object: `{"$set": {"s": {"$ifNull": [{"$arrayElemAt": ["$s", 0]}, {}]}}}`
4. In the final `$group`, using `$ifNull` to treat missing values as 0:
   - `"headcount": {"$sum": 1}` counts every employee from the initial match
   - `"present_x2": {"$sum": {"$ifNull": ["$s.present_x2", 0]}}`
   - Other metrics similarly default to 0

**Key insight:** By starting from employees (not logs) and using left join semantics with `$lookup`, every employee contributes to headcount regardless of attendance records.

## 5. Scaling to 100x Data

**Primary change: Implement time-based partitioning (sharding) on attendance_logs**

**Strategy:**
- Shard attendance_logs by `date` (range-based sharding by month/quarter)
- Keeps recent data on hot shards, archives old data to cold storage
- Most queries naturally filter by date range, so sharding aligns with query patterns

**Additional optimizations:**
- Add a **caching layer** (Redis) for frequently accessed analytics (department summaries, leaderboards)
- Implement **materialized views** for pre-aggregated monthly summaries instead of computing on-the-fly
- Move heavy analytics queries to **read replicas** to offload primary database
- Add **pagination limits** to prevent unbounded result sets
- Consider **batch processing** for trend calculations using scheduled jobs instead of real-time computation
- Implement **connection pooling** with proper sizing for increased concurrent requests
