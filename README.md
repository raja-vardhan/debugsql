# DebugSQL  
A command-line tool for explaining unexpected SQL query results.

DebugSQL helps analysts understand *why* a SQL query produced a surprising output—whether an aggregate is too large, a join returns unexpected counts, predicates behave incorrectly, or a tuple that “should” be in the output is missing.

It supports five major debugging modes:

1. **Aggregate analysis** – Identify tuples contributing disproportionately to SUM/COUNT/AVG.  
2. **Join analysis** – Diagnose unexpected join cardinalities and mismatched join keys.  
3. **Predicate analysis** – Understand how WHERE predicates filter tuples.  
4. **Why-Not analysis** – Explain why a tuple is missing from the query output.  

DebugSQL draws inspiration from prior work such as DataPrism, Why-Not provenance, and query reverse engineering.

---

## Features

### Aggregate Contribution Analysis (`agg`)
- Identify outlier tuples contributing to unexpected aggregates.
- Supports specifying expected SUM/COUNT/AVG.

### Join Diagnosis (`join`)
- Explain unexpectedly high or low join output size.
- Reveal missing join partners and mismatched key values.

### Predicate Debugging (`predicate`)
- Show which predicates filtered which tuples.
- Helps debug complex boolean logic.

### Why-Not Explanations (`why-not`)
- Explain the absence of a tuple from the result.
- Supports output modes: `summary`, `detailed`, `both`.

---

## Installation

```bash
git clone <your-repo-url>
cd debugsql
pip install -r requirements.txt
```

Configure PostgreSQL credentials in `src/db.py`.

---

## Usage

The main entry point is:

```bash
python src/debugsql.py <mode> --query "<SQL>"
```

Where `<mode>` is one of:

```
agg | join | predicate | why-not
```

If no mode is given, DebugSQL simply executes the SQL query and prints its result.

---

# Examples

---

## 1. Aggregate Analysis

```bash
python src/debugsql.py agg \
    --query "SELECT SUM(amount * multiplier) FROM Sales JOIN ExchangeRates USING(region);" \
    --expected-sum 5000
```

Accepted expectations:

```
--expected-sum <float>
--expected-count <float>
--expected-avg <float>
```

---

## 2. Join Key Mismatch Analysis

```bash
python src/debugsql.py join \
    --query "SELECT * FROM A JOIN B ON A.id = B.id" \
    --expected-count 100
```

---

## 3. Predicate Debugging

```bash
python src/debugsql.py predicate \
    --query "SELECT * FROM Movie WHERE year > 2010 AND rating > 8;"
```

DebugSQL extracts WHERE predicates and analyzes tuple filtering.

---

## 4. Why-Not Explanation

```bash
python src/debugsql.py why-not \
    --query "SELECT * FROM movie m JOIN genre g ON m.id = g.mid WHERE g.genre = 'Comedy';" \
    --table Movie \
    --key "m.name = 'Inception'" \
    --output detailed
```

Valid output modes:

```
summary | detailed | both
```

---

## Project Structure

```
src/
 ├── debugsql.py              # Main CLI
 ├── analyzer/
 │    ├── aggregate.py
 │    ├── join.py
 │    ├── predicates.py
 │    ├── nonagg.py
 │    ├── expected.py
 │    └── why_not.py
 ├── sqlmeta.py               # Query parsing and metadata
 ├── db.py                    # PostgreSQL connection wrapper
 ├── utils.py                 # Table formatting helpers
```
