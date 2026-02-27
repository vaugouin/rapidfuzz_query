# `rapidfuzz_query.py` (RapidFuzz + MariaDB/MySQL)

This repository contains a small **person-name search / autocorrect** module built on:

- **MariaDB / MySQL** for candidate retrieval
- **RapidFuzz** for lexical similarity scoring

It can be used as:

- A **CLI** interactive checker (run the script)
- A **Python module** (import and call `search_first_match()`), intended to be reused by other projects (e.g. a FastAPI Text2SQL API)

---

## Key features

## 1) Name normalization

The module normalizes user input to improve matching quality.

- **`normalize_name(s)`**
  - Lowercases
  - Removes non-alphanumeric characters (keeps spaces)
  - Collapses repeated whitespace

- **`to_key(s)`**
  - Builds a compact key used for prefix searching
  - `normalize_name(s)` then removes spaces

These functions are designed to be consistent with the database **generated columns** described below.

---

## 2) Database-backed candidate retrieval (fast shortlist)

The search is intentionally a **two-step pipeline**:

1. Use the database to retrieve a reasonably small candidate set
2. Use RapidFuzz to score/rank that set

Candidate retrieval is performed by `fetch_candidates()` using multiple strategies:

- **Prefix strategy (primary)**
  - Query: `PERSON_NAME_KEY LIKE '<prefix>%'`
  - Intended to use an index on `PERSON_NAME_KEY`

- **FULLTEXT strategy (fallback, recommended)**
  - Query: `MATCH(PERSON_NAME_NORM) AGAINST (... IN BOOLEAN MODE)`
  - Requires a FULLTEXT index on the normalized column

- **LIKE strategy (last resort)**
  - Query: `PERSON_NAME_NORM LIKE '%token%'`
  - Typically slow on large tables; keep limits reasonable

### FULLTEXT boolean query builder

- **`build_boolean_query(tokens)`** builds a boolean-mode query string.
  - Tokens of length >= 4 get a trailing `*` for prefix matching.

---

## 3) RapidFuzz ranking

Ranking is done with:

- `rapidfuzz.process.extract()`
- scorer: **`rapidfuzz.fuzz.WRatio`**

The module builds a `choices` dictionary of `{ID -> normalized_string}` and extracts the top `TOP_K` matches.

### Tie-breaker: POPULARITY

When two results have identical RapidFuzz scores, the module **breaks ties using `POPULARITY`** (descending).

This is implemented by sorting the ranked list by:

- `SCORE` (descending)
- `POPULARITY` (descending)

---

## 4) Auto-correct decision logic

The module can decide whether it is confident enough to auto-correct.

- **`AUTO_SCORE`**: minimum score to auto-correct
- **`MIN_MARGIN`**: `top1_score - top2_score` must be >= this margin

Function:

- **`decide_autocorrect(ranked)`** returns:
  - `auto` (bool)
  - best candidate (or `None`)
  - `reason` string

---

## 5) Module-friendly search API

### `search_first_match()` (recommended entry point for imports)

`search_first_match()` encapsulates the full pipeline:

- exact match
- candidate fetch
- RapidFuzz ranking
- auto-correct decision

It is designed to be imported by other projects.

#### Behavior guarantee

If there is **no exact match** but there are ranked suggestions, the function:

- **always returns the first suggestion** as `best` (i.e. `ranked[0]`)
  - even when not confident enough to auto-correct

#### Signature (current)

```python
search_first_match(
    cur,
    strtablename: str,
    strcolumnid: str,
    strcolumndesc: str,
    strcolumndescnorm: str,
    strcolumndesckey: str,
    strcolumnpopularity: str,
    raw: str,
    has_fulltext: bool,
    timings_enabled: bool = False,
) -> Dict[str, Any]
```

#### Return structure

The function returns a dict containing:

- `hit`: exact-match row dict or `None`
- `ranked`: list of ranked suggestions (possibly empty)
- `auto`: bool (whether it would auto-correct)
- `best`: the selected best row (exact-hit row OR `ranked[0]` OR `None`)
- `reason`: string (e.g. `"exact"`, `"auto(...)"`, `"suggest(...)"`)
- `candidates_count`: integer count of fetched candidates
- `timings`: optional dict of timing details

Importantly:

- `search_first_match()` **does not print**.

---

## 6) CLI interactive mode

Running the script directly provides an interactive prompt.

It:

- reads a person name using `input()`
- measures wall-clock duration for the search
- prints either:
  - exact match (valid)
  - auto-correction result
  - suggestion list (with the selected `Best` line)

---

## Configuration

## Tunable scoring/search constants

At the top of the file:

- `AUTO_SCORE`
- `MIN_MARGIN`
- `TOP_K`
- `PREFIX_LIMIT`
- `FTX_LIMIT`
- `LIKE_LIMIT`
- `MIN_CANDIDATES_OK`

These control both speed and behavior.

---

## Table/column constants

The module defines constants for the target table and columns:

- `PERSON_TABLE`
- `COL_ID_PERSON`
- `COL_PERSON_NAME`
- `COL_PERSON_NAME_NORM`
- `COL_PERSON_NAME_KEY`
- `COL_POPULARITY`

All DB helper/search functions are written to accept:

- `strtablename`
- `strcolumnid`
- `strcolumndesc`
- `strcolumndescnorm`
- `strcolumndesckey`
- `strcolumnpopularity`

This makes the module easier to reuse with different schemas.

---

## Environment variables

### DB connection

The module reads:

- `DB_HOST` (default `127.0.0.1`)
- `DB_PORT` (default `3306`)
- `DB_USER` (default `root`)
- `DB_PASS` or `DB_PASSWORD`
- `DB_NAME` (**required**, otherwise the script exits)

### Timing logs

- `TIMINGS=1` enables detailed timing breakdown output in CLI mode.

---

## Dependencies

From `requirements.txt`:

- `rapidfuzz`
- `python-dotenv` (optional; loads `.env` if installed)
- `pymysql`

The DB driver uses:

- `pymysql.connect(..., cursorclass=pymysql.cursors.DictCursor)`

so rows are returned as dictionaries.

---

## Database schema expectations

The module expects at least these columns to exist:

- `PERSON_NAME_NORM` (normalized name)
- `PERSON_NAME_KEY` (compact key for prefix searching)

It checks this at startup via `INFORMATION_SCHEMA.COLUMNS`.

### Recommended indexes

For performance, you generally want:

- Index on `PERSON_NAME_KEY` (for prefix lookup)
- Index on `PERSON_NAME_NORM` (for exact match)
- FULLTEXT on `PERSON_NAME_NORM` (optional, but strongly recommended)

---

## Creating a FULLTEXT index (example)

```sql
ALTER TABLE T_WC_T2S_PERSON
  ADD FULLTEXT INDEX ft_person_name_norm (PERSON_NAME_NORM);
```

Verify:

```sql
SHOW INDEX FROM T_WC_T2S_PERSON WHERE Index_type='FULLTEXT';
```

---

## Timing / profiling

When `TIMINGS=1`, CLI mode prints:

- `exact_match` duration
- `fetch_total` duration
- `rank` duration
- breakdown of candidate retrieval stages:
  - prefix time + row count
  - fulltext time + row count
  - like time + row count

This is intended to help you locate whether the bottleneck is:

- DB retrieval (too many candidates or missing indexes)
- RapidFuzz scoring (too many candidates)

---

## Example: import usage

Below is a minimal example of using this module from another Python project.

```python
import rapidfuzz_query

conn = rapidfuzz_query.get_db_connection()
cur = conn.cursor()

has_fulltext = rapidfuzz_query.db_has_fulltext(
    cur,
    rapidfuzz_query.PERSON_TABLE,
    rapidfuzz_query.COL_PERSON_NAME_NORM,
)

result = rapidfuzz_query.search_first_match(
    cur,
    rapidfuzz_query.PERSON_TABLE,
    rapidfuzz_query.COL_ID_PERSON,
    rapidfuzz_query.COL_PERSON_NAME,
    rapidfuzz_query.COL_PERSON_NAME_NORM,
    rapidfuzz_query.COL_PERSON_NAME_KEY,
    rapidfuzz_query.COL_POPULARITY,
    raw="jenny aguter",
    has_fulltext=has_fulltext,
    timings_enabled=False,
)

best = result["best"]
if best is None:
    print("No match")
else:
    print("Best:", best)
```

---

## Notes / cautions

- This module dynamically injects the **table/column identifiers** into SQL using backticks.
  - It assumes these identifier strings are trusted constants from your code (not user input).
- For production API usage:
  - keep candidate limits reasonable
  - ensure indexes exist
  - consider connection pooling (on the FastAPI side)

---

## Changelog (high-level)

- Added module-friendly `search_first_match()` for reuse
- Added optional `TIMINGS` instrumentation
- Added `POPULARITY` tie-breaker for equal RapidFuzz scores
- Threaded `strtablename` / `strcolumn*` params through helper functions
