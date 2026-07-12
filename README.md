# RapidFuzz Query

`rapidfuzz_query` is a person-name search and auto-correction module for the Agent BBB movie/TV database. It resolves approximate, misspelled, or partial person names against the shared MySQL/MariaDB `T_WC_*` tables, combining indexed candidate retrieval with [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) lexical similarity to pick (and optionally auto-correct to) the best matching person.

The single module [rapidfuzz_query.py](rapidfuzz_query.py) is imported by the `fastapi-text2sql` API to map free-text person names extracted from user questions onto canonical database rows. It also ships an interactive command-line harness (the `main()` loop) for ad-hoc testing against a live database.

For agent / contributor conventions see [AGENTS.md](AGENTS.md).

---

## What it does

Given a raw name string and a target table description, the module:

1. **Normalizes** the input. `normalize_name()` lowercases, replaces every non-alphanumeric character (including punctuation) with a space, and collapses whitespace. `to_key()` further strips all spaces to produce a compact prefix key. These mirror the `STORED` generated columns defined in [T2S_PERSON-rapidfuzz.sql](T2S_PERSON-rapidfuzz.sql) and [T_WC_TMDB_PERSON_ALSO_KNOWN_AS-rapidfuzz.sql](T_WC_TMDB_PERSON_ALSO_KNOWN_AS-rapidfuzz.sql).
2. **Tries an exact match** on the normalized column (`PERSON_NAME_NORM`). A hit returns immediately.
3. **Otherwise builds a candidate pool** via the multi-tier retrieval below.
4. **Ranks candidates** with RapidFuzz `fuzz.WRatio` and decides whether the top match is strong enough to auto-correct.

### Candidate-retrieval strategy

The pool is built by `fetch_candidates()` in additive layers until it reaches `MIN_CANDIDATES_OK` (200) candidates:

1. **Prefix lookup** on the key column `PERSON_NAME_KEY` — `WHERE PERSON_NAME_KEY LIKE CONCAT(prefix, '%')`, where `prefix` is the first 6 characters of the query key (or fewer for short queries, minimum 3). Index-friendly, capped at `PREFIX_LIMIT` (5000) rows.
2. **BK-tree Levenshtein pool** (optional, when a pre-built `BKTreeIndex` is supplied). An in-memory BK-tree over the normalized name column returns every indexed name within a small edit distance of the query. This **always runs when available**, even when the prefix pool is already full, because it catches typos that sit *inside* the first characters (e.g. `RICARDO` vs `RICCARDO`) which a prefix lookup cannot reach. The max distance is chosen adaptively by query length (see [Configuration](#configuration)); up to `BKTREE_FETCH_CAP` (500) new ids are batch-fetched.
3. **FULLTEXT fallback** on `PERSON_NAME_NORM` — runs only if a FULLTEXT index exists and the pool is still below the threshold. Uses MariaDB boolean mode (`MATCH ... AGAINST (... IN BOOLEAN MODE)`) over the three longest tokens, each ≥4 chars getting a trailing `*` prefix wildcard. Capped at `FTX_LIMIT` (20000).
4. **LIKE fallback** (last resort) — `WHERE PERSON_NAME_NORM LIKE CONCAT('%', token, '%')` on the single longest token. Capped at `LIKE_LIMIT` (20000).

Each tier de-duplicates against ids already in the pool. When `timings` are enabled, the `used` field records which combination fired (e.g. `prefix`, `bktree+prefix`, `bktree+fulltext`, `bktree+like`).

### Ranking

`rank_candidates()` scores the normalized query against each candidate's normalized name using `rapidfuzz.process.extract` with `scorer=fuzz.WRatio`, keeping the top `TOP_K` (10). Results are sorted by descending `SCORE`, with the popularity column (`POPULARITY`) as a tie-breaker.

---

## Public API

The library entry points (intended to be imported; they do not print) are in [rapidfuzz_query.py](rapidfuzz_query.py).

### `search_first_match(...)`

The low-level search:

```python
search_first_match(
    cur,                       # pymysql DictCursor
    strtablename,              # e.g. "T_WC_T2S_PERSON"
    strcolumnid,               # e.g. "ID_PERSON"
    strcolumndesc,             # display column, e.g. "PERSON_NAME"
    strcolumndescnorm,         # normalized column, e.g. "PERSON_NAME_NORM"
    strcolumndesckey,          # key column, e.g. "PERSON_NAME_KEY"
    strcolumnpopularity,       # tie-breaker column, e.g. "POPULARITY"
    raw,                       # raw user input string
    has_fulltext,              # bool: is a FULLTEXT index present?
    timings_enabled=False,
    bktree=None,               # optional pre-built BKTreeIndex
) -> dict
```

Returns a dict with this shape:

| Key | Type | Meaning |
|-----|------|---------|
| `hit` | row dict or `None` | exact normalized match (when found) |
| `ranked` | list of dicts | ranked suggestions, each with the id/desc/norm/popularity fields plus a `SCORE` float (empty when there was an exact hit or no candidates) |
| `auto` | bool | whether the result is confident enough to auto-correct (always `True` for an exact `hit`) |
| `best` | row dict or `None` | top suggestion (the `hit` itself on an exact match) |
| `reason` | str | `exact`, `empty_query`, `no_candidates`, `auto(score=..., margin=...)`, or `suggest(score=..., margin=...)` |
| `timings` | dict | timing breakdown (empty unless `timings_enabled=True`) |
| `candidates_count` | int | number of candidates fetched |

### `search_first_match_configured(...)`

A higher-level, config-driven wrapper used by callers such as `fastapi-text2sql`. It takes a `config` dict (with `search`, `enrich`, and `enrich_mode` keys), calls `search_first_match()` under the hood, and returns the same top-level keys but wraps each row in a normalized **match object** via `build_match_object()`:

```python
{
  "source": <cmd>, "table": ..., "id": ..., "text": ..., "norm": ...,
  "fields": <raw row dict>, "enriched": {...}, "score": <float, when applicable>
}
```

The `search` sub-config carries the column mapping (`table`, `id`, `desc`, `norm`, `key`, `pop`) plus the resolved `has_fulltext` flag and an optional pre-built `bktree`. Optional `enrich` steps perform config-driven secondary lookups (e.g. attaching the canonical person row to an "also known as" alias hit); `enrich_mode` is `best_only` (default) or `all_ranked`.

### Helpers

`normalize_name()`, `to_key()`, `build_boolean_query()`, `exact_match()`, `fetch_candidates()`, `rank_candidates()`, `decide_autocorrect()`, the `BKTreeIndex` class, `build_bktree_for_config()`, `db_has_norm_columns()`, `db_has_fulltext()`, `db_lookup_by_id()`, `enrich_match_object()`, and `get_db_connection()` are also exported.

---

## Auto-correction logic

`decide_autocorrect()` inspects the ranked list using two thresholds:

- **`AUTO_SCORE = 90`** — the top candidate's WRatio score must be at least this high.
- **`MIN_MARGIN = 5`** — the gap between the top and second candidate's scores must be at least this large (if there is only one candidate, the margin is treated as effectively infinite).

If both conditions hold, the result is `auto=True` with reason `auto(score=..., margin=...)`; otherwise `auto=False` with reason `suggest(score=..., margin=...)` and the top candidate is still returned as `best` for display as a suggestion. These constants are module-level in [rapidfuzz_query.py](rapidfuzz_query.py) and can be tuned.

---

## Configuration

### Database connection (`.env`)

`get_db_connection()` reads connection settings from environment variables, loaded from a `.env` file via `python-dotenv`. Copy [.env.example](.env.example) to `.env` and fill in your own values:

| Variable | Default | Notes |
|----------|---------|-------|
| `DB_HOST` | `127.0.0.1` | |
| `DB_PORT` | `3306` | |
| `DB_USER` | `root` | |
| `DB_PASSWORD` | — | preferred; `DB_PASS` is accepted as a fallback |
| `DB_NAME` | — | **required** — the CLI exits if unset |
| `TIMINGS` | `0` | set `1`/`true`/`yes`/`on` to emit timing breakdowns |
| `BKTREE_ENABLED` | `1` | build the in-memory BK-tree at startup (CLI) |

The connection uses `pymysql` with a `DictCursor`, so cursor fetches return dictionaries and the MySQL/MariaDB `%s` parameter style applies.

> Do not commit real credentials. The checked-in `.env` is git-ignored via [.gitignore](.gitignore); only `.env.example` (with placeholder values) belongs in version control.

### Tunable module constants

Defined near the top of [rapidfuzz_query.py](rapidfuzz_query.py):

| Constant | Value | Purpose |
|----------|-------|---------|
| `AUTO_SCORE` | `90` | auto-correct score threshold |
| `MIN_MARGIN` | `5` | minimum top-1 vs top-2 score gap to auto-correct |
| `TOP_K` | `10` | number of suggestions ranked/returned |
| `PREFIX_LIMIT` | `5000` | row cap for the prefix tier |
| `FTX_LIMIT` | `20000` | row cap for the FULLTEXT tier |
| `LIKE_LIMIT` | `20000` | row cap for the LIKE tier |
| `MIN_CANDIDATES_OK` | `200` | pool size that lets later tiers be skipped |
| `BKTREE_LEN_SHORT` / `BKTREE_LEN_LONG` | `6` / `14` | query-length bands for adaptive BK-tree distance |
| `BKTREE_K_SHORT` / `BKTREE_K_MEDIUM` / `BKTREE_K_LONG` | `1` / `2` / `3` | max Levenshtein distance per band |
| `BKTREE_FETCH_CAP` | `500` | max new ids fetched from one BK-tree query |

### Required database columns

The target table must expose the `STORED` generated columns and indexes defined in the bundled SQL:

- [T2S_PERSON-rapidfuzz.sql](T2S_PERSON-rapidfuzz.sql) — adds `PERSON_NAME_NORM`, `PERSON_NAME_KEY`, their indexes, and the `ft_person_name_norm` FULLTEXT index to `T_WC_T2S_PERSON`.
- [T_WC_TMDB_PERSON_ALSO_KNOWN_AS-rapidfuzz.sql](T_WC_TMDB_PERSON_ALSO_KNOWN_AS-rapidfuzz.sql) — the equivalent for the alias table (Unicode-aware normalization).

`db_has_norm_columns()` and `db_has_fulltext()` probe `INFORMATION_SCHEMA` / `SHOW INDEX` at startup so the module degrades gracefully when the FULLTEXT index is absent.

---

## Running the test CLI

The `main()` loop provides an interactive harness against a live database. With a populated `.env`:

```bash
pip install -r requirements.txt
python rapidfuzz_query.py
```

At startup it builds the BK-tree (unless `BKTREE_ENABLED=0`) and then accepts commands:

```
person <person_name>     # search T_WC_T2S_PERSON
aka <person_name>        # search T_WC_TMDB_PERSON_ALSO_KNOWN_AS (enriched with canonical person)
help
quit / exit / q
```

Omitting the command word reuses the previously selected search set. Each result prints the validated/auto-corrected name (with Levenshtein distance and `ID_PERSON`), or a ranked suggestion list when confidence is low, plus per-tier timing details when `TIMINGS=1`.

### Docker

A [Dockerfile](Dockerfile) (Python 3.12 slim) and [start.sh](start.sh) are provided to build and run the CLI in a container, passing the `.env` via `--env-file` and `--network=host`:

```bash
docker build -t rapidfuzz_query-python-app .
docker run -it --rm --network="host" --env-file /path/to/.env --name rapidfuzz_query rapidfuzz_query-python-app
```

---

## Dependencies

Pinned (unversioned) in [requirements.txt](requirements.txt):

- **[rapidfuzz](https://github.com/rapidfuzz/RapidFuzz)** — `fuzz.WRatio` scoring and `Levenshtein` distance (used by the BK-tree and CLI output).
- **[pymysql](https://github.com/PyMySQL/PyMySQL)** — MySQL/MariaDB driver (`DictCursor`).
- **[python-dotenv](https://github.com/theskumar/python-dotenv)** — loads `.env` (import is optional/guarded).

> Note: the module docstring mentions `mariadb` as a prereq, but the actual runtime driver is `pymysql` as listed in `requirements.txt`.

---

## License

See [LICENSE](LICENSE).
