#!/usr/bin/env python3
"""
Interactive person-name checker + autocorrect using:
- MariaDB table T_WC_T2S_PERSON
- RapidFuzz lexical similarity (WRatio)

Behavior:
- If exact normalized match exists -> valid
- Else -> shortlist candidates via indexed prefix on PERSON_NAME_KEY
         fallback via FULLTEXT on PERSON_NAME_NORM (optional but recommended)
- Rank with RapidFuzz
- Auto-correct if score >= AUTO_SCORE and margin >= MIN_MARGIN
- Otherwise show suggestions

Prereqs:
  pip install mariadb rapidfuzz

Recommended DB schema additions (once):
  - PERSON_NAME_NORM (stored) + PERSON_NAME_KEY (stored) + index on KEY
  - optional FULLTEXT on PERSON_NAME_NORM
"""

import os
import re
import sys
import time
from typing import List, Dict, Tuple, Any, Optional

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import pymysql
from rapidfuzz import process, fuzz

# ----------------------------
# Config (tune as needed)
# ----------------------------
AUTO_SCORE = 90          # auto-correct threshold
MIN_MARGIN = 5           # score1 - score2 must be >= MIN_MARGIN to auto-correct
TOP_K = 10               # suggestions shown
PREFIX_LIMIT = 5000      # candidates fetched for prefix
FTX_LIMIT = 20000        # candidates fetched for fulltext fallback
LIKE_LIMIT = 20000       # last resort fallback
MIN_CANDIDATES_OK = 200  # if prefix yields >= this, skip fallbacks

TABLE = "T_WC_T2S_PERSON"
COL_ID_PERSON = "ID_PERSON"
COL_PERSON_NAME = "PERSON_NAME"
COL_PERSON_NAME_NORM = "PERSON_NAME_NORM"
COL_PERSON_NAME_KEY = "PERSON_NAME_KEY"

# Environment variables (recommended)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "")

TIMINGS = os.getenv("TIMINGS", "0").strip().lower() in {"1", "true", "yes", "on"}

# ----------------------------
# Normalization (should match your generated columns logic)
# ----------------------------
_rx_non_alnum_space = re.compile(r"[^0-9a-zA-ZÀ-ÿ ]+")
_rx_spaces = re.compile(r"\s+")

def normalize_name(s: str) -> str:
    """Normalize a person name for matching.

    Lowercases, strips, removes non-alphanumeric characters (keeping spaces),
    and collapses whitespace.

    Args:
        s: Raw input string.

    Returns:
        Normalized string.
    """
    s = (s or "").strip().lower()
    s = _rx_non_alnum_space.sub(" ", s)
    s = _rx_spaces.sub(" ", s).strip()
    return s

def to_key(s: str) -> str:
    """Build a compact key version of a name for prefix lookups.

    Args:
        s: Raw input string.

    Returns:
        Normalized name with spaces removed.
    """
    return normalize_name(s).replace(" ", "")

def build_boolean_query(tokens: List[str]) -> str:
    """Build a MariaDB FULLTEXT boolean query from normalized tokens.

    Tokens of length >= 4 get a trailing '*' for prefix matching.

    Args:
        tokens: List of normalized tokens.

    Returns:
        A boolean-mode query string suitable for `AGAINST (... IN BOOLEAN MODE)`.
    """
    # MariaDB boolean mode: +token* forces token presence, * is prefix
    parts = []
    for t in tokens:
        if len(t) >= 4:
            parts.append(f"+{t}*")
        else:
            parts.append(f"+{t}")
    return " ".join(parts)

# ----------------------------
# DB Helpers
# ----------------------------
def get_db_connection():
    """Create a PyMySQL connection using environment variables.

    Uses `DictCursor` so `fetchone()` / `fetchall()` return dictionaries.
    Expects MySQL/MariaDB parameter style `%s`.

    Environment variables:
        DB_HOST, DB_PORT, DB_USER, DB_PASS/DB_PASSWORD, DB_NAME

    Returns:
        A live `pymysql.Connection`.
    """
    strdbhost = os.getenv("DB_HOST", DB_HOST)
    lngdbport = int(os.getenv("DB_PORT", str(DB_PORT)))
    strdbuser = os.getenv("DB_USER", DB_USER)
    strdbpassword = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS", DB_PASS)
    strdbname = os.getenv("DB_NAME", DB_NAME)

    if not strdbname:
        print("ERROR: Set DB_NAME env var (and DB_HOST/DB_USER/DB_PASS as needed).", file=sys.stderr)
        sys.exit(1)

    return pymysql.connect(
        host=strdbhost,
        port=lngdbport,
        user=strdbuser,
        password=strdbpassword,
        database=strdbname,
        cursorclass=pymysql.cursors.DictCursor,
    )

def db_has_norm_columns(
    cur,
    strtablename: str,
    strcolumndescnorm: str,
    strcolumndesckey: str,
) -> bool:
    """Check if required generated columns exist on the target table.

    Args:
        cur: A DB cursor (DictCursor).

    Returns:
        True if both `PERSON_NAME_NORM` and `PERSON_NAME_KEY` exist.
    """
    # Check for PERSON_NAME_NORM and PERSON_NAME_KEY existence
    cur.execute("""
        SELECT COUNT(*) AS cnt
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME IN (%s, %s)
    """, (strtablename, strcolumndescnorm, strcolumndesckey))
    row = cur.fetchone()
    return row["cnt"] == 2

def db_has_fulltext(
    cur,
    strtablename: str,
    strcolumndescnorm: str,
) -> bool:
    """Check whether a FULLTEXT index exists on `PERSON_NAME_NORM`.

    Args:
        cur: A DB cursor (DictCursor).

    Returns:
        True if a FULLTEXT index is found, otherwise False.
    """
    # crude check: whether any FULLTEXT index exists on PERSON_NAME_NORM
    cur.execute(
        f"SHOW INDEX FROM `{strtablename}` WHERE Index_type='FULLTEXT' AND Column_name=%s",
        (strcolumndescnorm,),
    )
    return cur.fetchone() is not None

def exact_match(
    cur,
    strtablename: str,
    strcolumnid: str,
    strcolumndesc: str,
    strcolumndescnorm: str,
    q_norm: str,
) -> Optional[Dict[str, Any]]:
    """Find an exact normalized match in the database.

    Args:
        cur: A DB cursor (DictCursor).
        q_norm: Normalized query string.

    Returns:
        A row dict if found, else None.
    """
    # Exact match on normalized form (fast with index on PERSON_NAME_NORM)
    cur.execute(
        f"""
        SELECT `{strcolumnid}`, `{strcolumndesc}`, `{strcolumndescnorm}`
        FROM `{strtablename}`
        WHERE `{strcolumndescnorm}` = %s
        LIMIT 1
        """,
        (q_norm,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return row

def fetch_candidates(
    cur,
    strtablename: str,
    strcolumnid: str,
    strcolumndesc: str,
    strcolumndescnorm: str,
    strcolumndesckey: str,
    q_norm: str,
    q_key: str,
    has_fulltext: bool,
    timings: Optional[Dict[str, Any]] = None,
) -> List[Tuple[int, str, str]]:
    """Fetch candidate rows that may match the query.

    Strategy:
        1) Prefix lookup on `PERSON_NAME_KEY`.
        2) Optional FULLTEXT fallback on `PERSON_NAME_NORM`.
        3) LIKE fallback as last resort.

    Args:
        cur: A DB cursor (DictCursor).
        q_norm: Normalized query string.
        q_key: Key form of query (normalized without spaces).
        has_fulltext: Whether FULLTEXT is available on `PERSON_NAME_NORM`.
        timings: Optional dict to store timing measurements.

    Returns:
        A list of row dicts with at least `ID_PERSON`, `PERSON_NAME`, `PERSON_NAME_NORM`.
    """
    # 1) Prefix on PERSON_NAME_KEY (index-friendly)
    prefix_len = 6 if len(q_key) >= 6 else max(3, len(q_key))
    prefix = q_key[:prefix_len]

    t0 = time.perf_counter() if timings is not None else 0.0
    cur.execute(
        f"""
        SELECT `{strcolumnid}`, `{strcolumndesc}`, `{strcolumndescnorm}`
        FROM `{strtablename}`
        WHERE `{strcolumndesckey}` LIKE CONCAT(%s, '%%')
        LIMIT %s
        """,
        (prefix, PREFIX_LIMIT),
    )
    rows = cur.fetchall() or []
    if timings is not None:
        timings["prefix_s"] = time.perf_counter() - t0
        timings["prefix_n"] = len(rows)
    if len(rows) >= MIN_CANDIDATES_OK:
        if timings is not None:
            timings["used"] = "prefix"
        return rows

    # 2) FULLTEXT fallback (recommended)
    tokens = [t for t in q_norm.split() if t]
    tokens = sorted(tokens, key=len, reverse=True)[:3]  # longest tokens first
    if has_fulltext and tokens:
        t1 = time.perf_counter() if timings is not None else 0.0
        ftx_query = build_boolean_query(tokens)
        cur.execute(
            f"""
            SELECT `{strcolumnid}`, `{strcolumndesc}`, `{strcolumndescnorm}`
            FROM `{strtablename}`
            WHERE MATCH(`{strcolumndescnorm}`) AGAINST (%s IN BOOLEAN MODE)
            LIMIT %s
            """,
            (ftx_query, FTX_LIMIT),
        )
        rows2 = cur.fetchall() or []
        if timings is not None:
            timings["fulltext_s"] = time.perf_counter() - t1
            timings["fulltext_n"] = len(rows2)
        if rows2:
            seen = {r[strcolumnid] for r in rows}
            rows.extend([r for r in rows2 if r[strcolumnid] not in seen])
            if len(rows) >= MIN_CANDIDATES_OK:
                if timings is not None:
                    timings["used"] = "fulltext"
                return rows

    # 3) LIKE fallback (last resort)
    if tokens:
        t2 = time.perf_counter() if timings is not None else 0.0
        t = tokens[0]
        cur.execute(
            f"""
            SELECT `{strcolumnid}`, `{strcolumndesc}`, `{strcolumndescnorm}`
            FROM `{strtablename}`
            WHERE `{strcolumndescnorm}` LIKE CONCAT('%%', %s, '%%')
            LIMIT %s
            """,
            (t, LIKE_LIMIT),
        )
        rows3 = cur.fetchall() or []
        if timings is not None:
            timings["like_s"] = time.perf_counter() - t2
            timings["like_n"] = len(rows3)
        if rows3:
            seen = {r[strcolumnid] for r in rows}
            rows.extend([r for r in rows3 if r[strcolumnid] not in seen])

    if timings is not None and "used" not in timings:
        timings["used"] = "like" if tokens else "prefix"

    return rows

# ----------------------------
# RapidFuzz decision logic
# ----------------------------
def rank_candidates(
    strcolumnid: str,
    strcolumndesc: str,
    strcolumndescnorm: str,
    q_norm: str,
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Rank candidate rows by lexical similarity using RapidFuzz.

    Args:
        q_norm: Normalized query string.
        candidates: Candidate row dicts.

    Returns:
        A list of dicts containing the candidate fields plus a `SCORE` float.
    """
    # Dict choices: id -> norm for scoring
    choices = {row[strcolumnid]: row[strcolumndescnorm] for row in candidates}
    matches = process.extract(q_norm, choices, scorer=fuzz.WRatio, limit=TOP_K)

    id_to_row = {row[strcolumnid]: row for row in candidates}
    out = []
    for _match, score, pid in matches:
        r = id_to_row[pid]
        out.append({
            strcolumnid: r[strcolumnid],
            strcolumndesc: r[strcolumndesc],
            strcolumndescnorm: r[strcolumndescnorm],
            "SCORE": float(score),
        })
    return out

def decide_autocorrect(ranked: List[Dict[str, Any]]) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """Decide whether to auto-correct based on the top ranked candidates.

    Uses `AUTO_SCORE` and `MIN_MARGIN` as thresholds.

    Args:
        ranked: Ranked candidates from `rank_candidates()`.

    Returns:
        Tuple of (should_autocorrect, best_candidate_or_none, reason_string).
    """
    if not ranked:
        return (False, None, "no_candidates")

    top1 = ranked[0]
    top2 = ranked[1] if len(ranked) > 1 else None
    margin = (top1["SCORE"] - top2["SCORE"]) if top2 else 999.0

    if top1["SCORE"] >= AUTO_SCORE and margin >= MIN_MARGIN:
        return (True, top1, f"auto(score={top1['SCORE']:.1f}, margin={margin:.1f})")

    return (False, top1, f"suggest(score={top1['SCORE']:.1f}, margin={margin:.1f})")

# ----------------------------
# Main interactive loop
# ----------------------------
def main():
    """Run the interactive CLI loop."""
    conn = get_db_connection()
    cur = conn.cursor()

    strtablename = TABLE
    strcolumnid = COL_ID_PERSON
    strcolumndesc = COL_PERSON_NAME
    strcolumndescnorm = COL_PERSON_NAME_NORM
    strcolumndesckey = COL_PERSON_NAME_KEY

    if not db_has_norm_columns(cur, strtablename, strcolumndescnorm, strcolumndesckey):
        print(
            "ERROR: Columns PERSON_NAME_NORM and PERSON_NAME_KEY are missing.\n"
            "Add them as STORED generated columns + index, then rerun.",
            file=sys.stderr
        )
        sys.exit(2)

    has_fulltext = db_has_fulltext(cur, strtablename, strcolumndescnorm)

    print("Person name checker (RapidFuzz + MariaDB)")
    print(f"- DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"- Table: {strtablename}")
    print(f"- FULLTEXT on PERSON_NAME_NORM: {'yes' if has_fulltext else 'no'}")
    print("Type 'quit' to exit.\n")

    while True:
        raw = input("Enter a person name: ").strip()
        if not raw:
            continue
        if raw.lower() in ("quit", "exit", "q"):
            break

        # Start timing the search operation
        start_time = time.time()
        q_norm = normalize_name(raw)
        q_key = to_key(raw)

        t_exact0 = time.perf_counter() if TIMINGS else 0.0
        hit = exact_match(cur, strtablename, strcolumnid, strcolumndesc, strcolumndescnorm, q_norm)

        t_exact1 = time.perf_counter() if TIMINGS else 0.0
        if hit:
            print(f" Valid name: {hit[strcolumndesc]}  ({strcolumnid}={hit[strcolumnid]})\n")

            if TIMINGS:
                print(f"timings: exact_match={t_exact1 - t_exact0:.4f}s\n")
            continue

        fetch_t = {} if TIMINGS else None
        t_fetch0 = time.perf_counter() if TIMINGS else 0.0
        candidates = fetch_candidates(
            cur,
            strtablename,
            strcolumnid,
            strcolumndesc,
            strcolumndescnorm,
            strcolumndesckey,
            q_norm,
            q_key,
            has_fulltext,
            timings=fetch_t,
        )

        t_fetch1 = time.perf_counter() if TIMINGS else 0.0

        t_rank0 = time.perf_counter() if TIMINGS else 0.0
        ranked = rank_candidates(strcolumnid, strcolumndesc, strcolumndescnorm, q_norm, candidates)

        t_rank1 = time.perf_counter() if TIMINGS else 0.0

        auto, best, reason = decide_autocorrect(ranked)
        # End timing and calculate duration
        end_time = time.time()
        search_duration = end_time - start_time
        if not ranked:
            print(" No candidates found.\n")
            continue

        if auto and best:
            print(f"  Auto-corrected ({reason}):")
            print(f"    Input : {raw}")
            print(f"    Fixed : {best[strcolumndesc]}  ({strcolumnid}={best[strcolumnid]})\n")
        else:
            print(f"  Not confident to auto-correct ({reason}). Top suggestions:")
            for i, r in enumerate(ranked, 1):
                print(f"  {i:2d}. {r[strcolumndesc]}  [score={r['SCORE']:.1f}]  {strcolumnid}={r[strcolumnid]}")
            print("")

        print(f"Search duration: {search_duration:.4f} seconds\n")
        if TIMINGS:
            prefix_s = fetch_t.get("prefix_s", 0.0)
            fulltext_s = fetch_t.get("fulltext_s", 0.0)
            like_s = fetch_t.get("like_s", 0.0)
            print(
                "timings: "
                f"exact_match={t_exact1 - t_exact0:.4f}s "
                f"fetch_total={t_fetch1 - t_fetch0:.4f}s "
                f"rank={t_rank1 - t_rank0:.4f}s\n"
                f"  candidates={len(candidates)} used={fetch_t.get('used')}\n"
                f"  prefix={prefix_s:.4f}s n={fetch_t.get('prefix_n')}\n"
                f"  fulltext={fulltext_s:.4f}s n={fetch_t.get('fulltext_n')}\n"
                f"  like={like_s:.4f}s n={fetch_t.get('like_n')}\n"
            )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()