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

# Environment variables (recommended)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "")

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

def db_has_norm_columns(cur) -> bool:
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
          AND COLUMN_NAME IN ('PERSON_NAME_NORM', 'PERSON_NAME_KEY')
    """, (TABLE,))
    row = cur.fetchone()
    return row["cnt"] == 2

def db_has_fulltext(cur) -> bool:
    """Check whether a FULLTEXT index exists on `PERSON_NAME_NORM`.

    Args:
        cur: A DB cursor (DictCursor).

    Returns:
        True if a FULLTEXT index is found, otherwise False.
    """
    # crude check: whether any FULLTEXT index exists on PERSON_NAME_NORM
    cur.execute(f"""
        SHOW INDEX FROM {TABLE}
        WHERE Index_type='FULLTEXT' AND Column_name='PERSON_NAME_NORM'
    """)
    return cur.fetchone() is not None

def exact_match(cur, q_norm: str) -> Optional[Dict[str, Any]]:
    """Find an exact normalized match in the database.

    Args:
        cur: A DB cursor (DictCursor).
        q_norm: Normalized query string.

    Returns:
        A row dict if found, else None.
    """
    # Exact match on normalized form (fast with index on PERSON_NAME_NORM)
    cur.execute(f"""
        SELECT ID_PERSON, PERSON_NAME, PERSON_NAME_NORM
        FROM {TABLE}
        WHERE PERSON_NAME_NORM = %s
          AND (DELETED IS NULL OR DELETED = 0)
        LIMIT 1
    """, (q_norm,))
    row = cur.fetchone()
    if not row:
        return None
    return row

def fetch_candidates(cur, q_norm: str, q_key: str, has_fulltext: bool) -> List[Tuple[int, str, str]]:
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

    Returns:
        A list of row dicts with at least `ID_PERSON`, `PERSON_NAME`, `PERSON_NAME_NORM`.
    """
    # 1) Prefix on PERSON_NAME_KEY (index-friendly)
    prefix_len = 6 if len(q_key) >= 6 else max(3, len(q_key))
    prefix = q_key[:prefix_len]

    cur.execute(f"""
        SELECT ID_PERSON, PERSON_NAME, PERSON_NAME_NORM
        FROM {TABLE}
        WHERE PERSON_NAME_KEY LIKE CONCAT(%s, '%%')
          AND (DELETED IS NULL OR DELETED = 0)
        LIMIT %s
    """, (prefix, PREFIX_LIMIT))
    rows = cur.fetchall() or []
    if len(rows) >= MIN_CANDIDATES_OK:
        return rows

    # 2) FULLTEXT fallback (recommended)
    tokens = [t for t in q_norm.split() if t]
    tokens = sorted(tokens, key=len, reverse=True)[:3]  # longest tokens first
    if has_fulltext and tokens:
        ftx_query = build_boolean_query(tokens)
        cur.execute(f"""
            SELECT ID_PERSON, PERSON_NAME, PERSON_NAME_NORM
            FROM {TABLE}
            WHERE MATCH(PERSON_NAME_NORM) AGAINST (%s IN BOOLEAN MODE)
              AND (DELETED IS NULL OR DELETED = 0)
            LIMIT %s
        """, (ftx_query, FTX_LIMIT))
        rows2 = cur.fetchall() or []
        if rows2:
            seen = {r["ID_PERSON"] for r in rows}
            rows.extend([r for r in rows2 if r["ID_PERSON"] not in seen])
            if len(rows) >= MIN_CANDIDATES_OK:
                return rows

    # 3) LIKE fallback (last resort)
    if tokens:
        t = tokens[0]
        cur.execute(f"""
            SELECT ID_PERSON, PERSON_NAME, PERSON_NAME_NORM
            FROM {TABLE}
            WHERE PERSON_NAME_NORM LIKE CONCAT('%%', %s, '%%')
              AND (DELETED IS NULL OR DELETED = 0)
            LIMIT %s
        """, (t, LIKE_LIMIT))
        rows3 = cur.fetchall() or []
        if rows3:
            seen = {r["ID_PERSON"] for r in rows}
            rows.extend([r for r in rows3 if r["ID_PERSON"] not in seen])

    return rows

# ----------------------------
# RapidFuzz decision logic
# ----------------------------
def rank_candidates(q_norm: str, candidates: List[Tuple[int, str, str]]) -> List[Dict[str, Any]]:
    """Rank candidate rows by lexical similarity using RapidFuzz.

    Args:
        q_norm: Normalized query string.
        candidates: Candidate row dicts.

    Returns:
        A list of dicts containing the candidate fields plus a `SCORE` float.
    """
    # Dict choices: id -> norm for scoring
    choices = {row["ID_PERSON"]: row["PERSON_NAME_NORM"] for row in candidates}
    matches = process.extract(q_norm, choices, scorer=fuzz.WRatio, limit=TOP_K)

    id_to_row = {row["ID_PERSON"]: row for row in candidates}
    out = []
    for _match, score, pid in matches:
        r = id_to_row[pid]
        out.append({
            "ID_PERSON": r["ID_PERSON"],
            "PERSON_NAME": r["PERSON_NAME"],
            "PERSON_NAME_NORM": r["PERSON_NAME_NORM"],
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

    if not db_has_norm_columns(cur):
        print(
            "ERROR: Columns PERSON_NAME_NORM and PERSON_NAME_KEY are missing.\n"
            "Add them as STORED generated columns + index, then rerun.",
            file=sys.stderr
        )
        sys.exit(2)

    has_fulltext = db_has_fulltext(cur)

    print("Person name checker (RapidFuzz + MariaDB)")
    print(f"- DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"- Table: {TABLE}")
    print(f"- FULLTEXT on PERSON_NAME_NORM: {'yes' if has_fulltext else 'no'}")
    print("Type 'quit' to exit.\n")

    while True:
        raw = input("Enter a person name: ").strip()
        if not raw:
            continue
        if raw.lower() in ("quit", "exit", "q"):
            break

        q_norm = normalize_name(raw)
        q_key = to_key(raw)

        # 1) Exact match
        hit = exact_match(cur, q_norm)
        if hit:
            print(f"✅ Valid name: {hit['PERSON_NAME']}  (ID_PERSON={hit['ID_PERSON']})\n")
            continue

        # 2) Candidates + rank
        candidates = fetch_candidates(cur, q_norm, q_key, has_fulltext)
        ranked = rank_candidates(q_norm, candidates)

        auto, best, reason = decide_autocorrect(ranked)
        if not ranked:
            print("❌ No candidates found.\n")
            continue

        if auto and best:
            print(f"✏️  Auto-corrected ({reason}):")
            print(f"    Input : {raw}")
            print(f"    Fixed : {best['PERSON_NAME']}  (ID_PERSON={best['ID_PERSON']})\n")
        else:
            print(f"⚠️  Not confident to auto-correct ({reason}). Top suggestions:")
            for i, r in enumerate(ranked, 1):
                print(f"  {i:2d}. {r['PERSON_NAME']}  [score={r['SCORE']:.1f}]  ID={r['ID_PERSON']}")
            print("")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
    