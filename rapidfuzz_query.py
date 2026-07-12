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
from rapidfuzz.distance import Levenshtein

def levenshtein_distance(a: str, b: str) -> int:
    return int(Levenshtein.distance(a or "", b or ""))

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

# BK-tree (Levenshtein) adaptive distance thresholds.
# The BK-tree pool covers typos anywhere in the string (including inside
# the first few characters, which the prefix pool cannot reach).
BKTREE_LEN_SHORT = 6     # queries up to this length use BKTREE_K_SHORT
BKTREE_LEN_LONG = 14     # queries beyond this length use BKTREE_K_LONG
BKTREE_K_SHORT = 1
BKTREE_K_MEDIUM = 2
BKTREE_K_LONG = 3
BKTREE_FETCH_CAP = 500   # max ids batch-fetched from a single BK-tree query

# Environment variables (recommended)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "")

TIMINGS = os.getenv("TIMINGS", "0").strip().lower() in {"1", "true", "yes", "on"}
BKTREE_ENABLED = os.getenv("BKTREE_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}

# ----------------------------
# Normalization (should match your generated columns logic)
# ----------------------------
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
    s = (s or "").strip()
    if not s:
        return ""

    out_chars = []
    for ch in s:
        if ch.isalnum():
            out_chars.append(ch.lower())
        elif ch.isspace():
            out_chars.append(" ")
        else:
            out_chars.append(" ")

    s_norm = "".join(out_chars)
    s_norm = _rx_spaces.sub(" ", s_norm).strip()
    return s_norm

def to_key(s: str) -> str:
    """Build a compact key version of a name for prefix lookups.

    Args:
        s: Raw input string.

    Returns:
        Normalized name with spaces removed.
    """
    return normalize_name(s).replace(" ", "")

# ----------------------------
# Franchise-stopword neutralization (collections only)
# ----------------------------
# Generic franchise / collection words carry no identifying signal: a query like
# "Star Wars universe" must resolve to the "Star Wars Collection" row. Neutralizing
# these words on BOTH sides brings "star wars universe" ~ "star wars collection" to
# fuzz.ratio 100 (embeddings/WRatio otherwise mis-rank it under "Marvel Cinematic
# Universe" because "universe" matches "universe"). Applied ONLY to the collection
# target, never to person names.
#
# This tuple is the SOURCE OF TRUTH and MUST be kept in sync with the
# COLLECTION_NAME_NORM generated column (T2S_COLLECTION-rapidfuzz.sql) once the DB
# column is extended for production. Decided with Philippe (2026-07-12): Tier 1
# + series + n-logies; abbreviations (MCU, DCEU) and articles deliberately excluded.
import unicodedata as _unicodedata

_FRANCHISE_STOPWORD_WORDS = (
    # English
    "collection", "universe", "franchise", "saga",
    "trilogy", "duology", "tetralogy", "quadrilogy", "pentalogy", "hexalogy", "heptalogy",
    "film", "films", "movie", "movies", "series", "cinematic",
    # French (franchise / saga / collection / film / films are identical to EN)
    "univers", "trilogie", "duologie", "tétralogie", "quadrilogie",
    "pentalogie", "hexalogie", "heptalogie", "série", "séries", "cinématographique",
)


def _fold_ascii(s: str) -> str:
    """Lowercase + strip diacritics so accented FR words match whether or not the
    upstream normalizer preserved accents (é -> e, série -> serie)."""
    return "".join(
        c for c in _unicodedata.normalize("NFKD", s.lower()) if not _unicodedata.combining(c)
    )


_FRANCHISE_STOPWORDS = frozenset(_fold_ascii(w) for w in _FRANCHISE_STOPWORD_WORDS)


def strip_franchise_words(norm: str) -> str:
    """Remove generic franchise/collection words from an ALREADY-normalized string.

    Whole-word, accent-insensitive, idempotent (safe to apply to an already-stripped
    value). Guard: if neutralization would empty the string (e.g. a query that is
    literally "collection"), the input is returned unchanged.
    """
    if not norm:
        return norm
    kept = [tok for tok in norm.split() if _fold_ascii(tok) not in _FRANCHISE_STOPWORDS]
    stripped = " ".join(kept)
    return stripped if stripped else norm


def normalize_collection_name(s: str) -> str:
    """normalize_name() + franchise-stopword neutralization (collections only).

    Keep byte-for-byte in sync with the COLLECTION_NAME_NORM generated column.
    """
    return strip_franchise_words(normalize_name(s))

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
# BK-tree (Levenshtein) fuzzy index
# ----------------------------
class BKTreeIndex:
    """In-memory BK-tree keyed by Levenshtein distance.

    Complements the prefix / FULLTEXT / LIKE candidate pools by returning
    every indexed name within a small edit distance of the query — regardless
    of where the typo occurs. Designed to fix cases like RICARDO vs RICCARDO
    where the misspelling sits inside the first characters and therefore
    defeats a prefix lookup.

    Nodes are represented as lightweight 3-element lists
    `[norm_name, item_id, children_dict]` keyed on the Levenshtein distance
    from the parent. The triangle inequality is used to prune the search:
    at a node whose distance to the query is `d`, only children on edges in
    `[d - k, d + k]` can contain results within distance `k`.

    Concurrency: queries are safe to run from multiple threads once the tree
    is fully built. Insertions are NOT thread-safe — build first, query later.
    """

    __slots__ = ("_root", "_size")

    def __init__(self) -> None:
        self._root: Optional[List[Any]] = None
        self._size = 0

    @property
    def size(self) -> int:
        return self._size

    def insert(self, item_id: Any, norm_name: str) -> None:
        if not norm_name:
            return
        new_node: List[Any] = [norm_name, item_id, {}]
        if self._root is None:
            self._root = new_node
            self._size = 1
            return
        node = self._root
        while True:
            d = int(Levenshtein.distance(norm_name, node[0]))
            children = node[2]
            child = children.get(d)
            if child is None:
                children[d] = new_node
                self._size += 1
                return
            node = child

    def query(self, q_norm: str, max_distance: int) -> List[Tuple[Any, str, int]]:
        """Return `(id, norm_name, distance)` for every node within `max_distance`.

        Results are unordered; callers that need them sorted should sort by
        the third element.
        """
        out: List[Tuple[Any, str, int]] = []
        if self._root is None or not q_norm or max_distance < 0:
            return out
        k = max_distance
        stack: List[List[Any]] = [self._root]
        while stack:
            node = stack.pop()
            d = int(Levenshtein.distance(q_norm, node[0]))
            if d <= k:
                out.append((node[1], node[0], d))
            lo = d - k
            hi = d + k
            for edge, child in node[2].items():
                if lo <= edge <= hi:
                    stack.append(child)
        return out

    def build_from_cursor(
        self,
        cur,
        table: str,
        id_col: str,
        norm_col: str,
        batch_size: int = 50_000,
    ) -> None:
        """Populate the tree by streaming `(id, norm_name)` rows from the DB."""
        cur.execute(
            f"SELECT `{id_col}`, `{norm_col}` FROM `{table}`"
            f" WHERE `{norm_col}` IS NOT NULL AND `{norm_col}` <> ''"
        )
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                nm = row.get(norm_col)
                if not nm:
                    continue
                self.insert(row.get(id_col), nm)


def choose_bktree_k(q_norm: str) -> int:
    """Pick an adaptive max-distance for a BK-tree query based on query length."""
    n = len(q_norm)
    if n <= BKTREE_LEN_SHORT:
        return BKTREE_K_SHORT
    if n <= BKTREE_LEN_LONG:
        return BKTREE_K_MEDIUM
    return BKTREE_K_LONG


def build_bktree_for_config(cur, search_cfg: Dict[str, Any]) -> BKTreeIndex:
    """Build a BK-tree over the normalized name column described by `search_cfg`.

    `search_cfg` must contain `table`, `id`, and `norm` keys (same shape used
    elsewhere by `search_first_match_configured`).
    """
    idx = BKTreeIndex()
    idx.build_from_cursor(
        cur,
        search_cfg["table"],
        search_cfg["id"],
        search_cfg["norm"],
    )
    return idx


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
    strcolumnpopularity: str,
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
        SELECT `{strcolumnid}`, `{strcolumndesc}`, `{strcolumndescnorm}`, `{strcolumnpopularity}`
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
    strcolumnpopularity: str,
    q_norm: str,
    q_key: str,
    has_fulltext: bool,
    timings: Optional[Dict[str, Any]] = None,
    bktree: Optional[BKTreeIndex] = None,
) -> List[Tuple[int, str, str]]:
    """Fetch candidate rows that may match the query.

    Strategy:
        1) Prefix lookup on `PERSON_NAME_KEY`.
        2) BK-tree Levenshtein pool (optional, when `bktree` is provided).
           Always runs when available — even if the prefix already produced
           many rows — so typos inside the prefix cannot hide the correct row.
        3) Optional FULLTEXT fallback on `PERSON_NAME_NORM`.
        4) LIKE fallback as last resort.

    Args:
        cur: A DB cursor (DictCursor).
        q_norm: Normalized query string.
        q_key: Key form of query (normalized without spaces).
        has_fulltext: Whether FULLTEXT is available on `PERSON_NAME_NORM`.
        timings: Optional dict to store timing measurements.
        bktree: Optional pre-built `BKTreeIndex` over the normalized name column.

    Returns:
        A list of row dicts with at least `ID_PERSON`, `PERSON_NAME`, `PERSON_NAME_NORM`.
    """
    # 1) Prefix on PERSON_NAME_KEY (index-friendly)
    prefix_len = 6 if len(q_key) >= 6 else max(3, len(q_key))
    prefix = q_key[:prefix_len]

    t0 = time.perf_counter() if timings is not None else 0.0
    cur.execute(
        f"""
        SELECT `{strcolumnid}`, `{strcolumndesc}`, `{strcolumndescnorm}`, `{strcolumnpopularity}`
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

    # 2) BK-tree pool (always runs when provided; additive to the prefix pool)
    bk_added = 0
    if bktree is not None and bktree.size > 0 and q_norm:
        t_bk0 = time.perf_counter() if timings is not None else 0.0
        k = choose_bktree_k(q_norm)
        bk_matches = bktree.query(q_norm, k)
        if bk_matches:
            bk_matches.sort(key=lambda m: m[2])
            seen_ids = {r[strcolumnid] for r in rows}
            ids_to_fetch: List[Any] = []
            for mid, _mnm, _md in bk_matches:
                if mid in seen_ids:
                    continue
                ids_to_fetch.append(mid)
                if len(ids_to_fetch) >= BKTREE_FETCH_CAP:
                    break
            if ids_to_fetch:
                placeholders = ",".join(["%s"] * len(ids_to_fetch))
                cur.execute(
                    f"""
                    SELECT `{strcolumnid}`, `{strcolumndesc}`, `{strcolumndescnorm}`, `{strcolumnpopularity}`
                    FROM `{strtablename}`
                    WHERE `{strcolumnid}` IN ({placeholders})
                    """,
                    ids_to_fetch,
                )
                bk_rows = cur.fetchall() or []
                rows.extend(bk_rows)
                bk_added = len(bk_rows)
        if timings is not None:
            timings["bktree_s"] = time.perf_counter() - t_bk0
            timings["bktree_n"] = bk_added
            timings["bktree_k"] = k
            timings["bktree_raw"] = len(bk_matches)

    if len(rows) >= MIN_CANDIDATES_OK:
        if timings is not None:
            timings["used"] = "bktree+prefix" if bk_added > 0 else "prefix"
        return rows

    # 2) FULLTEXT fallback (recommended)
    tokens = [t for t in q_norm.split() if t]
    tokens = sorted(tokens, key=len, reverse=True)[:3]  # longest tokens first
    if has_fulltext and tokens:
        t1 = time.perf_counter() if timings is not None else 0.0
        ftx_query = build_boolean_query(tokens)
        cur.execute(
            f"""
            SELECT `{strcolumnid}`, `{strcolumndesc}`, `{strcolumndescnorm}`, `{strcolumnpopularity}`
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
                    timings["used"] = "bktree+fulltext" if bk_added > 0 else "fulltext"
                return rows

    # 3) LIKE fallback (last resort)
    if tokens:
        t2 = time.perf_counter() if timings is not None else 0.0
        t = tokens[0]
        cur.execute(
            f"""
            SELECT `{strcolumnid}`, `{strcolumndesc}`, `{strcolumndescnorm}`, `{strcolumnpopularity}`
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
        if tokens:
            timings["used"] = "bktree+like" if bk_added > 0 else "like"
        else:
            timings["used"] = "bktree+prefix" if bk_added > 0 else "prefix"

    return rows

# ----------------------------
# RapidFuzz decision logic
# ----------------------------
def rank_candidates(
    strcolumnid: str,
    strcolumndesc: str,
    strcolumndescnorm: str,
    strcolumnpopularity: str,
    q_norm: str,
    candidates: List[Dict[str, Any]],
    strip_stopwords: bool = False,
) -> List[Dict[str, Any]]:
    """Rank candidate rows by lexical similarity using RapidFuzz.

    Args:
        q_norm: Normalized query string.
        candidates: Candidate row dicts.
        strip_stopwords: When True, neutralize franchise/collection words in each
            candidate's normalized name before scoring (test-side mirror of the
            query neutralization; idempotent once the stored NORM column is stripped).

    Returns:
        A list of dicts containing the candidate fields plus a `SCORE` float.
    """
    # Dict choices: id -> norm for scoring
    if strip_stopwords:
        choices = {row[strcolumnid]: strip_franchise_words(row[strcolumndescnorm]) for row in candidates}
    else:
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
            strcolumnpopularity: r.get(strcolumnpopularity),
            "SCORE": float(score),
        })

    out.sort(
        key=lambda d: (
            -d["SCORE"],
            -(d.get(strcolumnpopularity) or 0),
        )
    )
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

def search_first_match(
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
    bktree: Optional[BKTreeIndex] = None,
    strip_stopwords: bool = False,
) -> Dict[str, Any]:
    """Search for a person name and return the best match.

    This is designed to be imported and reused by other modules (e.g. a FastAPI
    service). It does not print.

    Args:
        cur: A DB cursor (DictCursor).
        strtablename: Table name.
        strcolumnid: Primary key column name.
        strcolumndesc: Display column name (original name).
        strcolumndescnorm: Normalized name column name.
        strcolumndesckey: Key column name.
        strcolumnpopularity: Popularity column name.
        raw: Raw user input.
        has_fulltext: Whether FULLTEXT is available.
        timings_enabled: If True, return timing details.
        bktree: Optional pre-built `BKTreeIndex` over the normalized name
            column. When provided, extends the candidate pool with Levenshtein
            neighbours so typos in the first characters still resolve.

    Returns:
        A dict with:
            - hit: exact-match row or None
            - ranked: ranked suggestions (possibly empty)
            - auto: bool
            - best: best suggestion row or None
            - reason: reason string
            - timings: dict of timing breakdown (may be empty)
            - candidates_count: number of candidates fetched
    """
    q_norm = normalize_name(raw)
    if strip_stopwords:
        q_norm = strip_franchise_words(q_norm)
    if not q_norm:
        return {
            "hit": None,
            "ranked": [],
            "auto": False,
            "best": None,
            "reason": "empty_query",
            "timings": {},
            "candidates_count": 0,
        }

    # Derive the prefix key from the (possibly stopword-stripped) q_norm so retrieval
    # and scoring stay consistent. For the non-strip path this equals to_key(raw).
    q_key = q_norm.replace(" ", "")

    t_exact0 = time.perf_counter() if timings_enabled else 0.0
    hit = exact_match(
        cur,
        strtablename,
        strcolumnid,
        strcolumndesc,
        strcolumndescnorm,
        strcolumnpopularity,
        q_norm,
    )
    t_exact1 = time.perf_counter() if timings_enabled else 0.0
    if hit:
        return {
            "hit": hit,
            "ranked": [],
            "auto": True,
            "best": hit,
            "reason": "exact",
            "timings": {"exact_match": t_exact1 - t_exact0} if timings_enabled else {},
            "candidates_count": 0,
        }

    fetch_t = {} if timings_enabled else None
    t_fetch0 = time.perf_counter() if timings_enabled else 0.0
    candidates = fetch_candidates(
        cur,
        strtablename,
        strcolumnid,
        strcolumndesc,
        strcolumndescnorm,
        strcolumndesckey,
        strcolumnpopularity,
        q_norm,
        q_key,
        has_fulltext,
        timings=fetch_t,
        bktree=bktree,
    )
    t_fetch1 = time.perf_counter() if timings_enabled else 0.0

    t_rank0 = time.perf_counter() if timings_enabled else 0.0
    ranked = rank_candidates(
        strcolumnid,
        strcolumndesc,
        strcolumndescnorm,
        strcolumnpopularity,
        q_norm,
        candidates,
        strip_stopwords=strip_stopwords,
    )
    t_rank1 = time.perf_counter() if timings_enabled else 0.0

    auto, _best, reason = decide_autocorrect(ranked)
    best = ranked[0] if ranked else None

    timings: Dict[str, Any] = {}
    if timings_enabled:
        timings["exact_match"] = t_exact1 - t_exact0
        timings["fetch_total"] = t_fetch1 - t_fetch0
        timings["rank"] = t_rank1 - t_rank0
        timings["fetch_breakdown"] = fetch_t or {}

    return {
        "hit": None,
        "ranked": ranked,
        "auto": auto,
        "best": best,
        "reason": reason,
        "timings": timings,
        "candidates_count": len(candidates),
    }

def db_lookup_by_id(
    cur,
    strtablename: str,
    strkeycolumn: str,
    strkeyvalue: Any,
    select_columns: List[str],
) -> Optional[Dict[str, Any]]:
    """Lookup a single row by ID and return selected columns.

    Args:
        cur: A DB cursor (DictCursor).
        strtablename: Table name.
        strkeycolumn: Key column name.
        strkeyvalue: Key value.
        select_columns: Columns to select.

    Returns:
        Row dict or None.
    """
    cols_sql = ", ".join(f"`{c}`" for c in select_columns)
    cur.execute(
        f"SELECT {cols_sql} FROM `{strtablename}` WHERE `{strkeycolumn}` = %s LIMIT 1",
        (strkeyvalue,),
    )
    return cur.fetchone()

def build_match_object(
    *,
    source: str,
    table: str,
    id_col: str,
    text_col: str,
    norm_col: str,
    row: Dict[str, Any],
    score: Optional[float] = None,
    enriched: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a normalized match object for output."""
    out: Dict[str, Any] = {
        "source": source,
        "table": table,
        "id": row.get(id_col),
        "text": row.get(text_col),
        "norm": row.get(norm_col),
        "fields": row,
        "enriched": enriched or {},
    }
    if score is not None:
        out["score"] = float(score)
    return out

def enrich_match_object(cur, match: Dict[str, Any], enrich_config: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Enrich a match object using config-driven lookups."""
    if not enrich_config:
        return match

    enriched: Dict[str, Any] = dict(match.get("enriched") or {})
    fields = match.get("fields") or {}
    for step in enrich_config:
        from_key = step.get("from_key")
        attach_as = step.get("attach_as")
        lookup = step.get("lookup") or {}
        if not from_key or not attach_as:
            continue

        key_val = fields.get(from_key)
        if key_val is None:
            continue

        row = db_lookup_by_id(
            cur,
            lookup.get("table"),
            lookup.get("key_col"),
            key_val,
            lookup.get("select_cols") or [],
        )
        if row is not None:
            enriched[attach_as] = row

    match["enriched"] = enriched
    return match

def search_first_match_configured(
    *,
    cur,
    cmd: str,
    config: Dict[str, Any],
    raw: str,
    timings_enabled: bool = False,
) -> Dict[str, Any]:
    """Config-driven search returning structured match objects (with optional enrichment)."""
    search_cfg = config.get("search") or {}
    enrich_cfg = config.get("enrich") or []
    enrich_mode = config.get("enrich_mode", "best_only")

    state_has_fulltext = bool(search_cfg.get("has_fulltext"))
    state_bktree = search_cfg.get("bktree")
    base = search_first_match(
        cur,
        search_cfg["table"],
        search_cfg["id"],
        search_cfg["desc"],
        search_cfg["norm"],
        search_cfg["key"],
        search_cfg["pop"],
        raw,
        state_has_fulltext,
        timings_enabled=timings_enabled,
        bktree=state_bktree if isinstance(state_bktree, BKTreeIndex) else None,
        strip_stopwords=bool(search_cfg.get("strip_franchise_stopwords")),
    )

    def _wrap_row(row: Optional[Dict[str, Any]], score: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Wrap a raw database row in the normalized match-object structure."""
        if row is None:
            return None
        m = build_match_object(
            source=cmd,
            table=search_cfg["table"],
            id_col=search_cfg["id"],
            text_col=search_cfg["desc"],
            norm_col=search_cfg["norm"],
            row=row,
            score=score,
        )
        return m

    hit_match = _wrap_row(base.get("hit"))
    ranked_matches: List[Dict[str, Any]] = []
    for r in base.get("ranked") or []:
        ranked_matches.append(_wrap_row(r, score=r.get("SCORE")))

    best_match = _wrap_row(base.get("best"), score=(base.get("best") or {}).get("SCORE") if base.get("best") else None)

    if enrich_mode == "best_only":
        if hit_match is not None:
            hit_match = enrich_match_object(cur, hit_match, enrich_cfg)
        if best_match is not None:
            best_match = enrich_match_object(cur, best_match, enrich_cfg)
    elif enrich_mode == "all_ranked":
        if hit_match is not None:
            hit_match = enrich_match_object(cur, hit_match, enrich_cfg)
        if best_match is not None:
            best_match = enrich_match_object(cur, best_match, enrich_cfg)
        ranked_matches = [enrich_match_object(cur, m, enrich_cfg) for m in ranked_matches]

    return {
        "hit": hit_match,
        "ranked": ranked_matches,
        "auto": base.get("auto"),
        "best": best_match,
        "reason": base.get("reason"),
        "timings": base.get("timings"),
        "candidates_count": base.get("candidates_count"),
    }

# ----------------------------
# Main interactive loop
# ----------------------------
def main():
    """Run the interactive CLI loop."""
    conn = get_db_connection()
    cur = conn.cursor()

    configs = {
        "person": {
            "search": {
                "table": "T_WC_T2S_PERSON",
                "id": "ID_PERSON",
                "desc": "PERSON_NAME",
                "norm": "PERSON_NAME_NORM",
                "key": "PERSON_NAME_KEY",
                "pop": "POPULARITY",
            },
            "enrich": [],
            "enrich_mode": "best_only",
        },
        "aka": {
            "search": {
                "table": "T_WC_TMDB_PERSON_ALSO_KNOWN_AS",
                "id": "ID_ROW",
                "desc": "PERSON_NAME",
                "norm": "PERSON_NAME_NORM",
                "key": "PERSON_NAME_KEY",
                "pop": "ID_PERSON",
            },
            "enrich": [
                {
                    "from_key": "ID_PERSON",
                    "attach_as": "person",
                    "lookup": {
                        "table": "T_WC_T2S_PERSON",
                        "key_col": "ID_PERSON",
                        "select_cols": ["ID_PERSON", "PERSON_NAME"],
                    },
                }
            ],
            "enrich_mode": "best_only",
        },
        # Collection resolution (T2S_COLLECTION). Mirrors the `Collection_name`
        # rapidfuzz strategy in fastapi-text2sql/data/entity_resolution.json
        # (search_mode=rapidfuzz on COLLECTION_NAME_NORM/_KEY). Lets us reproduce
        # franchise/universe queries (e.g. "collection Star Wars universe") against
        # the exact production lexical path, not just the embeddings fallback that
        # embedding-query exercises. Generated columns: T2S_COLLECTION-rapidfuzz.sql.
        "collection": {
            "search": {
                "table": "T_WC_T2S_COLLECTION",
                "id": "ID_T2S_COLLECTION",
                "desc": "COLLECTION_NAME",
                "norm": "COLLECTION_NAME_NORM",
                "key": "COLLECTION_NAME_KEY",
                "pop": "POPULARITY",
                # Neutralize generic franchise words on both the query and (in-memory)
                # each candidate NORM, so "Star Wars universe" resolves to "Star Wars
                # Collection". Test-side today; production mirrors it in the stored
                # NORM column (step 3). See strip_franchise_words().
                "strip_franchise_stopwords": True,
            },
            "enrich": [],
            "enrich_mode": "best_only",
        },
    }

    table_state: Dict[str, Dict[str, Any]] = {}

    def ensure_table_ready(cmd: str) -> Dict[str, Any]:
        """Cache per-table search capabilities before running an interactive lookup."""
        if cmd in table_state:
            return table_state[cmd]

        search_cfg = configs[cmd]["search"]
        if not db_has_norm_columns(cur, search_cfg["table"], search_cfg["norm"], search_cfg["key"]):
            print(
                f"ERROR: Columns {search_cfg['norm']} and {search_cfg['key']} are missing on table {search_cfg['table']}.\n"
                "Add them as STORED generated columns + index, then rerun.",
                file=sys.stderr,
            )
            sys.exit(2)

        has_fulltext = db_has_fulltext(cur, search_cfg["table"], search_cfg["norm"])
        state: Dict[str, Any] = {"has_fulltext": has_fulltext}
        search_cfg["has_fulltext"] = has_fulltext

        if BKTREE_ENABLED:
            print(
                f"Building BK-tree for {search_cfg['table']}...",
                flush=True,
            )
            t0 = time.perf_counter()
            bktree = build_bktree_for_config(cur, search_cfg)
            t1 = time.perf_counter()
            print(f"  bktree built: {bktree.size} entries in {t1 - t0:.1f}s")
            state["bktree"] = bktree
            search_cfg["bktree"] = bktree

        table_state[cmd] = state
        return state

    current_cmd = "person"
    ensure_table_ready(current_cmd)

    print("Person name checker (RapidFuzz + MariaDB)")
    print(f"- DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("Commands:")
    print("  person <person_name>")
    print("  aka <person_name>")
    print("  collection <collection_name>")
    print("(If you omit the command, the previous command is reused.)")
    print("Type 'quit' to exit.\n")

    while True:
        raw = input("Enter your command (e.g. 'person <name>'): ").strip()
        if not raw:
            continue
        if raw.lower() in ("quit", "exit", "q"):
            break

        words = raw.split()
        if not words:
            continue

        cmd = words[0].lower()
        if cmd == "help":
            print("\nCommands:")
            print("  person <person_name>")
            print("  aka <person_name>")
            print("  collection <collection_name>")
            print("  help")
            print("  quit / exit / q\n")
            continue

        if cmd in configs:
            current_cmd = cmd
            query = " ".join(words[1:]).strip()
            if not query:
                print(f" Current search set to '{current_cmd}'.\n")
                continue
        else:
            query = raw
            cmd = current_cmd

        cfg = configs[cmd]
        ensure_table_ready(cmd)

        start_time = time.time()

        result = search_first_match_configured(
            cur=cur,
            cmd=cmd,
            config=cfg,
            raw=query,
            timings_enabled=TIMINGS,
        )

        end_time = time.time()
        search_duration = end_time - start_time

        if result["hit"] is not None:
            hit = result["hit"]
            hit_fields = hit.get("fields") or {}
            id_label = configs[cmd]["search"]["id"]
            text_label = configs[cmd]["search"]["desc"]
            hit_dist = levenshtein_distance(query, hit.get("text") or "")
            print(
                f" Valid name: {hit.get('text')}  "
                f"({id_label}={hit.get('id')})  "
                f"[Levenshtein={hit_dist}]"
            )
            if cmd == "aka":
                if "ID_PERSON" in hit_fields:
                    print(f" ID_PERSON={hit_fields.get('ID_PERSON')}")
                person = (hit.get("enriched") or {}).get("person")
                if person and person.get("PERSON_NAME") is not None:
                    print(f" PERSON_NAME (English)={person.get('PERSON_NAME')}")
            print("")
            print("")
            print(f"Search duration: {search_duration:.4f} seconds\n")
            if TIMINGS:
                print(f"timings: exact_match={result['timings'].get('exact_match', 0.0):.4f}s\n")
            continue

        if not result["ranked"]:
            print(" No candidates found.\n")
            continue

        if result["auto"] and result["best"]:
            print(f"  Auto-corrected ({result['reason']}):")
            print(f"    Input : {query}")
            id_label = configs[cmd]["search"]["id"]
            best_dist = levenshtein_distance(query, result["best"].get("text") or "")
            print(
                f"    Fixed : {result['best'].get('text')}  "
                f"({id_label}={result['best'].get('id')})  "
                f"[Levenshtein={best_dist}]"
            )
            if cmd == "aka":
                best_fields = result["best"].get("fields") or {}
                if "ID_PERSON" in best_fields:
                    print(f" ID_PERSON={best_fields.get('ID_PERSON')}")
                person = (result["best"].get("enriched") or {}).get("person")
                if person and person.get("PERSON_NAME") is not None:
                    print(f" PERSON_NAME (English)={person.get('PERSON_NAME')}")
            print("\n")
        else:
            print(f"  Not confident to auto-correct ({result['reason']}). Top suggestions:")
            if result["best"] is not None:
                best_fields = result["best"].get("fields") or {}
                pop_label = configs[cmd]["search"]["pop"]
                score = result["best"].get("score")
                best_dist = levenshtein_distance(query, result["best"].get("text") or "")
                print(
                    f"    Best : {result['best'].get('text')}  "
                    f"[score={score:.1f}]  "
                    f"[Levenshtein={best_dist}]  "
                    f"{pop_label}={best_fields.get(pop_label)}"
                )
            for i, r in enumerate(result["ranked"], 1):
                r_fields = r.get("fields") or {}
                pop_label = configs[cmd]["search"]["pop"]
                score = r.get("score")
                r_dist = levenshtein_distance(query, r.get("text") or "")
                print(
                    f"  {i:2d}. {r.get('text')}  "
                    f"[score={score:.1f}]  "
                    f"[Levenshtein={r_dist}]  "
                    f"{pop_label}={r_fields.get(pop_label)}"
                )
            print("")

        print(f"Search duration: {search_duration:.4f} seconds\n")
        if TIMINGS:
            fetch_t = result["timings"].get("fetch_breakdown", {})
            prefix_s = fetch_t.get("prefix_s", 0.0)
            bktree_s = fetch_t.get("bktree_s", 0.0)
            fulltext_s = fetch_t.get("fulltext_s", 0.0)
            like_s = fetch_t.get("like_s", 0.0)
            print(
                "timings: "
                f"exact_match={result['timings'].get('exact_match', 0.0):.4f}s "
                f"fetch_total={result['timings'].get('fetch_total', 0.0):.4f}s "
                f"rank={result['timings'].get('rank', 0.0):.4f}s\n"
                f"  candidates={result.get('candidates_count')} used={fetch_t.get('used')}\n"
                f"  prefix={prefix_s:.4f}s n={fetch_t.get('prefix_n')}\n"
                f"  bktree={bktree_s:.4f}s n={fetch_t.get('bktree_n')} "
                f"k={fetch_t.get('bktree_k')} raw={fetch_t.get('bktree_raw')}\n"
                f"  fulltext={fulltext_s:.4f}s n={fetch_t.get('fulltext_n')}\n"
                f"  like={like_s:.4f}s n={fetch_t.get('like_n')}\n"
            )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()