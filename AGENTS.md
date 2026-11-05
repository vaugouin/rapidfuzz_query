# AGENTS.md - Agent Guide for RapidFuzz Query

## Canary

Start every reply with "Philippe,": no exceptions, including short replies and tool summaries.

Canonical agent guide for this repo (`CLAUDE.md` and any other tool-specific file only point here). Overview, public API, configuration, required columns, test CLI and Docker are in @README.md; algorithm detail in `RAPIDFUZZ.md`; generated-column DDL in the `*-rapidfuzz.sql` files at the root (reference only unless asked). Update `README.md` for user-facing changes, this file only when agent workflow or safety context changes.

---

## Related repositories (project ecosystem)

Name-resolution stage of **Agent BBB** (`github.com/vaugouin/<repo>`, cloned under `%USERPROFILE%/Code/<repo>`): the module `fastapi-text2sql` imports in-process to resolve approximate names (indexed candidate retrieval plus RapidFuzz WRatio ranking), with a test CLI. Not deployed as a container of its own. Roster: `%USERPROFILE%/Nestor/projets/t2s-backlog/topics/related-repositories.txt`; stage map: `tmdb-front/AGENTS.md` § Related repositories.

---

## Where things live (file → role)

**`rapidfuzz_query.py` is a copy; never edit it here (RAPIDFUZZ-QUERY-006, 2026-10-05).** The source of truth is `fastapi-text2sql/rapidfuzz_query.py`, the module the API imports. Make every change there, copy it as is (`cp ../fastapi-text2sql/rapidfuzz_query.py .`) and check that `cmp` between the two prints nothing: this CLI must resolve names exactly as the API does. Code conventions are therefore those of `fastapi-text2sql`.

## Conventions

- SQL names follow the `T_WC_*` conventions (uppercase snake case, `T_WC_T2S_*` read-model, generated search columns `*_NORM` / `*_KEY`, `POPULARITY` tie-breaker); preserve the existing legacy style of index names.
- Keep Markdown, prompts, JSON and logs UTF-8 (non-ASCII names); avoid tools that rewrite them with mojibake.

---

**Last Updated**: 2026-11-05
**Current Version**: 1.0.5

## Backlog (Nestor second-brain)

Backlog (`RAPIDFUZZ-QUERY-NNN`, status and priority) in the Nestor repo: `C:\Users\vaugo\Nestor\projets\t2s-backlog\repos\rapidfuzz-query.md` (dashboard: `...\t2s-backlog\index.md`). Consult it before implementing. Local paths on Philippe's PC only; they do not resolve on the VPS or cloud agents.
