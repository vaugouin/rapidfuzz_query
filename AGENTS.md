# AGENTS.md - Agent Guide for Embedding Update

This file gives you the agentic context you need to work on this codebase safely. For project overview, features, install / deploy steps and human-facing security / performance / troubleshooting material, read @README.md — that file is canonical and not duplicated here.

This is the single canonical guide for autonomous coding agents in this repository. Assistant-specific files such as @CLAUDE.md, and any future tool-specific guide such as `GEMINI.md`, should only point here and should not duplicate repository instructions.

Deeper specs live in their own files:
- @doc/sql/*.sql — reference DDL for the database schema; treat these files as read-only unless the user explicitly asks you to edit schema documentation

- For any project update, keep documentation aligned:
  - Update `README.md` for user-facing behavior, configuration, setup, deployment, troubleshooting, or verification changes.
  - Update this file only when agent workflow or safety context changes.

---

## Where things live (file → role)

Edit at the right layer; the architecture is intentionally split.

## Code conventions

- **Hungarian notation** for variables (legacy style):
  - `str` — strings (`strtablename`, `strapiversion`)
  - `lng` — integers (`lngpage`, `lngrowsperpage`)
  - `dbl` — floats (`dblavailableram`)
  - `arr` — lists / arrays
  - `int` — boolean-like flags (`intcleanupenabled`, `intentity`)
- **Function naming**: public pipeline entry points use `f_` (`f_text2sql`, `f_entity_extraction`, `f_resolve_complex_question`, `f_answer_single_value`, `f_hello_world`); private helpers use `_` (`_call_chat_llm`, `_normalize_llm_model`).
- **Docstrings**: Google-style on public functions.
- **Error handling**: broad try/except with console logging; surface failures via the `error` response field and the `messages` trace. Database execution errors are not returned directly to clients — they go through the complex-question retry path when enabled.
- **JSON serialization**: use `logs.decimal_serializer()` for `Decimal` and `datetime`.

---

## Database Schema Sources

Full DDL lives under [doc/sql/](doc/sql/); do not duplicate table definitions here. Treat these files as reference-only unless the user explicitly asks for schema-doc edits.

- [doc/sql/T2S-tables.sql](doc/sql/T2S-tables.sql) — canonical Text2SQL read-model tables.
- [doc/sql/TMDb-tables.sql](doc/sql/TMDb-tables.sql) — source TMDb tables and reference tables.

---

## SQL Object Naming Conventions

- SQL table and column names are uppercase snake case, except legacy imported TMDb genre columns such as `id` and `name`.
- Persistent tables use `T_WC_*`.
- Text2SQL read-model tables use `T_WC_T2S_*`.
- TMDb source/reference tables use `T_WC_TMDB_*`.
- Wikidata tables use `T_WC_WIKIDATA_*`; staging tables use `STG_T_WC_WIKIDATA_*`.
- Wikipedia tables use `T_WC_WIKIPEDIA_*`.
- Join tables usually follow `T_WC_T2S_{PARENT}_{CHILD}`, for example `T_WC_T2S_MOVIE_GENRE`, `T_WC_T2S_PERSON_MOVIE`.
- Primary keys are usually `ID_{ENTITY}` for entity tables, `ID_ROW` for generic/join rows, or a table-specific surrogate such as `ID_T2S_PERSON_MOVIE`.
- Foreign keys reuse the referenced primary-key name, for example `ID_MOVIE`, `ID_PERSON`, `ID_GENRE`.
- Date columns use `DAT_*`; datetime/timestamp columns use `TIM_*`.
- Boolean-like flags use `IS_*` or legacy integer flags such as `DELETED`.
- Ordering uses `DISPLAY_ORDER`.
- Aggregate counters use `*_COUNT`.
- Media paths use `*_PATH`.
- Language-specific labels/titles often use suffixes such as `_FR`; generic language rows use `LANG`.
- RapidFuzz/generated search columns use `*_NORM` and `*_KEY`; popularity tie-breakers commonly use `POPULARITY`.
- Index names are mixed legacy style. Preserve existing style: simple `KEY COLUMN_NAME`, `IDX_*` for indexes, `UK_*` for unique keys, `FK_*` for foreign keys, and `ft_*` for FULLTEXT indexes.

---

## Encoding

Keep Markdown, prompt files, JSON config, and logs UTF-8. These files contain non-ASCII names and multilingual examples. Avoid editor or terminal operations that rewrite them with mojibake.

---

**Last Updated**: 2026-05-18
**Current Version**: 1.0.0 
