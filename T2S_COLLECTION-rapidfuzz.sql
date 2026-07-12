-- Generated-column support for RapidFuzz collection resolution (T2S_COLLECTION).
-- Mirrors T2S_PERSON-rapidfuzz.sql for the `collection` target of the CLI harness
-- and the `Collection_name` rapidfuzz strategy in fastapi-text2sql
-- (data/entity_resolution.json). These columns very likely already exist in
-- production (the API's rapidfuzz mode reads COLLECTION_NAME_NORM/_KEY); the
-- IF NOT EXISTS guards make this safe to run against an existing schema.
--
-- NOTE (pending FASTAPI-TEXT2SQL franchise-stopword fix): the NORM definition
-- below currently only strips punctuation. The agreed franchise-stopword
-- neutralization ("Star Wars universe" ~ "Star Wars Collection") will EXTEND the
-- inner REGEXP_REPLACE to also drop the generic franchise words, and MUST be kept
-- byte-for-byte in sync with normalize_collection_name() on the Python side so the
-- stored column and the query normalize identically.

ALTER TABLE T_WC_T2S_COLLECTION
  ADD COLUMN IF NOT EXISTS COLLECTION_NAME_NORM VARCHAR(255)
  AS (
    LOWER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(COLLECTION_NAME, '[^[:alnum:] ]', ' '), -- drop punctuation
        ' +', ' '                                              -- collapse spaces
      )
    )
  ) STORED;

CREATE INDEX IF NOT EXISTS IDX_T2S_COLLECTION_NAME_NORM ON T_WC_T2S_COLLECTION (COLLECTION_NAME_NORM);

ALTER TABLE T_WC_T2S_COLLECTION
  ADD COLUMN IF NOT EXISTS COLLECTION_NAME_KEY VARCHAR(255)
  AS (REPLACE(COLLECTION_NAME_NORM, ' ', '')) STORED;

CREATE INDEX IF NOT EXISTS IDX_T2S_COLLECTION_NAME_KEY ON T_WC_T2S_COLLECTION (COLLECTION_NAME_KEY);

ALTER TABLE T_WC_T2S_COLLECTION
  ADD FULLTEXT INDEX IF NOT EXISTS ft_collection_name_norm (COLLECTION_NAME_NORM);
