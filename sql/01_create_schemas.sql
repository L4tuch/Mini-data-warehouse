-- Schemas for the warehouse:
-- Creates: raw (landing), stg (cleaned), dw (star schema), meta (future control tables).
-- Run once before any other SQL files.


CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;