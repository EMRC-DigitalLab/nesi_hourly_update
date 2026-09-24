-- 002: add a unique key on (hour, Company) to discoloadprofile.
--
-- Run as a MySQL admin:
--   sudo mysql --defaults-file=/etc/mysql/debian.cnf jksutauf_nesidb < migrations/002_disco_unique_key.sql
--
-- `Date` holds the exact scrape time (with minutes/seconds), but there should
-- be one row per company per hour. HourStart is that time truncated to the
-- hour, computed by MySQL. It is INVISIBLE, so `SELECT *` and anything else
-- reading this table sees exactly the same three columns as before.
-- Checked on 2026-09-24: no existing duplicates.
--
-- Rollback:
--   ALTER TABLE discoloadprofile DROP KEY uq_disco_hour_company, DROP COLUMN HourStart;

ALTER TABLE discoloadprofile
  ADD COLUMN HourStart DATETIME
    AS (CAST(DATE_FORMAT(`Date`, '%Y-%m-%d %H:00:00') AS DATETIME)) STORED INVISIBLE,
  ADD UNIQUE KEY uq_disco_hour_company (HourStart, Company);
