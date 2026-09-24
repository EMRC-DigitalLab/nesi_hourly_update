-- 001: remove duplicate GENCO rows and add a unique key on (Date, Hour, Gencos).
--
-- Run as a MySQL admin, with no scraper running:
--   sudo mysql --defaults-file=/etc/mysql/debian.cnf jksutauf_nesidb < migrations/001_genco_dedupe_unique_key.sql
--
-- How it works:
--   1. Build an empty copy of the table that already has the unique key.
--   2. Copy every row across. When a (Date, Hour, Gencos) repeats, the later
--      row overwrites the earlier one, so the most recently scraped value wins.
--      (The table has no primary key, so InnoDB stores rows in insertion order
--      and a full scan returns them oldest-first.)
--   3. Swap the tables in one atomic RENAME. The original is kept untouched as
--      combined_hourly_energy_generated_mwh_bak_001 for rollback.
--
-- Rollback (only if something is wrong):
--   RENAME TABLE combined_hourly_energy_generated_mwh TO combined_hourly_energy_generated_mwh_failed_001,
--                combined_hourly_energy_generated_mwh_bak_001 TO combined_hourly_energy_generated_mwh;

CREATE TABLE combined_hourly_energy_generated_mwh_new LIKE combined_hourly_energy_generated_mwh;

ALTER TABLE combined_hourly_energy_generated_mwh_new
  ADD UNIQUE KEY uq_genco_date_hour_genco (Date, Hour, Gencos);

INSERT INTO combined_hourly_energy_generated_mwh_new (Date, Hour, Gencos, EnergyGeneratedMWh)
SELECT Date, Hour, Gencos, EnergyGeneratedMWh
FROM combined_hourly_energy_generated_mwh
ON DUPLICATE KEY UPDATE EnergyGeneratedMWh = VALUES(EnergyGeneratedMWh);

RENAME TABLE
  combined_hourly_energy_generated_mwh     TO combined_hourly_energy_generated_mwh_bak_001,
  combined_hourly_energy_generated_mwh_new TO combined_hourly_energy_generated_mwh;

SELECT
  (SELECT COUNT(*) FROM combined_hourly_energy_generated_mwh_bak_001) AS rows_before,
  (SELECT COUNT(*) FROM combined_hourly_energy_generated_mwh)         AS rows_after;
