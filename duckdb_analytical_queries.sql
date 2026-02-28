-- ============================================================
-- duckdb_analytical_queries.sql
--
-- Purpose:
-- Analytical SQL layer for wildfire–air pollution analysis.
-- This script:
-- 1. Loads Spark-transformed Parquet data into DuckDB
-- 2. Deduplicates pollution observations by worst-case AQI
-- 3. Engineers pre/post wildfire pollution features
-- 4. Computes correlation metrics
-- 5. Exports analysis-ready CSVs for Tableau
--
-- Design principles:
-- - Deterministic transformations
-- - Readable, interpretable feature logic
-- - BI-ready outputs
-- ============================================================


-- ============================================================
-- 1. Base Table Definition
-- ============================================================

CREATE TABLE IF NOT EXISTS wildfire_pollution_raw (
    Fire_ID INTEGER,
    OBJECTID INTEGER,
    SOURCE_REPORTING_UNIT_NAME VARCHAR,
    FIRE_NAME VARCHAR,
    DISCOVERY_DATE DATE,
    DISCOVERY_DOY INTEGER,
    FIRE_YEAR INTEGER,
    STAT_CAUSE_CODE VARCHAR,
    STAT_CAUSE_DESCR VARCHAR,
    CONT_DOY INTEGER,
    CONT_DATE DATE,
    FIRE_SIZE REAL,
    FIRE_SIZE_CLASS VARCHAR,
    LATITUDE REAL,
    LONGITUDE REAL,
    Fire_State VARCHAR,
    Fire_County VARCHAR,
    FIPS_NAME VARCHAR,
    start_date DATE,
    _c0 VARCHAR,
    "Date Local" DATE,
    "CO AQI" REAL,
    "CO Mean" REAL,
    "SO2 AQI" REAL,
    "SO2 Mean" REAL,
    "O3 AQI" REAL,
    "O3 Mean" REAL,
    "NO2 AQI" REAL,
    "NO2 Mean" REAL,
    Final_Latitude REAL,
    Final_Longitude REAL,
    State VARCHAR,
    County VARCHAR,
    Date_Local DATE,
    distance_miles REAL,
    date_diff_fire_pollution INTEGER,
    FIRE_MONTH INTEGER
);

-- Load Spark output (Parquet) into DuckDB
COPY wildfire_pollution_raw
FROM '$LOADPATH'
(FORMAT PARQUET);


-- ============================================================
-- 2. Deduplication: Worst-Case Daily AQI
-- ============================================================
-- Rationale:
-- Multiple pollution readings may exist per fire/date.
-- We retain the observation with the highest NO2 AQI
-- to represent worst-case exposure.

CREATE TEMP TABLE ranked_observations AS
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY start_date, Date_Local
        ORDER BY "NO2 AQI" DESC
    ) AS rn
FROM wildfire_pollution_raw;

CREATE TEMP TABLE daily_max_aqi AS
SELECT *
FROM ranked_observations
WHERE rn = 1;


-- ============================================================
-- 3. Distance-Based Pollution Analysis
-- ============================================================
-- Aggregates AQI metrics by distance bands from wildfire.

SELECT
    CASE
        WHEN distance_miles < 10 THEN 'Under 10 miles'
        WHEN distance_miles BETWEEN 10 AND 50 THEN '10–50 miles'
        ELSE 'Over 50 miles'
    END AS distance_category,
    AVG("CO AQI")  AS avg_co_aqi,
    AVG("NO2 AQI") AS avg_no2_aqi,
    AVG("O3 AQI")  AS avg_o3_aqi,
    AVG("SO2 AQI") AS avg_so2_aqi
FROM wildfire_pollution_raw
GROUP BY distance_category
ORDER BY distance_category;


-- ============================================================
-- 4. Feature Engineering: Pre/Post Wildfire Windows
-- ============================================================
-- Window:
-- Pre-fire  = days -7 to -1
-- Post-fire = days +1 to +7
--
-- Features are computed per wildfire event (start_date).

CREATE TEMP TABLE wildfire_event_features AS
SELECT
    Fire_ID,
    OBJECTID,
    SOURCE_REPORTING_UNIT_NAME,
    FIRE_NAME,
    DISCOVERY_DATE,
    DISCOVERY_DOY,
    FIRE_YEAR,
    STAT_CAUSE_CODE,
    STAT_CAUSE_DESCR,
    CONT_DOY,
    CONT_DATE,
    FIRE_SIZE,
    FIRE_SIZE_CLASS,
    LATITUDE,
    LONGITUDE,
    Fire_State,
    Fire_County,
    FIPS_NAME,
    start_date,
    _c0,
    "Date Local",
    Final_Latitude,
    Final_Longitude,
    State,
    County,
    Date_Local,
    distance_miles,
    date_diff_fire_pollution,
    FIRE_MONTH,

    -- Pre-fire averages
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN -7 AND -1
             THEN "SO2 AQI" END) OVER (PARTITION BY start_date) AS avg_so2_pre,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN -7 AND -1
             THEN "NO2 AQI" END) OVER (PARTITION BY start_date) AS avg_no2_pre,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN -7 AND -1
             THEN "CO AQI" END)  OVER (PARTITION BY start_date) AS avg_co_pre,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN -7 AND -1
             THEN "O3 AQI" END)  OVER (PARTITION BY start_date) AS avg_o3_pre,

    -- Post-fire averages
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN 1 AND 7
             THEN "SO2 AQI" END) OVER (PARTITION BY start_date) AS avg_so2_post,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN 1 AND 7
             THEN "NO2 AQI" END) OVER (PARTITION BY start_date) AS avg_no2_post,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN 1 AND 7
             THEN "CO AQI" END)  OVER (PARTITION BY start_date) AS avg_co_post,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN 1 AND 7
             THEN "O3 AQI" END)  OVER (PARTITION BY start_date) AS avg_o3_post

FROM ranked_observations;


-- ============================================================
-- 5. Export Analysis-Ready Tables
-- ============================================================

COPY wildfire_event_features
TO 'aggregated_data.csv'
(FORMAT CSV, HEADER TRUE);

COPY daily_max_aqi
TO 'all_data.csv'
(FORMAT CSV, HEADER TRUE);


-- ============================================================
-- 6. Correlation Analysis
-- ============================================================
-- Examines relationship between wildfire size and:
-- 1. Post-fire pollution levels
-- 2. Pre vs post pollution deltas

CREATE TEMP TABLE fire_size_correlations AS
SELECT
    corr(FIRE_SIZE, avg_co_post)                     AS corr_fire_size_co,
    corr(FIRE_SIZE, avg_no2_post)                    AS corr_fire_size_no2,
    corr(FIRE_SIZE, avg_o3_post)                     AS corr_fire_size_o3,
    corr(FIRE_SIZE, avg_so2_post)                    AS corr_fire_size_so2,
    corr(FIRE_SIZE, (avg_co_post  - avg_co_pre))     AS corr_fire_size_co_diff,
    corr(FIRE_SIZE, (avg_no2_post - avg_no2_pre))    AS corr_fire_size_no2_diff,
    corr(FIRE_SIZE, (avg_o3_post  - avg_o3_pre))     AS corr_fire_size_o3_diff,
    corr(FIRE_SIZE, (avg_so2_post - avg_so2_pre))    AS corr_fire_size_so2_diff
FROM wildfire_event_features;

COPY fire_size_correlations
TO 'corr.csv'
(FORMAT CSV, HEADER TRUE);
