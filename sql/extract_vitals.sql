-- =========================================
-- Project: ICU Mortality Prediction
-- File: extract_vitals.sql
-- Description:
--   Extract raw vital sign measurements from MIMIC-IV
--   for adult patients' first ICU stay within the first 48 hours.
--
-- Notes:
--   1. This script only performs cohort restriction and raw data extraction.
--   2. Downstream cleaning, unit harmonization, aggregation, and feature
--      engineering are handled in the Python pipeline.
-- =========================================

WITH cohort AS (
  WITH ranked AS (
    SELECT
      i.subject_id,
      i.hadm_id,
      i.stay_id,
      i.intime,
      i.outtime,
      i.los,
      p.anchor_age AS age,
      ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS rn
    FROM `physionet-data.mimiciv_3_1_icu.icustays` i
    LEFT JOIN `physionet-data.mimiciv_3_1_hosp.patients` p
      ON i.subject_id = p.subject_id
  )
  SELECT *
  FROM ranked
  WHERE rn = 1
    AND age >= 18
    AND los >= 2.0
),

vitals_raw AS (
  SELECT
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    c.intime,
    c.outtime,
    ce.charttime,
    DATETIME_DIFF(ce.charttime, c.intime, MINUTE) / 60.0 AS hours_from_intime,
    ce.itemid,
    LOWER(di.label) AS label,
    ce.valueuom,
    ce.valuenum AS value
  FROM cohort c
  JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce
    ON c.stay_id = ce.stay_id
  JOIN `physionet-data.mimiciv_3_1_icu.d_items` di
    ON ce.itemid = di.itemid
  WHERE ce.valuenum IS NOT NULL
    AND ce.charttime >= c.intime
    AND ce.charttime < DATETIME_ADD(c.intime, INTERVAL 48 HOUR)
    AND LOWER(di.label) IN (
      'heart rate',
      'respiratory rate',
      'respiratory rate (spontaneous)',
      'respiratory rate (total)',
      'o2 saturation pulseoxymetry',
      'non invasive blood pressure systolic',
      'non invasive blood pressure diastolic',
      'non invasive blood pressure mean',
      'arterial blood pressure systolic',
      'arterial blood pressure diastolic',
      'arterial blood pressure mean',
      'gcs - eye opening',
      'gcs - verbal response',
      'gcs - motor response',
      'temperature fahrenheit',
      'temperature celsius'
    )
)

SELECT
  subject_id,
  hadm_id,
  stay_id,
  intime,
  outtime,
  charttime,
  hours_from_intime,
  itemid,
  label,
  valueuom,
  value
FROM vitals_raw
ORDER BY stay_id, charttime;
