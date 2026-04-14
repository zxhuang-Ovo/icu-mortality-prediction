-- =========================================
-- Project: ICU Mortality Prediction
-- File: extract_vitals.sql
--
-- Description:
--   Extract raw vital sign measurements from MIMIC-IV
--   within the first 48 hours after ICU admission (or until ICU discharge if earlier).
--
-- Cohort Definition:
--   - Adult patients (age >= 18)
--   - First ICU stay per patient
--
-- Included Vital Signs:
--   - Heart Rate
--   - Respiratory Rate
--   - Oxygen Saturation (SpO₂)
--   - Blood Pressure (arterial and non-invasive)
--   - Glasgow Coma Scale (GCS)
--   - Temperature
--
-- Notes:
--   1. This script performs cohort restriction and raw vital extraction.
--   2. Multiple label variants are retained (e.g., arterial vs non-invasive BP).
--   3. No aggregation or imputation is performed at this stage.
--   4. Downstream preprocessing and feature engineering are handled in Python.
--
-- Data Source:
--   MIMIC-IV (v3.1)
--   Requires credentialed access via PhysioNet.
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
),

vitals_raw AS (
  SELECT
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    c.intime,
    c.outtime,
    c.los,

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
  los,
  charttime,
  hours_from_intime,
  itemid,
  label,
  valueuom,
  value
FROM vitals_raw
ORDER BY stay_id, charttime;
