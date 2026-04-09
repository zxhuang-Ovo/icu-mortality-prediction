-- =========================================
-- Project: ICU Mortality Prediction
-- File: extract_labs_48h.sql
--
-- Description:
--   Extract selected laboratory measurements from MIMIC-IV
--   for the first 48 hours after ICU admission.
--
-- Cohort Definition:
--   - Adult patients (age >= 18)
--   - First ICU stay per patient
--   - Length of stay >= 48 hours
--
-- Included Lab Variables:
--   - Creatinine
--   - Lactate
--   - White Blood Cell Count (WBC)
--   - Hemoglobin
--   - Platelet Count
--   - Total Bilirubin
--   - Bicarbonate
--   - Sodium
--   - Potassium
--
-- Notes:
--   1. This script performs cohort restriction and raw lab extraction.
--   2. Lab labels are harmonized into a reduced feature set via CASE mapping.
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
    AND los >= 2.0
),

labs_raw AS (
  SELECT
    c.stay_id,
    c.hadm_id,
    c.subject_id,
    le.charttime,
    DATETIME_DIFF(le.charttime, c.intime, MINUTE) / 60.0 AS hours_from_intime,
    le.itemid,
    dl.label,
    le.valueuom,
    le.valuenum AS value,

    CASE
      WHEN LOWER(dl.label) IN (
        'creatinine',
        'creatinine, serum',
        'creatinine, blood',
        'creatinine, whole blood'
      ) THEN 'creatinine'

      WHEN LOWER(dl.label) = 'lactate' THEN 'lactate'

      WHEN LOWER(dl.label) IN (
        'white blood cells',
        'wbc',
        'wbc count'
      ) THEN 'wbc'

      WHEN LOWER(dl.label) = 'hemoglobin' THEN 'hemoglobin'
      WHEN LOWER(dl.label) = 'platelet count' THEN 'platelet'
      WHEN LOWER(dl.label) = 'bilirubin, total' THEN 'bilirubin_total'
      WHEN LOWER(dl.label) = 'bicarbonate' THEN 'bicarbonate'
      WHEN LOWER(dl.label) = 'sodium' THEN 'sodium'
      WHEN LOWER(dl.label) = 'potassium' THEN 'potassium'

      ELSE NULL
    END AS feature_name

  FROM cohort c
  JOIN `physionet-data.mimiciv_3_1_hosp.labevents` le
    ON c.hadm_id = le.hadm_id
  JOIN `physionet-data.mimiciv_3_1_hosp.d_labitems` dl
    ON le.itemid = dl.itemid

  WHERE le.valuenum IS NOT NULL
    AND le.charttime >= c.intime
    AND le.charttime < DATETIME_ADD(c.intime, INTERVAL 48 HOUR)
    AND LOWER(dl.label) IN (
      'creatinine',
      'creatinine, serum',
      'creatinine, blood',
      'creatinine, whole blood',
      'lactate',
      'white blood cells',
      'wbc',
      'wbc count',
      'hemoglobin',
      'platelet count',
      'bilirubin, total',
      'bicarbonate',
      'sodium',
      'potassium'
    )
)

SELECT
  stay_id,
  hadm_id,
  subject_id,
  charttime,
  hours_from_intime,
  itemid,
  label,
  valueuom,
  value,
  feature_name
FROM labs_raw
WHERE feature_name IS NOT NULL
ORDER BY stay_id, charttime;
