-- =========================================
-- Project: ICU Mortality Prediction
-- File: extract_cohort.sql
--
-- Description:
--   Define the ICU cohort of adult patients from MIMIC-IV.
--   Select the first ICU stay per patient with demographic and admission information.
--
-- Cohort Definition:
--   - Adult patients (age >= 18)
--   - First ICU stay per patient
--   - Includes ICU stay info, demographics, and hospital admission data
--
-- Included Variables:
--   - stay_id, hadm_id, subject_id
--   - First care unit (first_careunit)
--   - ICU admission and discharge time (intime, outtime)
--   - Length of stay (los)
--   - Patient gender
--   - Age at admission
--   - Admission type
--   - In-hospital mortality flag (mortality)
--
-- Notes:
--   1. This script selects only the first ICU stay for each patient using ROW_NUMBER().
--   2. Downstream feature engineering and modeling are handled in Python.
--
-- Data Source:
--   MIMIC-IV (v3.1)
--   Requires credentialed access via PhysioNet.
-- =========================================

WITH ranked AS (
  SELECT
    i.subject_id,
    i.hadm_id,
    i.stay_id,
    i.first_careunit,
    i.intime,
    i.outtime,
    i.los,
    p.gender,
    p.anchor_age AS age,
    a.admission_type,
    a.hospital_expire_flag AS mortality,
    ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS rn
  FROM `physionet-data.mimiciv_3_1_icu.icustays` i
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.patients` p
    ON i.subject_id = p.subject_id
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` a
    ON i.hadm_id = a.hadm_id
)
SELECT
  subject_id,
  hadm_id,
  stay_id,
  first_careunit,
  intime,
  outtime,
  los,
  gender,
  age,
  admission_type,
  mortality
FROM ranked
WHERE rn = 1
  AND age >= 18
ORDER BY stay_id;
