# icu-mortality-prediction
Predict ICU mortality using MIMIC-IV data

## 🗄️ SQL Data Extraction

We provide SQL scripts to extract features directly from MIMIC-IV:

- `sql/extract_vitals.sql`: Extract vital signs
- `sql/cohort_selection.sql`: Define ICU cohort
- `sql/labels.sql`: Generate mortality labels
