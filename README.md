# icu-mortality-prediction
Predict ICU mortality using MIMIC-IV data

## Data Extraction Pipeline

The project separates raw data extraction from downstream feature engineering:

- `sql/extract_vitals.sql`: extracts raw vital sign measurements from MIMIC-IV for the first 48 hours of the first ICU stay in adult patients
- `src/preprocess.py`: performs cleaning, unit harmonization, and missing-value handling
- `src/feature_engineering.py`: constructs model-ready features from the extracted time-series data
- `src/train.py`: trains and evaluates predictive models


## 🗄️ SQL Data Extraction

We provide SQL scripts to extract features directly from MIMIC-IV:

- `sql/extract_vitals.sql`: Extract vital signs
- `sql/cohort_selection.sql`: Define ICU cohort
- `sql/labels.sql`: Generate mortality labels

