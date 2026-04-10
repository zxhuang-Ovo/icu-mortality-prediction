mkdir -p ~/Downloads/vitals
mkdir -p ~/Downloads/labs

gsutil -m cp "gs://icu-data-export-2026/vitals/*" ~/Downloads/vitals/
gsutil -m cp "gs://icu-data-export-2026/labs/*" ~/Downloads/labs/
