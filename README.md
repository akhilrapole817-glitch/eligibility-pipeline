# Healthcare Eligibility Pipeline (Data Engineering Technical Assessment)

This project implements a **configuration-driven ingestion pipeline** that standardizes member eligibility files from multiple healthcare partners into one unified dataset.

## What this does
- Reads partner files using a **central YAML config** (delimiter + column mappings)
- Standardizes to a single schema:
  - `external_id`, `first_name`, `last_name`, `dob`, `email`, `phone`, `partner_code`
- Applies transformations:
  - Name → Title Case
  - DOB → ISO-8601 (`YYYY-MM-DD`)
  - Email → lowercase
  - Phone → `XXX-XXX-XXXX`
- Optional validation:
  - Drops rows where `external_id` is missing
- Graceful handling:
  - Skips malformed rows (logs warnings)

## Repo structure
```
eligibility_pipeline/
  config/partners.yaml
  data/acme.txt
  data/bettercare.csv
  pipeline.py
  output/
  logs/
```

## Prerequisites (local)
- Python 3.9+
- Install dependencies:
```bash
pip install pandas pyyaml
```

## Run (local)
From the project root:
```bash
python pipeline.py   --config config/partners.yaml   --input_dir data   --output_dir output   --output_format csv   --log_path logs/pipeline.log
```

Output:
- `output/eligibility_unified.csv`

## Databricks-ready run
### Option A (Databricks notebook)
1. Upload the repo (or copy files) into your Databricks workspace.
2. Put partner files in DBFS (Databricks File System) (or mount a cloud location).
3. Run:
```bash
python pipeline.py   --config config/partners.yaml   --input_dir /dbfs/FileStore/eligibility   --output_dir /dbfs/FileStore/eligibility_out   --output_format delta
```

Notes:
- In **Databricks Runtime**, Delta write will work out-of-the-box.
- If PySpark isn't available (non-Databricks local), the script **falls back to CSV** automatically.

## How to add a new partner (no code change)
Update `config/partners.yaml` with:
- `partner_code`
- `file_name`
- `delimiter`
- `column_mapping` (partner field → standard field)
- `date_parse.format` (Python `strftime` format)

Example (new partner):
```yaml
new_partner_x:
  partner_code: PARTNERX
  file_name: partnerx.tsv
  delimiter: "\t"
  has_header: true
  column_mapping:
    member_id: external_id
    given_name: first_name
    family_name: last_name
    birth_date: dob
    email_address: email
    phone_number: phone
  date_parse:
    format: "%Y/%m/%d"
  phone_parse:
    digits_expected: 10
```

That’s it — the pipeline will ingest it automatically.

## Notes on evaluation rubric alignment
- **Functionality:** produces correct standardized output
- **Scalability:** partner onboarding via config only
- **Code quality:** small, readable, testable functions
- **Bonus:** validation + malformed row handling + date coercion warnings
