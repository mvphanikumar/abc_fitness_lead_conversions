ABC Fitness Lead Conversion Pipeline
=====================================

This project implements a modular data pipeline to process and analyze lead conversion events for ABC Fitness.
It reads raw membership and credit pack purchase data from CSVs, determines the earliest client conversion events,
and outputs enriched conversion event datasets.

---

## Folder Structure

```
├── data/
│   ├── dim_branch.csv
│   ├── dim_user.csv
│   ├── fct_credit_pack_purchases.csv
│   ├── fct_membership_purchases.csv
│   └── fct_client_conversion_events_part_2.csv
├── outputs/
│   ├── fct_client_conversion_events.csv
│   └── fct_lead_conversions.csv
├── main.py
├── data_processing.log
├── requirements.txt
└── README.md
```

---

## Features

- Parses datetime fields and JSON-encoded columns
- Processes credit pack and membership purchases to identify earliest conversion
- Generates `fct_client_conversion_events` and `fct_lead_conversions`
- Supports fallback logic for part 2 pre-generated file
- Outputs datetimes in ISO 8601 format with milliseconds and timezone (`2023-04-07T08:20:57.730+00:00`)

---

## Usage

### 1. Install Requirements (Optional)
This script only uses Python standard libraries (no external dependencies).

### 2. Prepare Input Data
Place your source CSVs in the `data/` directory with the following filenames:

- `dim_user.csv`
- `dim_branch.csv`
- `fct_credit_pack_purchases.csv`
- `fct_membership_purchases.csv`
- `fct_client_conversion_events_part_2.csv`

### 3. Run Full Pipeline
```bash
python main.py
```

### 4. Run Only Fallback (Part 2)
```bash
python mian.py --part2_only
```

---

## Outputs

### `fct_client_conversion_events.csv`
Contains enriched client conversion data combining credit and membership information.

### `fct_lead_conversions.csv`
Includes all combinations of conversion types: `MEMBERSHIP`, `USER_CREDIT`, and `ALL`.

---


## Author
Phanikumar V Merugumala

---
