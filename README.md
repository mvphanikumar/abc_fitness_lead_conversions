# Client and Lead Conversion Tracking

## Overview
This tool processes user data to track lead conversions to clients based on credit pack purchases and membership signups. It creates two primary output tables:
- `fct_client_conversion_events.csv`: Tracks each user's first client conversion event
- `fct_lead_conversions.csv`: Provides filtered views of conversion events by type

## Features
- Parses and normalizes datetime fields in various formats to ISO 8601
- Tracks the earliest credit pack purchase and membership purchase for each user
- Determines lead status (LEAD or CLIENT) based on purchase history
- Creates filtered views for different conversion event types
- Handles JSON-formatted purchase details

## Prerequisites
- Python 3.x
- CSV data files in the expected format

## Data Requirements
The script expects four CSV files in the `data/` directory:
- `dim_branch.csv`: Branch dimension data
- `dim_user.csv`: User dimension data
- `fct_credit_pack_purchases.csv`: Credit pack purchase data
- `fct_membership_purchases.csv`: Membership purchase data

## Installation
1. Clone this repository
2. Ensure your data files are in the `data/` directory
3. Create an `outputs/` directory for results

```bash
mkdir -p data outputs
# Place your CSV files in the data/ directory
```

## Usage
Run the main script:

```bash
python main.py
```

The script will:
1. Load and normalize the data from the input files
2. Process credit pack and membership purchases
3. Create client conversion events
4. Generate filtered lead conversion records
5. Save results to the `outputs/` directory

## Output Files

### fct_client_conversion_events.csv
Contains one record per user with their lead status and first conversion event details. Includes information about user identification, branch association, timestamps, lead status, and details about their first credit pack or membership purchase.

### fct_lead_conversions.csv
Contains filtered views of conversion events with separate records for different filter types ("ALL", "MEMBERSHIP", or "USER_CREDIT").

## Data Processing Logic
- A user is considered a CLIENT if they've made either a credit pack or membership purchase
- When a user has both types of purchases, the earliest one is considered their conversion event
- The lead conversions table includes separate records for different filter views


## Project Structure
```
├── data/
│   ├── dim_branch.csv
│   ├── dim_user.csv
│   ├── fct_credit_pack_purchases.csv
│   └── fct_membership_purchases.csv
├── outputs/
│   ├── fct_client_conversion_events.csv
│   └── fct_lead_conversions.csv
├── main.py
├── requirements.txt
└── README.md
```
