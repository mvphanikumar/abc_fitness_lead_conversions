"""
Client and Lead Conversion Pipeline - ABC Fitness
-------------------------------------------------
This is a modularized data engineering pipeline that processes gym lead conversion data.

Author: Phanikumar V Merugumala
Date: April 23, 2025
"""
# --------------------- IMPORTS --------------------- #

import csv
import json
import logging.config
import argparse
import sys
from datetime import datetime, timezone
import os
from typing import Dict, List, Optional, Any
import traceback

# --------------------- LOGGING --------------------- #

LOG_FILE = 'data_processing.log'
open(LOG_FILE, 'w').close() if os.path.exists(LOG_FILE) else None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)

# --------------------- CONFIG --------------------- #

DEFAULT_CONFIG = {
    "input_paths": {
        "dim_branch": "data/dim_branch.csv",
        "dim_user": "data/dim_user.csv",
        "fct_credit_pack_purchases": "data/fct_credit_pack_purchases.csv",
        "fct_membership_purchases": "data/fct_membership_purchases.csv",
        "fct_client_conversion_events_part_2": "data/fct_client_conversion_events_part_2.csv"
    },
    "output_paths": {
        "fct_client_conversion_events": "outputs/fct_client_conversion_events.csv",
        "fct_lead_conversions": "outputs/fct_lead_conversions.csv"
    }
}

def load_config(config_path: Optional[str] = None) -> Dict:
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
    return DEFAULT_CONFIG

# --------------------- UTILITIES --------------------- #

def parse_custom_datetime(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            dt = datetime.strptime(dt_str, '%d/%m/%y %H:%M').replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                dt = datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning(f"Failed to parse datetime: {dt_str}")
                return None

    return dt.isoformat(timespec='milliseconds')

# --------------------- CSV HANDLING --------------------- #

def read_csv(file_path: str, datetime_fields: Optional[List[str]] = None) -> List[Dict]:
    """Read a CSV file and return a list of dictionaries, handling datetime fields and JSON columns."""
    data = []
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return data

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, quotechar='"', escapechar='\\')
            for row in reader:
                # Clean whitespace
                cleaned = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

                # Parse datetime fields
                if datetime_fields:
                    for field in datetime_fields:
                        if field in cleaned and cleaned[field]:
                            dt = parse_custom_datetime(cleaned[field])
                            if dt:
                                cleaned[field] = dt
                                                               
                # Parse *_details JSON fields
                for key, value in cleaned.items():
                    if key and key.endswith('_details') and value:
                        try:
                            if isinstance(value, str):
                                cleaned[key] = json.loads(value)
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse JSON in field '{key}': {value}")
                            cleaned[key] = {}

                data.append(cleaned)

        logger.info(f"Loaded {len(data)} rows from {file_path}")
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        logger.error(traceback.format_exc())

    return data

def write_csv(data: List[Dict], file_path: str, fieldnames: List[str]) -> bool:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                formatted_row = {
                    k: (v.isoformat(timespec='milliseconds') if isinstance(v, datetime) else v)
                    for k, v in row.items()
                }
                writer.writerow(formatted_row)
        logger.info(f"Saved {len(data)} rows to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write CSV: {e}")
        logger.error(traceback.format_exc())
        return False

# --------------------- PROCESSING FUNCTIONS --------------------- #

def process_credit_packs(data: List[Dict]) -> Dict[str, Dict]:
    """
    Process credit pack purchase data to find the earliest credit pack purchase for each user.
    Assumes credit_pack_purchase_details is a valid JSON object already parsed in read_csv().
    """
    result = {}
    for row in data:
        user_id = row.get("user_id")
        if not user_id or not row.get("credit_pack_id"):
            logger.debug(f"Skipping credit pack record for user {user_id} - missing credit_pack_id")
            continue

        # Extract name and source from pre-parsed JSON
        details = row.get('credit_pack_purchase_details', {})
        name = details.get('name') if isinstance(details, dict) else None
        source = details.get('source') if isinstance(details, dict) else None

        # Update row with extracted values
        row['credit_pack_name'] = name
        row['credit_pack_source'] = source
        row['credit_pack_purchase_details'] = {"name": name, "source": source}
        
        # Handle missing purchase date
        purchase_date_str = row.get('credit_pack_purchased_at') or '9999-12-31T00:00:00Z'
        row['credit_pack_purchased_at'] = purchase_date_str
        logger.debug(f"Missing purchase date for user {user_id} credit pack - using future date")

        # Compare and store earliest purchase per user
        current_date = parse_custom_datetime(result.get(user_id, {}).get('credit_pack_purchased_at', '9999-12-31'))
        new_date = parse_custom_datetime(purchase_date_str)

        if user_id not in result or (new_date and current_date and new_date < current_date):
            result[user_id] = row

    logger.info(f"Processed credit packs: found {len(result)} unique users with credit pack purchases")
    return result

def process_memberships(data: List[Dict]) -> Dict[str, Dict]:
    """
   Process membership purchase data to find the earliest membership purchase for each user.
   Assumes membership_purchase_details is a valid JSON object already parsed in read_csv().
   """
    result = {}
    for row in data:
        user_id = row.get("user_id")
        if not user_id or not row.get("membership_id"):
            continue

         # Extract name and source from pre-parsed JSON
        details = row.get('membership_purchase_details', {})
        name = details.get('name') if isinstance(details, dict) else None
        source = details.get('source') if isinstance(details, dict) else None

        # Update row with extracted values
        row['membership_name'] = name
        row['membership_source'] = source
        row['membership_purchase_details'] = {"name": name, "source": source}

        # Handle missing purchase date
        purchase_date_str = row.get('membership_purchased_at') or '9999-12-31T00:00:00Z'
        row['membership_purchased_at'] = purchase_date_str
        logger.debug(f"Missing purchase date for user {user_id} membership - using future date")

        # Compare and store earliest purchase per user
        current_date = parse_custom_datetime(result.get(user_id, {}).get('membership_purchased_at', '9999-12-31'))
        new_date = parse_custom_datetime(purchase_date_str)

        if user_id not in result or (new_date and current_date and new_date < current_date):
            result[user_id] = row

    logger.info(f"Processed memberships: found {len(result)} unique users with membership purchases")
    return result

def create_client_conversion_events(users: List[Dict], credits: Dict[str, Dict], memberships: Dict[str, Dict]) -> List[Dict]:
    """
    Create client conversion events by combining user, credit pack, and membership data.
    Fields prefixed with client_conversion_ should contain details of either the first credit 
    pack or membership depending on which was purchased first.
    """
    results = []
    for user in users:
        user_id = user.get('user_id')
        if not user_id:
            continue

        # Initialize the event with default values
        event = {
            'user_id': user_id,
            'branch_id': user.get('branch_id'),
            'local_user_created_at': user.get('created_at'),
            'lead_status': 'LEAD',
            'client_conversion_event_type': None,
            'client_conversion_event_id': None,
            'client_conversion_event_local_created_at': None,
            'client_conversion_event_name': None,
            'client_conversion_event_source': None,
            'first_user_membership_id': None,
            'first_local_membership_purchased_at': None,
            'first_membership_name': None,
            'first_membership_source': None,
            'first_credit_pack_id': None,
            'first_local_credit_pack_purchased_at': None,
            'first_credit_pack_name': None,
            'first_credit_pack_source': None
        }

        # Add credit pack data if available
        has_credit = False
        if user_id in credits:
            credit = credits[user_id]
            has_credit = True
            event.update({
                'first_credit_pack_id': credit.get('credit_pack_id'),
                'first_local_credit_pack_purchased_at': credit.get('credit_pack_purchased_at'),
                'first_credit_pack_name': credit.get('credit_pack_name'),
                'first_credit_pack_source': credit.get('credit_pack_source')
            })

        # Add membership data if available
        has_membership = False
        if user_id in memberships:
            membership = memberships[user_id]
            has_membership = True
            event.update({
                'first_user_membership_id': membership.get('membership_id'),
                'first_local_membership_purchased_at': membership.get('membership_purchased_at'),
                'first_membership_name': membership.get('membership_name'),
                'first_membership_source': membership.get('membership_source')
            })

        # Determine which event happened first and set conversion event fields
        # First check if either event exists at all
        if has_credit or has_membership:
            # Default to setting lead_status to CLIENT since they have either membership or credit
            event['lead_status'] = 'CLIENT'

            credit_time = parse_custom_datetime(event.get('first_local_credit_pack_purchased_at'))
            membership_time = parse_custom_datetime(event.get('first_local_membership_purchased_at'))

            if has_credit and has_membership:
                # Both exist, determine which came first
                if credit_time and membership_time:
                    if credit_time <= membership_time:
                        event.update({
                            'client_conversion_event_type': 'USER_CREDIT',
                            'client_conversion_event_id': event['first_credit_pack_id'],
                            'client_conversion_event_local_created_at': event['first_local_credit_pack_purchased_at'],
                            'client_conversion_event_name': event['first_credit_pack_name'],
                            'client_conversion_event_source': event['first_credit_pack_source']
                        })
                    else:
                        event.update({
                            'client_conversion_event_type': 'MEMBERSHIP',
                            'client_conversion_event_id': event['first_user_membership_id'],
                            'client_conversion_event_local_created_at': event['first_local_membership_purchased_at'],
                            'client_conversion_event_name': event['first_membership_name'],
                            'client_conversion_event_source': event['first_membership_source']
                        })
            elif has_credit:
                event.update({
                    'client_conversion_event_type': 'USER_CREDIT',
                    'client_conversion_event_id': event['first_credit_pack_id'],
                    'client_conversion_event_local_created_at': event['first_local_credit_pack_purchased_at'],
                    'client_conversion_event_name': event['first_credit_pack_name'],
                    'client_conversion_event_source': event['first_credit_pack_source']
                })
            elif has_membership:
                event.update({
                    'client_conversion_event_type': 'MEMBERSHIP',
                    'client_conversion_event_id': event['first_user_membership_id'],
                    'client_conversion_event_local_created_at': event['first_local_membership_purchased_at'],
                    'client_conversion_event_name': event['first_membership_name'],
                    'client_conversion_event_source': event['first_membership_source']
                })

        results.append(event)

    # Count the number of clients found
    client_count = sum(1 for event in results if event['lead_status'] == 'CLIENT')
    logger.info(f"Created {len(results)} client conversion events ({client_count} CLIENTs, {len(results) - client_count} LEADs)")
    return results

def create_lead_conversions(events: List[Dict]) -> List[Dict]:
    """
    Create lead conversion records based on client conversion events.
    For each user who is a CLIENT:
    - If they have both membership and credit, create 3 records (MEMBERSHIP, USER_CREDIT, ALL)
    - If they have only membership, create 2 records (MEMBERSHIP, ALL)
    - If they have only credit, create 2 records (USER_CREDIT, ALL)
    
    The ALL record should show the conversion event that happened first.
    """

    records = []
    for event in events:
        # Skip if not a client (no conversion happened)
        if event.get('lead_status') != 'CLIENT':
            continue

        has_membership = event.get('first_user_membership_id') is not None
        has_credit = event.get('first_credit_pack_id') is not None

        if not has_membership and not has_credit:
            continue # Skip if no conversion info available

        membership_dt = parse_custom_datetime(event.get('first_local_membership_purchased_at'))
        credit_dt = parse_custom_datetime(event.get('first_local_credit_pack_purchased_at'))

        # Event type for ALL record - whichever event came first
        all_event_type = None
        if has_membership and has_credit:
            if credit_dt and membership_dt:
                all_event_type = 'USER_CREDIT' if credit_dt <= membership_dt else 'MEMBERSHIP'
        elif has_membership:
            all_event_type = 'MEMBERSHIP'
        elif has_credit:
            all_event_type = 'USER_CREDIT'

        # Create MEMBERSHIP record if applicable
        if has_membership:
            membership_record = event.copy()
            membership_record.update({
                'client_conversion_event_type': 'MEMBERSHIP',
                'client_conversion_event_id': event['first_user_membership_id'],
                'client_conversion_event_local_created_at': event['first_local_membership_purchased_at'],
                'client_conversion_event_name': event['first_membership_name'],
                'client_conversion_event_source': event['first_membership_source'],
                'client_conversion_event_filter': 'MEMBERSHIP'
            })
            records.append(membership_record)

        # Create USER_CREDIT record if applicable
        if has_credit:
            credit_record = event.copy()
            credit_record.update({
                'client_conversion_event_type': 'USER_CREDIT',
                'client_conversion_event_id': event['first_credit_pack_id'],
                'client_conversion_event_local_created_at': event['first_local_credit_pack_purchased_at'],
                'client_conversion_event_name': event['first_credit_pack_name'],
                'client_conversion_event_source': event['first_credit_pack_source'],
                'client_conversion_event_filter': 'USER_CREDIT'
            })
            records.append(credit_record)

        # Create ALL record with the earliest conversion type
        if all_event_type == 'MEMBERSHIP':
            all_record = event.copy()
            all_record.update({
                'client_conversion_event_type': 'MEMBERSHIP',
                'client_conversion_event_id': event['first_user_membership_id'],
                'client_conversion_event_local_created_at': event['first_local_membership_purchased_at'],
                'client_conversion_event_name': event['first_membership_name'],
                'client_conversion_event_source': event['first_membership_source'],
                'client_conversion_event_filter': 'ALL'
            })
            records.append(all_record)
        elif all_event_type == 'USER_CREDIT':
            all_record = event.copy()
            all_record.update({
                'client_conversion_event_type': 'USER_CREDIT',
                'client_conversion_event_id': event['first_credit_pack_id'],
                'client_conversion_event_local_created_at': event['first_local_credit_pack_purchased_at'],
                'client_conversion_event_name': event['first_credit_pack_name'],
                'client_conversion_event_source': event['first_credit_pack_source'],
                'client_conversion_event_filter': 'ALL'
            })
            records.append(all_record)

    # # Count records by filter type
    membership_count = sum(1 for r in records if r['client_conversion_event_filter'] == 'MEMBERSHIP')
    credit_count = sum(1 for r in records if r['client_conversion_event_filter'] == 'USER_CREDIT')
    all_count = sum(1 for r in records if r['client_conversion_event_filter'] == 'ALL')

    logger.info(f"Created {len(records)} lead conversion records ({membership_count} MEMBERSHIP, {credit_count} USER_CREDIT, {all_count} ALL)")
    return records

def read_conversion_events_part2(path: str) -> List[Dict]:
    """
    Read the pre-populated client conversion events part 2 file.
    This is used as a fallback if Part 1 fails to generate data.
    """
    if not os.path.exists(path):
        logger.warning(f"File not found: {path}")
        return []

    expected_fields = [
        'user_id','branch_id','local_user_created_at','lead_status','client_conversion_event_type',
        'client_conversion_event_id','client_conversion_event_local_created_at','client_conversion_event_name',
        'client_conversion_event_source','first_user_membership_id','first_local_membership_purchased_at',
        'first_membership_name','first_membership_source','first_credit_pack_id','first_local_credit_pack_purchased_at',
        'first_credit_pack_name','first_credit_pack_source'
    ]

    try:
        with open(path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            f.seek(0)
            has_header = 'user_id' in first_line
            reader = csv.DictReader(f, fieldnames=None if has_header else expected_fields)
            data = list(reader)
            logger.info(f"Loaded {len(data)} rows from {path}")
            return data
    except Exception as e:
        logger.error(f"Error reading {path}: {e}")
        logger.error(traceback.format_exc())
        return []

# --------------------- MAIN FUNCTION --------------------- #

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Path to config file', required=False)
    parser.add_argument('--part2_only', action='store_true', help='Run fallback Part 2 only')
    args = parser.parse_args()

    config = load_config(args.config)
    input_paths = config['input_paths']
    output_paths = config['output_paths']

    if args.part2_only:
        logger.info("Running Part 2 only mode using pre-populated data")
        data = read_conversion_events_part2(input_paths['fct_client_conversion_events_part_2'])
        if not data:
            logger.error("Failed to load fallback data")
            return
        leads = create_lead_conversions(data)
        if leads:
            os.makedirs(os.path.dirname(output_paths['fct_lead_conversions']), exist_ok=True)
            write_csv(leads, output_paths['fct_lead_conversions'], list(leads[0].keys()))
        else:
            logger.warning("No lead conversions data to write in part2_only mode")
        return

    # Full pipeline - Part 1 and Part 2
    logger.info("Running full pipeline (Part 1 and Part 2)")

    # Load data
    #users = read_csv(input_paths['dim_user'])
    users = read_csv(input_paths['dim_user'], ['created_at'])
    credit_data = read_csv(input_paths['fct_credit_pack_purchases'], ['credit_pack_purchased_at'])
    membership_data = read_csv(input_paths['fct_membership_purchases'], ['membership_purchased_at'])

    # Process Part 1: conversion events
    credits = process_credit_packs(credit_data)
    memberships = process_memberships(membership_data)
    client_events = create_client_conversion_events(users, credits, memberships)

    # Process Part 2: lead conversions from Part 1 output
    lead_conversions = create_lead_conversions(client_events)

    if lead_conversions:
        logger.info(f"Lead conversions generated from Part 1: {len(lead_conversions)}")
        print(f"Generated {len(lead_conversions)} lead conversions from Part 1")
    else:
        logger.warning("No lead conversions found from Part 1 — falling back to pre-generated part 2 data")
        fallback_events = read_conversion_events_part2(input_paths['fct_client_conversion_events_part_2'])
        if fallback_events:
            lead_conversions = create_lead_conversions(fallback_events)
            logger.info(f"Loaded and created {len(lead_conversions)} lead conversions from fallback file")
        else:
            logger.error("Fallback file is missing or empty")

    # Define field lists
    client_event_fields = [
    'user_id', 'branch_id', 'local_user_created_at', 'lead_status',
    'client_conversion_event_type', 'client_conversion_event_id',
    'client_conversion_event_local_created_at', 'client_conversion_event_name',
    'client_conversion_event_source', 'first_user_membership_id',
    'first_local_membership_purchased_at', 'first_membership_name',
    'first_membership_source', 'first_credit_pack_id',
    'first_local_credit_pack_purchased_at', 'first_credit_pack_name',
    'first_credit_pack_source'
    ]
    
    lead_conversion_fields = client_event_fields + ['client_conversion_event_filter']
    
    # Use actual fields from data if available
    if client_events:
        client_event_fields = list(client_events[0].keys())
    if lead_conversions:
        lead_conversion_fields = list(lead_conversions[0].keys())

    # Ensure output directories exist
    os.makedirs(os.path.dirname(output_paths['fct_client_conversion_events']), exist_ok=True)
    os.makedirs(os.path.dirname(output_paths['fct_lead_conversions']), exist_ok=True)

    # Write Part 1 output
    write_csv(client_events, output_paths['fct_client_conversion_events'], client_event_fields)

    # Write Part 2 output
    if lead_conversions:
        write_csv(lead_conversions, output_paths['fct_lead_conversions'], lead_conversion_fields)
    else:
        logger.warning("No lead conversions data to write")
       
if __name__ == '__main__':
    main()
