import csv
import json
from datetime import datetime, timezone
import os

def parse_custom_datetime(dt_str):
    """
    Parses a datetime string that could be in ISO 8601 or custom format.
    Replaces 'Z' with '+00:00' for UTC.
    Returns a datetime object or None on failure.
    """
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except ValueError:
        try:
            return datetime.strptime(dt_str, '%d/%m/%y %H:%M').replace(tzinfo=timezone.utc)
        except ValueError:
            return None

def read_csv(file_path, datetime_fields=None):
    """
    Reads CSV data and normalizes specified datetime fields to ISO 8601 format.
    Returns a list of row dictionaries.
    """
    data = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            for key in row:
                if datetime_fields and key in datetime_fields:
                    dt = parse_custom_datetime(row[key])
                    row[key] = dt.strftime('%Y-%m-%dT%H:%M:%SZ') if dt else None
            data.append(row)
    return data

def write_csv(data, file_path, fieldnames):
    """
    Writes a list of dictionaries to a CSV using specified fieldnames.
    """
    with open(file_path, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

print("Loading data...")
dim_branch = read_csv('data/dim_branch.csv')
dim_user = read_csv('data/dim_user.csv')
fct_credit_pack_purchases = read_csv('data/fct_credit_pack_purchases.csv', datetime_fields=['credit_pack_purchased_at'])
fct_membership_purchases = read_csv('data/fct_membership_purchases.csv', datetime_fields=['credit_membership_purchase_atpack_purchased_at'])

branch_lookup = {branch['branch_id']: branch for branch in dim_branch}
user_lookup = {user['user_id']: user for user in dim_user}

print("Processing credit pack purchases...")
user_credit_packs = {}
for purchase in fct_credit_pack_purchases:
    user_id = purchase['user_id']
    purchase['credit_pack_purchase_details'] = json.loads(purchase['credit_pack_purchase_details'])

    purchased_at = parse_custom_datetime(purchase['credit_pack_purchased_at'])
    if not purchased_at:
        continue

    # Store only the earliest credit pack purchase per user
    if user_id not in user_credit_packs or purchased_at < parse_custom_datetime(user_credit_packs[user_id]['credit_pack_purchased_at']):
        user_credit_packs[user_id] = purchase

print("Processing membership purchases...")
user_memberships = {}
for purchase in fct_membership_purchases:
    user_id = purchase['user_id']
    purchase['membership_purchase_details'] = json.loads(purchase['membership_purchase_details'])

    dt_str = purchase.get('credit_membership_purchase_atpack_purchased_at')
    purchased_at = parse_custom_datetime(dt_str)
    if not purchased_at:
        continue

    purchase['credit_membership_purchase_atpack_purchased_at'] = purchased_at.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Store only the earliest membership purchase per user
    if user_id not in user_memberships or purchased_at < parse_custom_datetime(user_memberships[user_id]['credit_membership_purchase_atpack_purchased_at']):
        user_memberships[user_id] = purchase

print("Creating client conversion events...")
client_conversion_events = []

for user in dim_user:
    user_id = user['user_id']
    branch_id = user['branch_id']

    # Initialize conversion event with LEAD status
    event = {
        'user_id': user_id,
        'branch_id': branch_id,
        'local_user_created_at': user['created_at'],
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

    # Populate credit pack info if available
    if user_id in user_credit_packs:
        credit_pack = user_credit_packs[user_id]
        event['first_credit_pack_id'] = credit_pack['credit_pack_id']
        event['first_local_credit_pack_purchased_at'] = credit_pack['credit_pack_purchased_at']
        event['first_credit_pack_name'] = credit_pack['credit_pack_purchase_details']['name']
        event['first_credit_pack_source'] = credit_pack['credit_pack_purchase_details']['source']

    # Populate membership info if available
    if user_id in user_memberships:
        membership = user_memberships[user_id]
        event['first_user_membership_id'] = membership['user_membership_id']
        event['first_local_membership_purchased_at'] = membership['credit_membership_purchase_atpack_purchased_at']
        event['first_membership_name'] = membership['membership_purchase_details']['name']
        event['first_membership_source'] = membership['membership_purchase_details']['source']

    # Determine conversion type and set client info
    has_credit = event['first_credit_pack_id'] is not None
    has_membership = event['first_user_membership_id'] is not None

    if has_credit and has_membership:
        credit_time = parse_custom_datetime(event['first_local_credit_pack_purchased_at'])
        membership_time = parse_custom_datetime(event['first_local_membership_purchased_at'])

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

    if event['client_conversion_event_type'] is not None:
        event['lead_status'] = 'CLIENT'

    client_conversion_events.append(event)

part1_fields = [
    'user_id', 'branch_id', 'local_user_created_at', 'lead_status', 'client_conversion_event_type',
    'client_conversion_event_id', 'client_conversion_event_local_created_at', 'client_conversion_event_name',
    'client_conversion_event_source', 'first_user_membership_id', 'first_local_membership_purchased_at',
    'first_membership_name', 'first_membership_source', 'first_credit_pack_id', 'first_local_credit_pack_purchased_at',
    'first_credit_pack_name', 'first_credit_pack_source'
]
write_csv(client_conversion_events, 'outputs/fct_client_conversion_events.csv', part1_fields)
print("Part 1 complete - Output saved to fct_client_conversion_events.csv")

print("Creating lead conversions table with filters...")
lead_conversions = []

for user_event in client_conversion_events:
    has_membership = user_event['first_user_membership_id'] is not None
    has_credit = user_event['first_credit_pack_id'] is not None

    # Membership conversion record
    if has_membership:
        membership_record = {
            'user_id': user_event['user_id'],
            'branch_id': user_event['branch_id'],
            'local_user_created_at': user_event['local_user_created_at'],
            'lead_status': 'CLIENT',
            'client_conversion_event_type': 'MEMBERSHIP',
            'client_conversion_event_id': user_event['first_user_membership_id'],
            'client_conversion_event_local_created_at': user_event['first_local_membership_purchased_at'],
            'client_conversion_event_name': user_event['first_membership_name'],
            'client_conversion_event_source': user_event['first_membership_source'],
            'client_conversion_event_filter': 'MEMBERSHIP'
        }
        lead_conversions.append(membership_record)
        all_record_membership = membership_record.copy()
        all_record_membership['client_conversion_event_filter'] = 'ALL'
        lead_conversions.append(all_record_membership)

    # Credit pack conversion record
    if has_credit:
        credit_record = {
            'user_id': user_event['user_id'],
            'branch_id': user_event['branch_id'],
            'local_user_created_at': user_event['local_user_created_at'],
            'lead_status': 'CLIENT',
            'client_conversion_event_type': 'USER_CREDIT',
            'client_conversion_event_id': user_event['first_credit_pack_id'],
            'client_conversion_event_local_created_at': user_event['first_local_credit_pack_purchased_at'],
            'client_conversion_event_name': user_event['first_credit_pack_name'],
            'client_conversion_event_source': user_event['first_credit_pack_source'],
            'client_conversion_event_filter': 'USER_CREDIT'
        }
        lead_conversions.append(credit_record)
        if not has_membership:
            all_record_credit = credit_record.copy()
            all_record_credit['client_conversion_event_filter'] = 'ALL'
            lead_conversions.append(all_record_credit)

part2_fields = [
    'user_id', 'branch_id', 'local_user_created_at', 'lead_status', 'client_conversion_event_type',
    'client_conversion_event_id', 'client_conversion_event_local_created_at', 'client_conversion_event_name',
    'client_conversion_event_source', 'client_conversion_event_filter'
]
write_csv(lead_conversions, 'outputs/fct_lead_conversions.csv', part2_fields)
print("Part 2 complete - Output saved to fct_lead_conversions.csv")

print("Processing complete!")
