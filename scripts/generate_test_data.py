#!/usr/bin/env python3
"""
Generate realistic test data for TechFlow Analytics dbt project

Creates synthetic data for:
- Users
- Subscriptions
- Usage events
- Payments (Stripe)

Saves to CSV files that can be loaded into DuckDB
"""

import csv
import random
from datetime import datetime, timedelta
from faker import Faker
import uuid

fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_USERS = 1000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2026, 1, 23)

PRODUCTS = ['cloudsync', 'teamchat', 'datahub']
PLANS = ['starter', 'professional', 'enterprise']
BILLING_PERIODS = ['monthly', 'annual']
SUBSCRIPTION_STATUSES = ['trial', 'active', 'past_due', 'canceled', 'expired']
COMPANY_SIZES = ['1-10', '11-50', '51-200', '201-500', '500+']
INDUSTRIES = ['Technology', 'Finance', 'Healthcare', 'Retail', 'Education', 'Manufacturing']
SIGNUP_SOURCES = ['website', 'referral', 'api', 'mobile_app']
UTM_SOURCES = ['google', 'facebook', 'linkedin', 'direct', 'referral']

# Price mapping (in cents)
PRICING = {
    ('cloudsync', 'starter', 'monthly'): 999,
    ('cloudsync', 'starter', 'annual'): 9990,
    ('cloudsync', 'professional', 'monthly'): 2999,
    ('cloudsync', 'professional', 'annual'): 29990,
    ('cloudsync', 'enterprise', 'monthly'): 9999,
    ('cloudsync', 'enterprise', 'annual'): 99990,
    ('teamchat', 'starter', 'monthly'): 1499,
    ('teamchat', 'starter', 'annual'): 14990,
    ('teamchat', 'professional', 'monthly'): 4999,
    ('teamchat', 'professional', 'annual'): 49990,
    ('teamchat', 'enterprise', 'monthly'): 14999,
    ('teamchat', 'enterprise', 'annual'): 149990,
    ('datahub', 'starter', 'monthly'): 1999,
    ('datahub', 'starter', 'annual'): 19990,
    ('datahub', 'professional', 'monthly'): 5999,
    ('datahub', 'professional', 'annual'): 59990,
    ('datahub', 'enterprise', 'monthly'): 19999,
    ('datahub', 'enterprise', 'annual'): 199990,
}


def random_date(start, end):
    """Generate random datetime between start and end"""
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )


def generate_users():
    """Generate user data"""
    users = []

    for i in range(1, NUM_USERS + 1):
        created_at = random_date(START_DATE, END_DATE - timedelta(days=30))

        user = {
            'id': i,
            'email': fake.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'account_tier': random.choice(['free', 'starter', 'professional', 'enterprise']),
            'account_status': random.choice(['active', 'inactive', 'suspended', 'churned']),
            'company_name': fake.company(),
            'company_size': random.choice(COMPANY_SIZES),
            'industry': random.choice(INDUSTRIES),
            'uses_cloud_sync': random.choice([True, False]),
            'uses_team_chat': random.choice([True, False]),
            'uses_data_hub': random.choice([True, False]),
            'country_code': fake.country_code(),
            'timezone': fake.timezone(),
            'created_at': created_at.isoformat(),
            'updated_at': created_at.isoformat(),
            'last_login_at': random_date(created_at, END_DATE).isoformat() if random.random() > 0.1 else None,
            'trial_started_at': created_at.isoformat() if random.random() > 0.3 else None,
            'trial_ended_at': (created_at + timedelta(days=14)).isoformat() if random.random() > 0.5 else None,
            'signup_source': random.choice(SIGNUP_SOURCES),
            'utm_source': random.choice(UTM_SOURCES),
            'utm_medium': random.choice(['cpc', 'organic', 'email', 'social']),
            'utm_campaign': f'campaign_{random.randint(1, 20)}',
            'referral_code': f'REF{random.randint(1000, 9999)}' if random.random() > 0.7 else None,
            'is_test_user': False,
            'is_internal_user': random.random() < 0.02,
            'email_verified': random.random() > 0.05,
            'phone_verified': random.random() > 0.6,
            'deleted_at': None
        }
        users.append(user)

    return users


def generate_subscriptions(users):
    """Generate subscription data"""
    subscriptions = []
    sub_id = 1

    for user in users:
        # 70% of users have at least one subscription
        if random.random() > 0.3:
            num_subs = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]

            for _ in range(num_subs):
                product = random.choice(PRODUCTS)
                plan = random.choice(PLANS)
                billing_period = random.choice(BILLING_PERIODS)

                created_at = datetime.fromisoformat(user['created_at']) + timedelta(days=random.randint(0, 7))
                trial_days = 14
                trial_end = created_at + timedelta(days=trial_days)

                # Determine status based on lifecycle
                if random.random() > 0.8:
                    status = 'trial'
                    canceled_at = None
                    ended_at = None
                elif random.random() > 0.7:
                    status = 'canceled'
                    canceled_at = random_date(created_at + timedelta(days=30), END_DATE)
                    ended_at = canceled_at + timedelta(days=30)
                elif random.random() > 0.95:
                    status = 'past_due'
                    canceled_at = None
                    ended_at = None
                else:
                    status = 'active'
                    canceled_at = None
                    ended_at = None

                amount_cents = PRICING.get((product, plan, billing_period), 1999)
                discount_cents = int(amount_cents * random.uniform(0, 0.3)) if random.random() > 0.7 else 0

                subscription = {
                    'id': sub_id,
                    'user_id': user['id'],
                    'plan_id': random.randint(1, 18),
                    'product': product,
                    'plan_name': plan,
                    'billing_period': billing_period,
                    'amount_cents': amount_cents,
                    'discount_cents': discount_cents,
                    'status': status,
                    'quantity': random.randint(1, 20),
                    'trial_start_date': created_at.date().isoformat() if status == 'trial' else None,
                    'trial_end_date': trial_end.date().isoformat() if status == 'trial' else None,
                    'current_period_start': created_at.date().isoformat(),
                    'current_period_end': (created_at + timedelta(
                        days=30 if billing_period == 'monthly' else 365)).date().isoformat(),
                    'cancel_at_period_end': status == 'canceled',
                    'canceled_at': canceled_at.isoformat() if canceled_at else None,
                    'ended_at': ended_at.isoformat() if ended_at else None,
                    'created_at': created_at.isoformat(),
                    'updated_at': created_at.isoformat(),
                    'stripe_subscription_id': f'sub_{uuid.uuid4().hex[:24]}',
                    'promo_code': f'PROMO{random.randint(10, 99)}' if discount_cents > 0 else None,
                    'payment_method': random.choice(['card', 'invoice', 'bank_transfer']),
                    'deleted_at': None
                }

                subscriptions.append(subscription)
                sub_id += 1

    return subscriptions


def write_csv(filename, data, fieldnames):
    """Write data to CSV file"""
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Created {filename} with {len(data)} rows")


def main():
    print("Generating synthetic data for TechFlow Analytics...")
    print(f"Users: {NUM_USERS}")
    print(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    print()

    # Generate data
    print("Generating users...")
    users = generate_users()

    print("Generating subscriptions...")
    subscriptions = generate_subscriptions(users)

    # Write to CSV
    print("\nWriting CSV files...")
    write_csv('data/users.csv', users, users[0].keys())
    write_csv('data/subscriptions.csv', subscriptions, subscriptions[0].keys())

    print("\n✓ All done! Load data with:")
    print("  dbt seed")


if __name__ == '__main__':
    main()