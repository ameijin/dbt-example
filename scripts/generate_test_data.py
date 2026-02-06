#!/usr/bin/env python3
"""
Generate realistic test data for TechFlow Analytics dbt project.

Creates synthetic data for multiple source systems:
- app_db: users, subscriptions, usage_events
- stripe: customers, charges, invoices
- segment: tracks, identifies
- salesforce: accounts, opportunities

Outputs Parquet files to data/ for DuckDB to read directly.
"""

import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_USERS = 500
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)
DATA_DIR = Path(__file__).parent.parent / "data"

PRODUCTS = ["cloudsync", "teamchat", "datahub"]
PLANS = ["starter", "professional", "enterprise"]
BILLING_PERIODS = ["monthly", "annual"]
SUBSCRIPTION_STATUSES = ["trial", "active", "past_due", "canceled", "expired"]
COMPANY_SIZES = ["1-10", "11-50", "51-200", "201-500", "500+"]
INDUSTRIES = [
    "Technology", "Finance", "Healthcare", "Retail",
    "Education", "Manufacturing",
]
SIGNUP_SOURCES = ["website", "referral", "api", "mobile_app"]
UTM_SOURCES = ["google", "facebook", "linkedin", "direct", "referral"]
UTM_MEDIUMS = ["cpc", "organic", "email", "social"]

PRICING = {
    ("cloudsync", "starter", "monthly"): 999,
    ("cloudsync", "starter", "annual"): 9990,
    ("cloudsync", "professional", "monthly"): 2999,
    ("cloudsync", "professional", "annual"): 29990,
    ("cloudsync", "enterprise", "monthly"): 9999,
    ("cloudsync", "enterprise", "annual"): 99990,
    ("teamchat", "starter", "monthly"): 1499,
    ("teamchat", "starter", "annual"): 14990,
    ("teamchat", "professional", "monthly"): 4999,
    ("teamchat", "professional", "annual"): 49990,
    ("teamchat", "enterprise", "monthly"): 14999,
    ("teamchat", "enterprise", "annual"): 149990,
    ("datahub", "starter", "monthly"): 1999,
    ("datahub", "starter", "annual"): 19990,
    ("datahub", "professional", "monthly"): 5999,
    ("datahub", "professional", "annual"): 59990,
    ("datahub", "enterprise", "monthly"): 19999,
    ("datahub", "enterprise", "annual"): 199990,
}

EVENT_TYPES = [
    "page_view", "feature_used", "file_uploaded", "file_downloaded",
    "message_sent", "dashboard_viewed", "report_generated", "api_call",
    "search_performed", "settings_changed", "integration_connected",
    "export_created",
]

CHARGE_STATUSES = ["succeeded", "failed", "refunded", "pending"]
INVOICE_STATUSES = ["paid", "open", "void", "uncollectible"]
OPPORTUNITY_STAGES = [
    "Prospecting", "Qualification", "Needs Analysis",
    "Proposal", "Negotiation", "Closed Won", "Closed Lost",
]


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = random.randint(0, max(int(delta.total_seconds()), 1))
    return start + timedelta(seconds=seconds)


def random_date_weighted_recent(start: datetime, end: datetime) -> datetime:
    """Weight toward more recent dates (exponential distribution)."""
    delta = (end - start).total_seconds()
    # Use beta distribution skewed toward 1.0 (recent)
    fraction = random.betavariate(2, 5)
    return start + timedelta(seconds=delta * (1 - fraction))


def iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def write_parquet(name: str, data: list[dict]) -> None:
    if not data:
        print(f"  SKIP {name} (no data)")
        return
    table = pa.Table.from_pylist(data)
    path = DATA_DIR / f"{name}.parquet"
    pq.write_table(table, path)
    print(f"  {name}.parquet — {len(data)} rows")


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_users() -> list[dict]:
    now = datetime.now()
    users = []
    for i in range(1, NUM_USERS + 1):
        created_at = random_date_weighted_recent(START_DATE, END_DATE - timedelta(days=30))
        updated_at = random_date(created_at, min(created_at + timedelta(days=180), END_DATE))
        last_login = random_date(created_at, END_DATE) if random.random() > 0.1 else None
        trial_started = created_at if random.random() > 0.3 else None
        trial_ended = (created_at + timedelta(days=14)) if trial_started and random.random() > 0.4 else None

        users.append({
            "id": i,
            "email": fake.unique.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "account_tier": random.choice(["free", "starter", "professional", "enterprise"]),
            "account_status": random.choices(
                ["active", "inactive", "suspended", "churned"],
                weights=[0.6, 0.15, 0.05, 0.2],
            )[0],
            "company_name": fake.company(),
            "company_size": random.choice(COMPANY_SIZES),
            "industry": random.choice(INDUSTRIES),
            "uses_cloud_sync": random.random() < 0.6,
            "uses_team_chat": random.random() < 0.5,
            "uses_data_hub": random.random() < 0.3,
            "country_code": fake.country_code(),
            "timezone": fake.timezone(),
            "created_at": iso(created_at),
            "updated_at": iso(updated_at),
            "last_login_at": iso(last_login),
            "trial_started_at": iso(trial_started),
            "trial_ended_at": iso(trial_ended),
            "signup_source": random.choice(SIGNUP_SOURCES),
            "utm_source": random.choice(UTM_SOURCES),
            "utm_medium": random.choice(UTM_MEDIUMS),
            "utm_campaign": f"campaign_{random.randint(1, 20)}",
            "referral_code": f"REF{random.randint(1000, 9999)}" if random.random() > 0.7 else None,
            "is_test_user": False,
            "is_internal_user": random.random() < 0.02,
            "email_verified": random.random() > 0.05,
            "phone_verified": random.random() > 0.6,
            "deleted_at": None,
            "loaded_at": iso(now),
        })
    return users


def generate_subscriptions(users: list[dict]) -> list[dict]:
    now = datetime.now()
    subscriptions = []
    sub_id = 1

    for user in users:
        if random.random() > 0.3:  # 70% have subscriptions
            num_subs = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
            for _ in range(num_subs):
                product = random.choice(PRODUCTS)
                plan = random.choice(PLANS)
                billing_period = random.choice(BILLING_PERIODS)
                created_at = datetime.fromisoformat(user["created_at"]) + timedelta(
                    days=random.randint(0, 7)
                )
                if created_at > END_DATE:
                    created_at = END_DATE - timedelta(days=1)
                trial_end = created_at + timedelta(days=14)

                roll = random.random()
                if roll < 0.15:
                    status = "trial"
                    canceled_at = None
                    ended_at = None
                elif roll < 0.25:
                    status = "canceled"
                    canceled_at = random_date(
                        created_at + timedelta(days=30),
                        min(created_at + timedelta(days=365), END_DATE),
                    )
                    ended_at = canceled_at + timedelta(days=30)
                    if ended_at > END_DATE:
                        ended_at = END_DATE
                elif roll < 0.30:
                    status = "past_due"
                    canceled_at = None
                    ended_at = None
                elif roll < 0.35:
                    status = "expired"
                    canceled_at = None
                    ended_at = random_date(
                        created_at + timedelta(days=60),
                        min(created_at + timedelta(days=365), END_DATE),
                    )
                else:
                    status = "active"
                    canceled_at = None
                    ended_at = None

                amount_cents = PRICING.get((product, plan, billing_period), 1999)
                discount = int(amount_cents * random.uniform(0, 0.3)) if random.random() > 0.7 else 0

                period_days = 30 if billing_period == "monthly" else 365
                period_end = created_at + timedelta(days=period_days)

                subscriptions.append({
                    "id": sub_id,
                    "user_id": user["id"],
                    "plan_id": PLANS.index(plan) * 6 + PRODUCTS.index(product) * 2 + (
                        1 if billing_period == "monthly" else 2
                    ),
                    "product": product,
                    "plan_name": plan,
                    "billing_period": billing_period,
                    "amount_cents": amount_cents,
                    "discount_cents": discount,
                    "status": status,
                    "quantity": random.randint(1, 20),
                    "trial_start_date": iso(created_at) if status == "trial" else None,
                    "trial_end_date": iso(trial_end) if status == "trial" else None,
                    "current_period_start": iso(created_at),
                    "current_period_end": iso(period_end),
                    "cancel_at_period_end": status == "canceled",
                    "canceled_at": iso(canceled_at),
                    "ended_at": iso(ended_at),
                    "created_at": iso(created_at),
                    "updated_at": iso(created_at),
                    "stripe_subscription_id": f"sub_{uuid.uuid4().hex[:24]}",
                    "promo_code": f"PROMO{random.randint(10, 99)}" if discount > 0 else None,
                    "payment_method": random.choice(["card", "invoice", "bank_transfer"]),
                    "deleted_at": None,
                    "loaded_at": iso(now),
                })
                sub_id += 1

    return subscriptions


def generate_usage_events(users: list[dict]) -> list[dict]:
    now = datetime.now()
    events = []
    event_id = 1

    for user in users:
        if user["account_status"] not in ("active", "inactive"):
            continue
        created_at = datetime.fromisoformat(user["created_at"])
        num_events = random.randint(20, 200)

        for _ in range(num_events):
            event_ts = random_date(created_at, END_DATE)
            event_type = random.choice(EVENT_TYPES)
            product = random.choice(PRODUCTS)

            events.append({
                "id": event_id,
                "user_id": user["id"],
                "event_type": event_type,
                "product": product,
                "event_timestamp": iso(event_ts),
                "session_id": f"sess_{uuid.uuid4().hex[:16]}",
                "page_url": f"/{product}/{fake.uri_path()}" if event_type == "page_view" else None,
                "feature_name": random.choice([
                    "file_sync", "version_history", "channels", "dashboards",
                    "sql_editor", "api_access",
                ]) if event_type == "feature_used" else None,
                "duration_seconds": random.randint(1, 3600) if event_type in (
                    "page_view", "dashboard_viewed"
                ) else None,
                "properties": f'{{"source": "{random.choice(["web", "mobile", "api"])}"}}',
                "loaded_at": iso(now),
            })
            event_id += 1

            if len(events) >= 50000:
                return events

    return events


def generate_stripe_customers(users: list[dict]) -> list[dict]:
    now = datetime.now()
    customers = []
    for user in users:
        if random.random() > 0.15:  # 85% have stripe customers
            created_at = datetime.fromisoformat(user["created_at"])
            customers.append({
                "id": f"cus_{uuid.uuid4().hex[:24]}",
                "email": user["email"],
                "name": f"{user['first_name']} {user['last_name']}",
                "description": user["company_name"],
                "currency": "usd",
                "default_payment_method": f"pm_{uuid.uuid4().hex[:24]}",
                "created": int(created_at.timestamp()),
                "livemode": True,
                "delinquent": random.random() < 0.05,
                "metadata_user_id": str(user["id"]),
                "loaded_at": iso(now),
            })
    return customers


def generate_stripe_charges(subscriptions: list[dict]) -> list[dict]:
    now = datetime.now()
    charges = []
    charge_id = 1

    for sub in subscriptions:
        if sub["status"] in ("trial",):
            continue
        created_at = datetime.fromisoformat(sub["created_at"])
        num_charges = random.randint(1, 12)

        for i in range(num_charges):
            charge_ts = created_at + timedelta(days=30 * i)
            if charge_ts > END_DATE:
                break

            roll = random.random()
            if roll < 0.85:
                status = "succeeded"
            elif roll < 0.92:
                status = "failed"
            elif roll < 0.97:
                status = "refunded"
            else:
                status = "pending"

            amount = sub["amount_cents"] - sub["discount_cents"]
            charges.append({
                "id": f"ch_{uuid.uuid4().hex[:24]}",
                "amount": amount,
                "amount_refunded": amount if status == "refunded" else 0,
                "currency": "usd",
                "customer_id": sub.get("stripe_subscription_id", "").replace("sub_", "cus_"),
                "subscription_id": sub["stripe_subscription_id"],
                "status": status,
                "paid": status == "succeeded",
                "failure_code": "card_declined" if status == "failed" else None,
                "failure_message": "Your card was declined." if status == "failed" else None,
                "created": int(charge_ts.timestamp()),
                "loaded_at": iso(now),
            })
            charge_id += 1

    return charges


def generate_stripe_invoices(subscriptions: list[dict]) -> list[dict]:
    now = datetime.now()
    invoices = []

    for sub in subscriptions:
        if sub["status"] == "trial":
            continue
        created_at = datetime.fromisoformat(sub["created_at"])
        num_invoices = random.randint(1, 12)

        for i in range(num_invoices):
            invoice_ts = created_at + timedelta(days=30 * i)
            if invoice_ts > END_DATE:
                break

            period_start = invoice_ts
            period_end = invoice_ts + timedelta(
                days=30 if sub["billing_period"] == "monthly" else 365
            )

            amount = sub["amount_cents"] - sub["discount_cents"]
            roll = random.random()
            if roll < 0.88:
                status = "paid"
            elif roll < 0.95:
                status = "open"
            elif roll < 0.98:
                status = "void"
            else:
                status = "uncollectible"

            invoices.append({
                "id": f"in_{uuid.uuid4().hex[:24]}",
                "customer_id": sub.get("stripe_subscription_id", "").replace("sub_", "cus_"),
                "subscription_id": sub["stripe_subscription_id"],
                "status": status,
                "currency": "usd",
                "amount_due": amount,
                "amount_paid": amount if status == "paid" else 0,
                "amount_remaining": 0 if status == "paid" else amount,
                "subtotal": amount,
                "tax": int(amount * 0.08) if random.random() > 0.5 else 0,
                "total": amount,
                "period_start": int(period_start.timestamp()),
                "period_end": int(period_end.timestamp()),
                "due_date": int((invoice_ts + timedelta(days=30)).timestamp()),
                "created": int(invoice_ts.timestamp()),
                "loaded_at": iso(now),
            })

    return invoices


def generate_segment_tracks(users: list[dict]) -> list[dict]:
    now = datetime.now()
    tracks = []

    track_events = [
        "Signed Up", "Logged In", "Plan Upgraded", "Plan Downgraded",
        "Feature Activated", "File Uploaded", "Dashboard Created",
        "Report Exported", "Invite Sent", "Settings Updated",
        "Integration Connected", "Subscription Canceled",
    ]

    for user in users:
        if user["account_status"] == "churned" and random.random() > 0.3:
            continue
        created_at = datetime.fromisoformat(user["created_at"])
        num_tracks = random.randint(5, 50)

        for _ in range(num_tracks):
            event_ts = random_date(created_at, END_DATE)
            event = random.choice(track_events)

            tracks.append({
                "id": str(uuid.uuid4()),
                "user_id": str(user["id"]),
                "anonymous_id": f"anon_{uuid.uuid4().hex[:16]}",
                "event": event,
                "timestamp": iso(event_ts),
                "received_at": iso(event_ts + timedelta(seconds=random.randint(0, 60))),
                "context_page_url": f"https://app.techflow.io/{random.choice(PRODUCTS)}",
                "context_user_agent": fake.user_agent(),
                "context_ip": fake.ipv4(),
                "context_locale": random.choice(["en-US", "en-GB", "de-DE", "fr-FR", "ja-JP"]),
                "loaded_at": iso(now),
            })

    return tracks


def generate_segment_identifies(users: list[dict]) -> list[dict]:
    now = datetime.now()
    identifies = []

    for user in users:
        created_at = datetime.fromisoformat(user["created_at"])
        # Most users have 1-3 identify calls
        num_identifies = random.randint(1, 3)

        for i in range(num_identifies):
            identify_ts = random_date(created_at, END_DATE)
            identifies.append({
                "id": str(uuid.uuid4()),
                "user_id": str(user["id"]),
                "anonymous_id": f"anon_{uuid.uuid4().hex[:16]}",
                "timestamp": iso(identify_ts),
                "received_at": iso(identify_ts + timedelta(seconds=random.randint(0, 30))),
                "email": user["email"],
                "name": f"{user['first_name']} {user['last_name']}",
                "company_name": user["company_name"],
                "plan": user["account_tier"],
                "context_ip": fake.ipv4(),
                "loaded_at": iso(now),
            })

    return identifies


def generate_salesforce_accounts(users: list[dict]) -> list[dict]:
    now = datetime.now()
    accounts = []
    seen_companies: set[str] = set()

    for user in users:
        company = user["company_name"]
        if company in seen_companies:
            continue
        seen_companies.add(company)

        # Only ~40% of companies become SF accounts (enterprise focus)
        if random.random() > 0.4:
            continue

        created_at = datetime.fromisoformat(user["created_at"])
        accounts.append({
            "id": f"001{uuid.uuid4().hex[:15]}",
            "name": company,
            "type": random.choice(["Customer", "Prospect", "Partner"]),
            "industry": user["industry"],
            "annual_revenue": random.choice([
                50000, 100000, 250000, 500000, 1000000, 5000000, 10000000,
            ]),
            "number_of_employees": random.choice([
                10, 25, 50, 100, 250, 500, 1000, 5000,
            ]),
            "billing_city": fake.city(),
            "billing_country": user["country_code"],
            "owner_id": f"005{uuid.uuid4().hex[:15]}",
            "created_date": iso(created_at),
            "last_modified_date": iso(random_date(created_at, END_DATE)),
            "is_deleted": False,
            "loaded_at": iso(now),
        })

    return accounts


def generate_salesforce_opportunities(accounts: list[dict]) -> list[dict]:
    now = datetime.now()
    opportunities = []

    for account in accounts:
        num_opps = random.randint(1, 3)
        created_at = datetime.fromisoformat(account["created_date"])

        for _ in range(num_opps):
            close_date = random_date(
                created_at + timedelta(days=14),
                min(created_at + timedelta(days=180), END_DATE),
            )
            stage = random.choice(OPPORTUNITY_STAGES)
            is_won = stage == "Closed Won"
            is_closed = stage in ("Closed Won", "Closed Lost")
            amount = random.choice([
                5000, 10000, 25000, 50000, 100000, 250000,
            ])

            opportunities.append({
                "id": f"006{uuid.uuid4().hex[:15]}",
                "account_id": account["id"],
                "name": f"{account['name']} - {random.choice(PRODUCTS).title()} {random.choice(PLANS).title()}",
                "stage_name": stage,
                "amount": amount,
                "probability": {
                    "Prospecting": 10,
                    "Qualification": 20,
                    "Needs Analysis": 40,
                    "Proposal": 60,
                    "Negotiation": 80,
                    "Closed Won": 100,
                    "Closed Lost": 0,
                }[stage],
                "close_date": iso(close_date),
                "type": random.choice(["New Business", "Expansion", "Renewal"]),
                "lead_source": random.choice([
                    "Web", "Inbound", "Outbound", "Referral", "Partner",
                ]),
                "is_won": is_won,
                "is_closed": is_closed,
                "owner_id": account["owner_id"],
                "created_date": iso(random_date(created_at, close_date)),
                "last_modified_date": iso(close_date),
                "loaded_at": iso(now),
            })

    return opportunities


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)

    print("Generating TechFlow Analytics test data...")
    print(f"  Users: {NUM_USERS}")
    print(f"  Date range: {START_DATE.date()} to {END_DATE.date()}")
    print()

    # app_db
    print("app_db:")
    users = generate_users()
    write_parquet("users", users)

    subscriptions = generate_subscriptions(users)
    write_parquet("subscriptions", subscriptions)

    usage_events = generate_usage_events(users)
    write_parquet("usage_events", usage_events)

    # stripe
    print("stripe:")
    stripe_customers = generate_stripe_customers(users)
    write_parquet("stripe_customers", stripe_customers)

    stripe_charges = generate_stripe_charges(subscriptions)
    write_parquet("stripe_charges", stripe_charges)

    stripe_invoices = generate_stripe_invoices(subscriptions)
    write_parquet("stripe_invoices", stripe_invoices)

    # segment
    print("segment:")
    segment_tracks = generate_segment_tracks(users)
    write_parquet("segment_tracks", segment_tracks)

    segment_identifies = generate_segment_identifies(users)
    write_parquet("segment_identifies", segment_identifies)

    # salesforce
    print("salesforce:")
    sf_accounts = generate_salesforce_accounts(users)
    write_parquet("salesforce_accounts", sf_accounts)

    sf_opportunities = generate_salesforce_opportunities(sf_accounts)
    write_parquet("salesforce_opportunities", sf_opportunities)

    print("\nDone! Parquet files written to data/")
    print("Next: dbt deps && dbt seed && dbt build")


if __name__ == "__main__":
    main()
