# TechFlow Analytics

Production-grade dbt project for a fictional multi-product SaaS company. Demonstrates three-layer architecture, incremental models, comprehensive testing, CI/CD with state deferral, and full documentation.

## Business Context

**TechFlow SaaS** offers three products:
- **CloudSync** - File storage and synchronization
- **TeamChat** - Team messaging and collaboration
- **DataHub** - Analytics and reporting platform

This project solves real analytics problems: MRR tracking, churn prediction signals, revenue attribution, cohort analysis, and marketing ROI.

## Architecture

```
Sources (Parquet)          Staging (views)           Intermediate (ephemeral)       Marts (tables)
------------------         ----------------          -------------------------      ---------------
app_db.users           ->  stg_app_db__users     ->  int_subscription_events    ->  dim_customers
app_db.subscriptions   ->  stg_app_db__subs      ->  int_daily_mrr_changes      ->  dim_subscriptions
app_db.usage_events    ->  stg_app_db__events    ->  int_revenue_attribution    ->  fct_mrr_daily (incr)
stripe.customers       ->  stg_stripe__customers ->  int_user_engagement_daily  ->  fct_revenue
stripe.charges         ->  stg_stripe__charges   ->  int_feature_adoption       ->  fct_subscription_events
stripe.invoices        ->  stg_stripe__invoices  ->  int_usage_cohorts          ->  dim_users
segment.tracks         ->  stg_segment__tracks   ->  int_user_acquisition       ->  fct_events
segment.identifies     ->  stg_segment__ids      ->  int_campaign_attribution   ->  rpt_feature_adoption
salesforce.accounts    ->  stg_sf__accounts      ->                             ->  fct_customer_acquisition
salesforce.opps        ->  stg_sf__opps          ->                             ->  rpt_marketing_roi
```

**Key patterns demonstrated:**
- Incremental model (`fct_mrr_daily`) with merge strategy and `is_incremental()` guard
- Surrogate keys via `dbt_utils.generate_surrogate_key()`
- Date spine via `dbt_utils.date_spine()` for gap-free time series
- SCD Type 2 snapshots with timestamp strategy
- Source freshness monitoring
- Unit tests, singular tests, and `dbt_expectations` business rule tests
- Custom schema routing macro
- Exposures for downstream BI dashboards and ML models
- CI/CD with state deferral (slim CI)

## Quick Start

### Prerequisites
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

### Setup (DuckDB - zero cost, zero config)

```bash
git clone <repo-url> && cd techflow-analytics

# Install dependencies
uv sync

# Generate synthetic test data (Parquet files)
uv run python scripts/generate_test_data.py

# Install dbt packages
dbt deps

# Load seed reference data
dbt seed

# Run the full project
dbt build

# Explore the docs
dbt docs generate && dbt docs serve
```

### Optional: Snowflake Setup

For dbt Cloud comparison testing:

1. Sign up for a [Snowflake free trial](https://signup.snowflake.com/)
2. Copy `profiles.yml.example` to `~/.dbt/profiles.yml`
3. Uncomment the Snowflake target and fill in credentials
4. Run with `dbt build --target snowflake`

## Data Model

### Seeds (reference data)
| Seed | Description |
|------|-------------|
| `plan_catalog` | 18 plans: 3 products x 3 tiers x 2 billing periods |
| `product_features` | Feature flags per product and plan tier |
| `utm_channel_mapping` | UTM parameter to marketing channel normalization |

### Sources (10 tables across 4 systems)
| System | Tables | Description |
|--------|--------|-------------|
| app_db | users, subscriptions, usage_events | Core application data |
| stripe | customers, charges, invoices | Payment processing |
| segment | tracks, identifies | Product analytics events |
| salesforce | accounts, opportunities | CRM/sales pipeline |

### Marts (3 domains)
| Domain | Models | Key Metrics |
|--------|--------|-------------|
| Finance | dim_customers, dim_subscriptions, fct_mrr_daily, fct_revenue, fct_subscription_events | MRR, ARR, churn rate, revenue by product |
| Product | dim_users, fct_events, fct_user_engagement_daily, rpt_feature_adoption | DAU, feature adoption, retention |
| Marketing | dim_campaigns, fct_customer_acquisition, rpt_marketing_roi | CAC, conversion rate, channel ROI |

## Testing Strategy

| Test Type | Count | Examples |
|-----------|-------|---------|
| Generic (YAML) | 80+ | unique, not_null, relationships, accepted_values |
| dbt_expectations | 4+ | column_values_between, table_row_count |
| Singular | 3 | MRR non-negative, valid subscription states, revenue reconciliation |
| Unit | 2 | Monthly MRR passthrough, annual-to-monthly normalization |

## CI/CD

### Slim CI (Pull Requests)
- Triggered on PRs to `main`
- Downloads production manifest artifact
- Runs `dbt build --select state:modified+ --defer` (only changed models + downstream)
- Lints changed SQL files with SQLFluff

### Production (Main Branch)
- Triggered on push to `main`
- Full `dbt build` + `dbt docs generate`
- Uploads manifest artifact for slim CI deferral

## dbt Core vs dbt Cloud

| Feature | dbt Core | dbt Cloud |
|---------|----------|-----------|
| **IDE** | VS Code + CLI | Cloud IDE (browser) |
| **CI/CD** | GitHub Actions (DIY) | Built-in CI with PR comments |
| **Scheduling** | cron / Airflow / Dagster | Built-in scheduler |
| **Docs** | `dbt docs serve` (local) | Hosted docs (auto-deployed) |
| **State Deferral** | Manual artifact management | Automatic with environments |
| **Cost** | Free (open source) | $100+/month per seat |
| **Semantic Layer** | Not included | Included (MetricFlow) |

This project demonstrates that dbt Core can achieve the same data quality outcomes with some additional DevOps setup. dbt Cloud adds convenience for scheduling, docs hosting, and team collaboration.

## Project Structure

```
techflow-analytics/
├── .github/workflows/     # CI/CD pipelines
├── analyses/              # Ad-hoc analysis queries
├── data/                  # Parquet source files (generated)
├── macros/                # Reusable SQL macros
├── models/
│   ├── staging/           # 1:1 source mirrors (views)
│   ├── intermediate/      # Business logic (ephemeral)
│   └── marts/             # Business-facing tables
├── scripts/               # Data generation
├── seeds/                 # Reference data (CSV)
├── snapshots/             # SCD Type 2 history
└── tests/                 # Singular SQL tests
```
