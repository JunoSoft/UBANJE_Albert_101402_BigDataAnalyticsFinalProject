"""
Shared configuration for the AUCA Big Data Analytics Final Project.
Toggle DATASET_MODE between DEMO (fast, presentation-friendly) and FULL (spec sizes).
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"
RESULTS_DIR = OUTPUTS_DIR / "results"

# DEMO: runs in minutes on a laptop. FULL: matches the assignment generator sizes.
DATASET_MODE = "DEMO"  # "DEMO" or "FULL"

DATASET_PROFILES = {
    "DEMO": {
        "NUM_USERS": 2000,
        "NUM_PRODUCTS": 800,
        "NUM_CATEGORIES": 15,
        "NUM_TRANSACTIONS": 15000,
        "NUM_SESSIONS": 40000,
        "TIMESPAN_DAYS": 90,
    },
    "FULL": {
        "NUM_USERS": 10000,
        "NUM_PRODUCTS": 5000,
        "NUM_CATEGORIES": 25,
        "NUM_TRANSACTIONS": 500000,
        "NUM_SESSIONS": 2000000,
        "TIMESPAN_DAYS": 90,
    },
}

# Subsets loaded into NoSQL stores (keeps Mongo/HBase demos practical)
MONGO_LOAD_LIMITS = {
    "users": None,          # all
    "products": None,       # all
    "categories": None,     # all
    "transactions": 20000,  # None = all
    "sessions": 15000,      # None = all
}

HBASE_LOAD_LIMITS = {
    "sessions": 15000,
    "page_views_per_session": None,  # all page views in loaded sessions
}

# Connection settings
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "ecommerce_analytics"

HBASE_HOST = "localhost"
HBASE_PORT = 9090  # Thrift

SPARK_APP_NAME = "AUCA_Ecommerce_Analytics"
SPARK_MASTER = "local[*]"

RANDOM_SEED = 42


def get_dataset_profile():
    mode = DATASET_MODE.upper()
    if mode not in DATASET_PROFILES:
        raise ValueError(f"Unknown DATASET_MODE={DATASET_MODE}. Use DEMO or FULL.")
    return DATASET_PROFILES[mode]


def ensure_dirs():
    for path in (DATA_DIR, FIGURES_DIR, RESULTS_DIR):
        path.mkdir(parents=True, exist_ok=True)
