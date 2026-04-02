"""
Breezeway → PostgreSQL Live Sync
Keeps your database in sync with Breezeway so Tableau Cloud
can maintain a live connection to always-fresh data.

Usage:
    1. Fill in your credentials in .env (see setup guide)
    2. pip install requests psycopg2-binary python-dotenv schedule
    3. python breezeway_tableau_sync.py
"""

import os
import time
import json
import logging
import requests
import psycopg2
import psycopg2.extras
import schedule
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# ─── Configuration ───────────────────────────────────────────────
BREEZEWAY_CLIENT_ID = os.getenv("BREEZEWAY_CLIENT_ID")
BREEZEWAY_CLIENT_SECRET = os.getenv("BREEZEWAY_CLIENT_SECRET")
BREEZEWAY_BASE_URL = "https://api.breezeway.io/public"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "breezeway_data")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "5"))
PAGE_SIZE = 100

# ─── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("breezeway_sync.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("breezeway_sync")


# ═══════════════════════════════════════════════════════════════════
# AUTH — handles token lifecycle so you never think about it
# ═══════════════════════════════════════════════════════════════════
class BreezewayAuth:
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None

    def get_token(self):
        """Returns a valid access token, refreshing if needed."""
        if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
            return self.access_token

        if self.refresh_token:
            try:
                return self._refresh()
            except Exception as e:
                log.warning(f"Token refresh failed, re-authenticating: {e}")

        return self._authenticate()

    def _authenticate(self):
        """Full authentication with client credentials."""
        log.info("Authenticating with Breezeway API...")
        resp = requests.post(
            f"{BREEZEWAY_BASE_URL}/auth/v1/",
            json={
                "client_id": BREEZEWAY_CLIENT_ID,
                "client_secret": BREEZEWAY_CLIENT_SECRET
            },
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        self.access_token = data["access_token"]
        self.refresh_token = data.get("refresh_token")
        # Set expiry 1 hour before actual (24h) to be safe
        self.token_expiry = datetime.now() + timedelta(hours=23)

        log.info("Authentication successful.")
        return self.access_token

    def _refresh(self):
        """Refresh an expired access token."""
        log.info("Refreshing access token...")
        resp = requests.post(
            f"{BREEZEWAY_BASE_URL}/auth/v1/refresh",
            json={"refresh_token": self.refresh_token},
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        self.access_token = data["access_token"]
        self.refresh_token = data.get("refresh_token", self.refresh_token)
        self.token_expiry = datetime.now() + timedelta(hours=23)

        log.info("Token refreshed successfully.")
        return self.access_token


auth = BreezewayAuth()


# ═══════════════════════════════════════════════════════════════════
# API CLIENT — paginated fetching for all endpoints
# ═══════════════════════════════════════════════════════════════════
def api_get(endpoint, params=None):
    """Single API GET request with auth."""
    token = auth.get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.get(
        f"{BREEZEWAY_BASE_URL}/{endpoint}",
        headers=headers,
        params=params or {},
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()


def fetch_all_pages(endpoint, params=None, data_key=None):
    """Fetch all pages from a paginated Breezeway endpoint."""
    all_records = []
    offset = 0
    params = params or {}

    while True:
        params.update({"limit": PAGE_SIZE, "offset": offset})
        try:
            response = api_get(endpoint, params)
        except requests.exceptions.HTTPError as e:
            log.error(f"API error on {endpoint} (offset={offset}): {e}")
            break

        # Breezeway may return data in a nested key or as a list
        if data_key and isinstance(response, dict):
            records = response.get(data_key, [])
        elif isinstance(response, list):
            records = response
        elif isinstance(response, dict):
            # Try common keys
            for key in ["data", "results", "items", "records"]:
                if key in response:
                    records = response[key]
                    break
            else:
                records = [response]
        else:
            records = []

        if not records:
            break

        all_records.extend(records)
        log.info(f"  {endpoint}: fetched {len(all_records)} records so far...")

        if len(records) < PAGE_SIZE:
            break  # Last page

        offset += PAGE_SIZE
        time.sleep(0.3)  # Be nice to the API

    return all_records


# ═══════════════════════════════════════════════════════════════════
# DATABASE — schema creation and upserts
# ═══════════════════════════════════════════════════════════════════
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def init_database():
    """Create tables if they don't exist."""
    log.info("Initializing database schema...")
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS breezeway_properties (
            id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            country TEXT,
            bedrooms INTEGER,
            bathrooms NUMERIC,
            property_type TEXT,
            status TEXT,
            company_id TEXT,
            raw_data JSONB,
            synced_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS breezeway_tasks (
            id TEXT PRIMARY KEY,
            property_id TEXT,
            property_name TEXT,
            task_type TEXT,
            task_name TEXT,
            status TEXT,
            priority TEXT,
            assigned_to TEXT,
            scheduled_start TIMESTAMP,
            scheduled_end TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            notes TEXT,
            company_id TEXT,
            raw_data JSONB,
            synced_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS breezeway_reservations (
            id TEXT PRIMARY KEY,
            property_id TEXT,
            guest_name TEXT,
            checkin_date DATE,
            checkout_date DATE,
            status TEXT,
            source TEXT,
            guest_count INTEGER,
            company_id TEXT,
            raw_data JSONB,
            synced_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS breezeway_supplies (
            id TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            quantity NUMERIC,
            unit TEXT,
            cost NUMERIC,
            company_id TEXT,
            raw_data JSONB,
            synced_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS breezeway_users (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            role TEXT,
            status TEXT,
            company_id TEXT,
            raw_data JSONB,
            synced_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id SERIAL PRIMARY KEY,
            sync_started TIMESTAMP,
            sync_completed TIMESTAMP,
            records_synced INTEGER,
            status TEXT,
            details TEXT
        );

        -- Indexes for Tableau performance
        CREATE INDEX IF NOT EXISTS idx_tasks_property ON breezeway_tasks(property_id);
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON breezeway_tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_scheduled ON breezeway_tasks(scheduled_start);
        CREATE INDEX IF NOT EXISTS idx_reservations_property ON breezeway_reservations(property_id);
        CREATE INDEX IF NOT EXISTS idx_reservations_checkin ON breezeway_reservations(checkin_date);
        CREATE INDEX IF NOT EXISTS idx_properties_company ON breezeway_properties(company_id);
    """)

    conn.commit()
    cur.close()
    conn.close()
    log.info("Database schema ready.")


def safe_get(record, *keys, default=None):
    """Safely extract a nested value from a dict."""
    val = record
    for key in keys:
        if isinstance(val, dict):
            val = val.get(key, default)
        else:
            return default
    return val


def upsert_properties(records):
    """Upsert property records into the database."""
    if not records:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    now = datetime.now()

    for r in records:
        cur.execute("""
            INSERT INTO breezeway_properties
                (id, name, address, city, state, zip_code, country,
                 bedrooms, bathrooms, property_type, status, company_id, raw_data, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip_code = EXCLUDED.zip_code,
                country = EXCLUDED.country,
                bedrooms = EXCLUDED.bedrooms,
                bathrooms = EXCLUDED.bathrooms,
                property_type = EXCLUDED.property_type,
                status = EXCLUDED.status,
                company_id = EXCLUDED.company_id,
                raw_data = EXCLUDED.raw_data,
                synced_at = EXCLUDED.synced_at
        """, (
            str(safe_get(r, "id", default="")),
            safe_get(r, "name"),
            safe_get(r, "address"),
            safe_get(r, "city"),
            safe_get(r, "state"),
            safe_get(r, "zip_code") or safe_get(r, "zipcode") or safe_get(r, "postal_code"),
            safe_get(r, "country"),
            safe_get(r, "bedrooms"),
            safe_get(r, "bathrooms"),
            safe_get(r, "property_type") or safe_get(r, "type"),
            safe_get(r, "status"),
            str(safe_get(r, "company_id", default="")),
            json.dumps(r),
            now
        ))

    conn.commit()
    cur.close()
    conn.close()
    return len(records)


def upsert_tasks(records):
    """Upsert task records into the database."""
    if not records:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    now = datetime.now()

    for r in records:
        cur.execute("""
            INSERT INTO breezeway_tasks
                (id, property_id, property_name, task_type, task_name, status,
                 priority, assigned_to, scheduled_start, scheduled_end,
                 completed_at, created_at, updated_at, notes, company_id, raw_data, synced_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                property_id = EXCLUDED.property_id,
                property_name = EXCLUDED.property_name,
                task_type = EXCLUDED.task_type,
                task_name = EXCLUDED.task_name,
                status = EXCLUDED.status,
                priority = EXCLUDED.priority,
                assigned_to = EXCLUDED.assigned_to,
                scheduled_start = EXCLUDED.scheduled_start,
                scheduled_end = EXCLUDED.scheduled_end,
                completed_at = EXCLUDED.completed_at,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                notes = EXCLUDED.notes,
                company_id = EXCLUDED.company_id,
                raw_data = EXCLUDED.raw_data,
                synced_at = EXCLUDED.synced_at
        """, (
            str(safe_get(r, "id", default="")),
            str(safe_get(r, "property_id", default="") or safe_get(r, "home_id", default="")),
            safe_get(r, "property_name") or safe_get(r, "property", "name"),
            safe_get(r, "task_type") or safe_get(r, "type"),
            safe_get(r, "name") or safe_get(r, "task_name"),
            safe_get(r, "status"),
            safe_get(r, "priority"),
            safe_get(r, "assigned_to") or safe_get(r, "assignee", "name"),
            safe_get(r, "scheduled_start") or safe_get(r, "start_date"),
            safe_get(r, "scheduled_end") or safe_get(r, "end_date"),
            safe_get(r, "completed_at") or safe_get(r, "completed_date"),
            safe_get(r, "created_at") or safe_get(r, "created_date"),
            safe_get(r, "updated_at") or safe_get(r, "updated_date"),
            safe_get(r, "notes") or safe_get(r, "description"),
            str(safe_get(r, "company_id", default="")),
            json.dumps(r),
            now
        ))

    conn.commit()
    cur.close()
    conn.close()
    return len(records)


def upsert_reservations(records):
    """Upsert reservation records into the database."""
    if not records:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    now = datetime.now()

    for r in records:
        cur.execute("""
            INSERT INTO breezeway_reservations
                (id, property_id, guest_name, checkin_date, checkout_date,
                 status, source, guest_count, company_id, raw_data, synced_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                property_id = EXCLUDED.property_id,
                guest_name = EXCLUDED.guest_name,
                checkin_date = EXCLUDED.checkin_date,
                checkout_date = EXCLUDED.checkout_date,
                status = EXCLUDED.status,
                source = EXCLUDED.source,
                guest_count = EXCLUDED.guest_count,
                company_id = EXCLUDED.company_id,
                raw_data = EXCLUDED.raw_data,
                synced_at = EXCLUDED.synced_at
        """, (
            str(safe_get(r, "id", default="")),
            str(safe_get(r, "property_id", default="") or safe_get(r, "home_id", default="")),
            safe_get(r, "guest_name") or safe_get(r, "guest", "name"),
            safe_get(r, "checkin_date") or safe_get(r, "check_in"),
            safe_get(r, "checkout_date") or safe_get(r, "check_out"),
            safe_get(r, "status"),
            safe_get(r, "source") or safe_get(r, "channel"),
            safe_get(r, "guest_count") or safe_get(r, "number_of_guests"),
            str(safe_get(r, "company_id", default="")),
            json.dumps(r),
            now
        ))

    conn.commit()
    cur.close()
    conn.close()
    return len(records)


def upsert_supplies(records):
    """Upsert supply records into the database."""
    if not records:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    now = datetime.now()

    for r in records:
        cur.execute("""
            INSERT INTO breezeway_supplies
                (id, name, category, quantity, unit, cost, company_id, raw_data, synced_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                category = EXCLUDED.category,
                quantity = EXCLUDED.quantity,
                unit = EXCLUDED.unit,
                cost = EXCLUDED.cost,
                company_id = EXCLUDED.company_id,
                raw_data = EXCLUDED.raw_data,
                synced_at = EXCLUDED.synced_at
        """, (
            str(safe_get(r, "id", default="")),
            safe_get(r, "name"),
            safe_get(r, "category"),
            safe_get(r, "quantity"),
            safe_get(r, "unit"),
            safe_get(r, "cost") or safe_get(r, "price"),
            str(safe_get(r, "company_id", default="")),
            json.dumps(r),
            now
        ))

    conn.commit()
    cur.close()
    conn.close()
    return len(records)


def upsert_users(records):
    """Upsert user/people records into the database."""
    if not records:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    now = datetime.now()

    for r in records:
        cur.execute("""
            INSERT INTO breezeway_users
                (id, first_name, last_name, email, role, status, company_id, raw_data, synced_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                role = EXCLUDED.role,
                status = EXCLUDED.status,
                company_id = EXCLUDED.company_id,
                raw_data = EXCLUDED.raw_data,
                synced_at = EXCLUDED.synced_at
        """, (
            str(safe_get(r, "id", default="")),
            safe_get(r, "first_name"),
            safe_get(r, "last_name"),
            safe_get(r, "email"),
            safe_get(r, "role") or safe_get(r, "type"),
            safe_get(r, "status"),
            str(safe_get(r, "company_id", default="")),
            json.dumps(r),
            now
        ))

    conn.commit()
    cur.close()
    conn.close()
    return len(records)


# ═══════════════════════════════════════════════════════════════════
# SYNC ENGINE — orchestrates the full pull
# ═══════════════════════════════════════════════════════════════════
def run_sync():
    """Run a full sync of all Breezeway data."""
    sync_start = datetime.now()
    total_records = 0
    log.info("=" * 60)
    log.info(f"SYNC STARTED at {sync_start.isoformat()}")
    log.info("=" * 60)

    try:
        # 1. Properties
        log.info("Syncing properties...")
        properties = fetch_all_pages("inventory/v1/property")
        count = upsert_properties(properties)
        total_records += count
        log.info(f"  → {count} properties synced")

        # 2. Tasks (may need to iterate per property or fetch company-wide)
        log.info("Syncing tasks...")
        tasks = fetch_all_pages("inventory/v1/task")
        count = upsert_tasks(tasks)
        total_records += count
        log.info(f"  → {count} tasks synced")

        # 3. Reservations
        log.info("Syncing reservations...")
        reservations = fetch_all_pages("inventory/v1/reservation")
        count = upsert_reservations(reservations)
        total_records += count
        log.info(f"  → {count} reservations synced")

        # 4. Supplies
        log.info("Syncing supplies...")
        supplies = fetch_all_pages("inventory/v1/supplies")
        count = upsert_supplies(supplies)
        total_records += count
        log.info(f"  → {count} supplies synced")

        # 5. Users / People
        log.info("Syncing users...")
        users = fetch_all_pages("inventory/v1/people")
        count = upsert_users(users)
        total_records += count
        log.info(f"  → {count} users synced")

        # Log success
        sync_end = datetime.now()
        duration = (sync_end - sync_start).total_seconds()
        log.info(f"SYNC COMPLETE — {total_records} total records in {duration:.1f}s")

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO sync_log (sync_started, sync_completed, records_synced, status, details)
            VALUES (%s, %s, %s, %s, %s)
        """, (sync_start, sync_end, total_records, "success",
              f"Completed in {duration:.1f}s"))
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        log.error(f"SYNC FAILED: {e}", exc_info=True)
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO sync_log (sync_started, sync_completed, records_synced, status, details)
                VALUES (%s, %s, %s, %s, %s)
            """, (sync_start, datetime.now(), total_records, "error", str(e)))
            conn.commit()
            cur.close()
            conn.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════
# MAIN — run once then schedule
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info(f"Breezeway → PostgreSQL sync starting (every {SYNC_INTERVAL_MINUTES} min)")

    # Validate config
    missing = []
    if not BREEZEWAY_CLIENT_ID:
        missing.append("BREEZEWAY_CLIENT_ID")
    if not BREEZEWAY_CLIENT_SECRET:
        missing.append("BREEZEWAY_CLIENT_SECRET")
    if not DB_HOST:
        missing.append("DB_HOST")
    if not DB_USER:
        missing.append("DB_USER")
    if not DB_PASSWORD:
        missing.append("DB_PASSWORD")

    if missing:
        log.error(f"Missing required env vars: {', '.join(missing)}")
        log.error("Create a .env file — see setup guide for details.")
        exit(1)

    # Init DB and run first sync
    init_database()
    run_sync()

    # Schedule continuous syncs
    schedule.every(SYNC_INTERVAL_MINUTES).minutes.do(run_sync)

    log.info(f"Scheduler running. Next sync in {SYNC_INTERVAL_MINUTES} minutes. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(1)
