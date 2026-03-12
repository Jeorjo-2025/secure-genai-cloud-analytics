import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------
# 1. Database connection
# ---------------------------------------------------------
DB_USER = "postgres"
DB_PASS = "admin123"   # your new password
DB_HOST = "localhost"
DB_PORT = "5432"       # change if your PostgreSQL uses 5433
DB_NAME = "secure_analytics"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ---------------------------------------------------------
# 2. Helper function to run SQL
# ---------------------------------------------------------
def run_sql(sql):
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

# ---------------------------------------------------------
# 3. CURATED TABLE: Daily User Activity
# ---------------------------------------------------------
daily_user_activity_sql = """
CREATE TABLE IF NOT EXISTS daily_user_activity AS
SELECT
    DATE(timestamp) AS activity_date,
    user_id,
    COUNT(*) FILTER (WHERE action = 'login_success') AS successful_logins,
    COUNT(*) FILTER (WHERE action = 'login_failed') AS failed_logins,
    COUNT(DISTINCT ip_address) AS unique_ips
FROM cloud_activity_logs_raw
GROUP BY 1, 2
ORDER BY 1, 2;
"""

# ---------------------------------------------------------
# 4. CURATED TABLE: Daily Security Alerts
# ---------------------------------------------------------
daily_security_alerts_sql = """
CREATE TABLE IF NOT EXISTS daily_security_alerts AS
SELECT
    DATE(alert_timestamp) AS alert_date,
    severity,
    COUNT(*) AS alert_count
FROM security_alerts_raw
GROUP BY 1, 2
ORDER BY 1, 2;
"""

# ---------------------------------------------------------
# 5. CURATED TABLE: Incident Summary
# ---------------------------------------------------------
incident_summary_sql = """
CREATE TABLE IF NOT EXISTS incident_summary AS
SELECT
    incident_id,
    DATE(incident_timestamp) AS incident_date,
    user_id,
    incident_type,
    severity,
    status,
    description
FROM incidents_raw
ORDER BY incident_timestamp;
"""

# ---------------------------------------------------------
# 6. Build all curated tables
# ---------------------------------------------------------
def build_curated_tables():
    print("Building curated table: daily_user_activity...")
    run_sql(daily_user_activity_sql)

    print("Building curated table: daily_security_alerts...")
    run_sql(daily_security_alerts_sql)

    print("Building curated table: incident_summary...")
    run_sql(incident_summary_sql)

    print("All curated tables created successfully.")

if __name__ == "__main__":
    build_curated_tables()
