import sqlite3
import pandas as pd
import os

def init_mock_db(db_path="argus.db"):
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    
    # Cases Table matching the new mockup
    cases_df = pd.DataFrame({
        "ID": ["CASE-9928-XA", "CASE-4102-EB", "CASE-8831-ZK"],
        "ENTITY_NAME": ["Al-Noor Logistics Ltd.", "Elena Rostova", "Zheng-Kwan Holdings"],
        "TYPE": ["CORPORATE", "INDIVIDUAL", "CORPORATE"],
        "COUNTRY": ["🇦🇪 UAE", "🇪🇺 EU (Multiple)", "🇸🇬 Singapore"],
        "RISK_SCORE": [88, 42, 12],
        "AI_CONFIDENCE": ["HIGH", "MED", "HIGH"],
        "STATUS": ["REQUIRES REVIEW", "IN PROGRESS", "AUTO-CLEARED"],
        "LAST_ACTIVITY": ["12 r...", "1 h...", "1 ..."] # Truncated strings as in mockup
    })
    cases_df.to_sql("cases", conn, index=False)
    
    # Case Metrics Table
    user_metrics = pd.DataFrame({
        "metric": ["Active Cases", "Pending Review (High Risk)", "AI Auto-Cleared (24H)", "Avg. Resolution Time"],
        "value": ["1,284", "42", "912", "4.2"],
        "unit": ["", "", "", "hrs"],
        "delta": ["+12% vs LY", "Urgent", "AI Boosted", ""]
    })
    user_metrics.to_sql("case_metrics", conn, index=False)
    
    # Old Metrics for the dashboard
    metrics_df = pd.DataFrame({
        "Day": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
        "Noise_Removed": [35, 45, 30, 60, 50, 75, 40]
    })
    metrics_df.to_sql("ai_metrics", conn, index=False)
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {db_path} with mock data.")

if __name__ == "__main__":
    init_mock_db()
