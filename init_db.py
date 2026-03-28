import sqlite3
import pandas as pd
import os

def init_mock_db(db_path="argus.db"):
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    
    # Cases Table matching the new mockup including Geo-coordinates for st.map
    cases_df = pd.DataFrame({
        "ID": ["CASE-9928-MS", "CASE-4102-EB", "CASE-8831-ZK", "CASE-5510-LW", "CASE-2391-AH", "CASE-7742-GT", "CASE-1093-SH", "CASE-6621-NF"],
        "ENTITY_NAME": [
            "Marcus Sterling", "Elena Rostova", "Zheng-Kwan Holdings", 
            "Li Wei", "Aisha Al-Hashimi", "Global Tech Solutions", 
            "Santiago Herrera", "Nordic Finance AB"
        ],
        "TYPE": ["INDIVIDUAL", "INDIVIDUAL", "CORPORATE", "INDIVIDUAL", "INDIVIDUAL", "CORPORATE", "INDIVIDUAL", "CORPORATE"],
        "COUNTRY": [
            "United Kingdom", "EU (Multiple)", "Hong Kong", 
            "China", "UAE", "United States", 
            "Colombia", "Sweden"
        ],
        "FLAG_URL": [
            "1f1ec-1f1e7.png", # GB
            "1f1ea-1f1fa.png", # EU
            "1f1ed-1f1f0.png", # HK
            "1f1e8-1f1f3.png", # CN
            "1f1e6-1f1ea.png", # AE
            "1f1fa-1f1f8.png", # US
            "1f1e8-1f1f4.png", # CO
            "1f1f8-1f1ea.png"  # SE
        ],
        "RISK_SCORE": [88.0, 42.0, 12.0, 95.0, 65.0, 28.0, 82.0, 15.0],
        "AI_CONFIDENCE": ["94%", "88%", "99%", "91%", "85%", "96%", "82%", "98%"],
        "STATUS": [
            "Pending Review", "Investigation", "AUTO-CLEARED",
            "Pending Review", "Investigation", "AUTO-CLEARED",
            "Pending Review", "AUTO-CLEARED"
        ],
        "LAST_ACTIVITY": ["12 mins ago", "1 hour ago", "1 day ago", "5 mins ago", "2 hours ago", "4 hours ago", "10 mins ago", "1 day ago"],
        "CREATED_DATE": [
            "Oct 24, 2023", "Nov 12, 2023", "Jan 05, 2024",
            "Feb 10, 2024", "Mar 01, 2024", "Mar 15, 2024",
            "Mar 20, 2024", "Mar 22, 2024"
        ],
        "LAT": [51.5072, 51.1657, 22.3193, 39.9042, 25.2048, 40.7128, 4.6097, 59.3293],
        "LON": [-0.1276, 10.4515, 114.1694, 116.4074, 55.2708, -74.0060, -74.0817, 18.0686],
        "CLIENT": ["Porsche", "Circle K", "Pleo", "Swile", "Qred", "Mynt", "Allpay", "Indo"],
        "PRODUCT": [
            "Co-branded Credit Card", "Fleet Loyalty Card", 
            "Corporate Spend Card", "Employee Benefits Card",
            "SME Business Card", "Unified Spend Solution",
            "Prepaid Card Solution", "Buy Now Pay Later (BNPL)"
        ],
        "DOB": ["12 May 1984", "04 Nov 1962", "Est. 2005", "10 Feb 1990", "01 Mar 1975", "Est. 2015", "20 Mar 1980", "Est. 1995"],
        "AI_INSIGHT_TITLE": [
            "High Probability False Positive", "Medium Risk Sanctions Review", "Low Risk Pattern",
            "Critical PEP Match", "Anomalous Transaction Volume", "Corporate Clean Pattern",
            "Sanctions Evasion Flag", "Verified Institutional"
        ],
        "AI_INSIGHT_DESC": [
            "While the name matches a known PEP (Politically Exposed Person), the subject's date of birth (1988) deviates from the watchlist entity (1962). Action: Clear Hit.",
            "Entity name matches a newly added regional sanctions list. Action: Escalate.",
            "Corporate formation matches a broad string search but no direct UBO overlap. Action: Auto-Clear.",
            "Exact match on PEP database with aligning DOB and regional footprint. Action: Escalate immediately.",
            "Entity flagged for 400% surge in cross-border volume matching typical placement patterns. Action: Review.",
            "Standard institutional behavior verified against established baselines. Action: Auto-Clear.",
            "Transfers correlate heavily with documented sanctions evasion heuristics. Action: Block.",
            "Known partner institution utilizing standard settlement channels. Action: Auto-Clear."
        ]
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
    
    # Audit History Logs Table
    history_df = pd.DataFrame({
        "CASE_ID": [
            "CASE-9928-MS", "CASE-9928-MS", "CASE-9928-MS",
            "CASE-4102-EB", "CASE-4102-EB",
            "CASE-8831-ZK",
            "CASE-5510-LW", "CASE-5510-LW",
            "CASE-2391-AH",
            "CASE-7742-GT",
            "CASE-1093-SH", "CASE-1093-SH",
            "CASE-6621-NF"
        ],
        "SORT_ORDER": [3, 2, 1, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1],
        "TIMESTAMP": [
            "Today, 10:45 AM", "Yesterday, 02:15 PM", "Oct 24, 2023, 09:00 AM",
            "Nov 13, 09:00 AM", "Nov 12, 11:30 AM",
            "Jan 05, 08:30 AM",
            "Today, 09:12 AM", "Yesterday, 04:00 PM",
            "Mar 01, 10:00 AM",
            "Mar 15, 11:45 AM",
            "Mar 20, 08:30 AM", "Mar 19, 05:00 PM",
            "Mar 22, 09:00 AM"
        ],
        "TITLE": [
            "AI Scan triggered investigation", "User 'Julian Thome' requested ID upload", "Case Created",
            "Compliance Review", "AI Sanctions Screen",
            "Automated Review",
            "Critical Match Identified", "Initial Screening",
            "Volume Anomaly Flagged",
            "Routine Screening",
            "Evasion Heuristic Matched", "Account Opened",
            "Whitelisted Entity Pass"
        ],
        "DESCRIPTION": [
            "System automatically flagged potential PEP hit in restricted region.", "Customer was contacted to provide updated proof of residency.", "Initial onboarding screening executed.",
            "Awaiting secondary documentation.", "Flagged matching entity name registered in restricted zone.",
            "No immediate flags on initial pipeline screening.",
            "Confirmed exact alignment with regional restriction database.", "System evaluated target metrics.",
            "Surge in routing detected.",
            "Zero anomalies found.",
            "Transfers flagged matching restricted typologies.", "Customer profile initiated.",
            "Entity pre-verified via existing network API."
        ]
    })
    history_df.to_sql("decision_history", conn, index=False)
    
    # Collaborative Case Notes Table
    notes_df = pd.DataFrame({
        "CASE_ID": ["CASE-9928-MS", "CASE-4102-EB"],
        "AUTHOR": ["Sarah Jenkins (Analyst)", "System Auto-Check"],
        "TIMESTAMP": ["Yesterday, 03:00 PM", "Nov 12, 02:00 PM"],
        "NOTE_TEXT": [
            "Reached out to the internal compliance team for secondary passport verification. Awaiting response.",
            "Initial documentation appears digitally verified by trusted external partner. Bypassing escalated queues."
        ]
    })
    notes_df.to_sql("case_notes", conn, index=False)
    
    # Evidence Files Relational Table
    evidence_df = pd.DataFrame({
        "CASE_ID": [
            "CASE-9928-MS", "CASE-9928-MS",
            "CASE-4102-EB",
            "CASE-8831-ZK", "CASE-8831-ZK",
            "CASE-5510-LW",
            "CASE-2391-AH",
            "CASE-7742-GT", "CASE-7742-GT",
            "CASE-1093-SH",
            "CASE-6621-NF"
        ],
        "FILE_NAME": [
            "passport_scan.jpg", "utility_bill.pdf",
            "incorporation_doc.pdf",
            "registry_extract.pdf", "kyb_submission.pdf",
            "id_front.jpg",
            "source_of_wealth.pdf",
            "w9_form.pdf", "directors_list.csv",
            "residency_permit.pdf",
            "annual_report.pdf"
        ],
        "UPLOAD_DATE": [
            "Uploaded 2 days ago", "Uploaded 2 days ago",
            "Uploaded 5 days ago",
            "Uploaded 1 month ago", "Uploaded 1 month ago",
            "Uploaded 1 hr ago",
            "Uploaded 10 days ago",
            "Uploaded 2 months ago", "Uploaded 2 months ago",
            "Uploaded 4 hrs ago",
            "Uploaded 1 year ago"
        ]
    })
    evidence_df.to_sql("evidence_files", conn, index=False)

    # Watchlist Hits Entity Matching Table
    watchlist_hits_df = pd.DataFrame({
        "CASE_ID": [
            "CASE-9928-MS", "CASE-9928-MS", "CASE-9928-MS",
            "CASE-4102-EB", "CASE-4102-EB",
            "CASE-8831-ZK",
            "CASE-5510-LW", "CASE-5510-LW"
        ],
        "ATTRIBUTE": [
            "Full Name", "Date of Birth", "Nationality",
            "Full Name", "Date of Birth",
            "Corporate Name",
            "Full Name", "Nationality"
        ],
        "SUBJECT_DATA": [
            "Marcus Sterling", "12 May 1988", "British",
            "Elena Rostova", "04 Nov 1962",
            "Zheng-Kwan Holdings",
            "Li Wei", "Chinese"
        ],
        "WATCHLIST_DATA": [
            "M. Sterling", "04 Nov 1962", "British",
            "Elena Rostova", "04 Nov 1962",
            "Zheng-Kwan Holdings",
            "Li Wei", "Chinese"
        ],
        "MATCH_STATUS": [
            "MATCH", "MISMATCH", "PARTIAL",
            "MATCH", "MATCH",
            "MATCH",
            "MATCH", "MATCH"
        ]
    })
    watchlist_hits_df.to_sql("watchlist_hits", conn, index=False)
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {db_path} with mock data.")

if __name__ == "__main__":
    init_mock_db()
