# Pharma Sales Territory Optimizer - Setup Script

# Install dependencies
pip install -r requirements.txt

# For local development, create .streamlit/secrets.toml:
# [secrets]
# SF_ACCOUNT = "your_account"
# SF_USER = "your_username"
# SF_PASSWORD = "your_password"
# SF_WAREHOUSE = "COMPUTE_WH"
# SF_DATABASE = "PHARMA_OS_DB"
# SF_SCHEMA = "SALES_OPS"

# Run phases
python phase1_synthesis.py
python phase2_snowflake_migration.py
python phase3_doctor_tiering.py
python phase4_territory_optimization.py

# Launch dashboard
streamlit run phase5_dashboard.py
