"""
Phase 2: Snowflake Migration Engine
Pharma Sales Territory Optimizer
Role: Data Warehouse Engineer

Uploads 1.44M records from final_pharma_enriched_data.csv.gz to Snowflake
Database: PHARMA_OS_DB | Schema: SALES_OPS
"""

import pandas as pd
import numpy as np
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import warnings
import os
from datetime import datetime

warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURATION - UPDATE YOUR CREDENTIALS HERE
# ============================================================
SNOWFLAKE_CONFIG = {
    'account': 'rwcfeut-wb78109',  # From: https://rwcfeut-wb78109.snowflakecomputing.com
    'user': 'SANDHIYABK',
    'password': 'k66T4jKv_LQDHXe',
    'warehouse': 'COMPUTE_WH',
    'database': 'PHARMA_OS_DB',
    'schema': 'SALES_OPS'
}

DATA_FILE = 'D:/pharma_project/final_pharma_enriched_data.csv.gz'
BATCH_SIZE = 100000

# ============================================================
# DDL STATEMENTS
# ============================================================
DDL_STATEMENTS = [
    "CREATE DATABASE IF NOT EXISTS PHARMA_OS_DB;",
    "CREATE SCHEMA IF NOT EXISTS PHARMA_OS_DB.SALES_OPS;",
    """CREATE TABLE IF NOT EXISTS PHARMA_OS_DB.SALES_OPS.DIM_DOCTORS (
        DOCTOR_ID VARCHAR(20) PRIMARY KEY,
        DOCTOR_NAME VARCHAR(100),
        SPECIALTY VARCHAR(50),
        LATITUDE FLOAT,
        LONGITUDE FLOAT,
        REGION VARCHAR(20),
        PRIMARY_CATEGORY VARCHAR(20),
        POTENTIAL_MULTIPLIER FLOAT,
        CREATED_DATE DATE DEFAULT CURRENT_DATE()
    );""",
    """CREATE TABLE IF NOT EXISTS PHARMA_OS_DB.SALES_OPS.FACT_SALES (
        TRANSACTION_ID VARCHAR(30) PRIMARY KEY,
        TRANSACTION_DATE DATE,
        YEAR INTEGER,
        MONTH VARCHAR(10),
        DOCTOR_ID VARCHAR(20),
        DRUG_CATEGORY VARCHAR(20),
        SPECIALTY VARCHAR(50),
        UNITS_SOLD INTEGER,
        REGION VARCHAR(20),
        TERRITORY_ID INTEGER,
        DOCTOR_POTENTIAL FLOAT,
        LATITUDE FLOAT,
        LONGITUDE FLOAT
    );""",
    """CREATE TABLE IF NOT EXISTS PHARMA_OS_DB.SALES_OPS.DIM_TERRITORY_ASSIGNMENTS (
        DOCTOR_ID VARCHAR(20),
        TERRITORY_ID INTEGER,
        START_DATE DATE,
        END_DATE DATE,
        IS_CURRENT BOOLEAN,
        PRIMARY KEY (DOCTOR_ID, START_DATE)
    );"""
]

# ============================================================
# CONNECTION FUNCTION
# ============================================================
def get_snowflake_connection(config):
    """Establish connection to Snowflake"""
    print("\n[CONNECTION] Connecting to Snowflake...")
    print(f"  Account: {config['account']}")
    print(f"  User: {config['user']}")
    print(f"  Warehouse: {config['warehouse']}")
    print(f"  Database: {config['database']}")
    print(f"  Schema: {config['schema']}")
    
    conn = snowflake.connector.connect(
        account=config['account'],
        user=config['user'],
        password=config['password'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    print("[CONNECTION] Connected successfully!")
    return conn

# ============================================================
# DDL EXECUTION
# ============================================================
def execute_ddl(conn):
    """Execute DDL statements to create database objects"""
    print("\n[DDL] Creating Database Objects...")
    cursor = conn.cursor()
    
    for ddl in DDL_STATEMENTS:
        stmt = ddl.strip()
        if stmt and not stmt.startswith('--'):
            try:
                cursor.execute(stmt)
                print(f"  [OK] {stmt[:60]}...")
            except Exception as e:
                print(f"  [WARN] {str(e)[:50]}...")
    
    cursor.close()
    print("[DDL] Database objects created/verified!")

# ============================================================
# DATA EXTRACTION
# ============================================================
def extract_data():
    """Extract and prepare data from CSV"""
    print("\n[EXTRACT] Loading data from CSV...")
    df = pd.read_csv(DATA_FILE, compression='gzip')
    print(f"  Loaded {len(df):,} records")
    return df

# ============================================================
# DIM_DOCTORS LOADING
# ============================================================
def load_dim_doctors(conn, sales_df):
    """Load DIM_DOCTORS dimension table"""
    print("\n[LOAD] Loading DIM_DOCTORS...")
    
    dim_doctors = sales_df.groupby('Doc_ID').agg({
        'Latitude': 'first',
        'Longitude': 'first',
        'Region': 'first',
        'Specialty': 'first',
        'Doctor_Potential': 'first'
    }).reset_index()
    
    dim_doctors.columns = ['DOCTOR_ID', 'LATITUDE', 'LONGITUDE', 'REGION', 'SPECIALTY', 'POTENTIAL_MULTIPLIER']
    dim_doctors['DOCTOR_NAME'] = dim_doctors['DOCTOR_ID'].apply(lambda x: f"Dr. {x}")
    
    dim_doctors = dim_doctors[['DOCTOR_ID', 'DOCTOR_NAME', 'SPECIALTY', 'LATITUDE', 'LONGITUDE', 'REGION', 'POTENTIAL_MULTIPLIER']]
    
    print(f"  Prepared {len(dim_doctors):,} doctor records")
    
    success, nchunks, nrows, _ = write_pandas(
        conn,
        dim_doctors,
        'DIM_DOCTORS',
        auto_create_table=True,
        overwrite=True
    )
    
    print(f"  [OK] Loaded {nrows:,} records in {nchunks} chunks")
    return success

# ============================================================
# FACT_SALES LOADING
# ============================================================
def load_fact_sales(conn, sales_df):
    """Load FACT_SALES fact table"""
    print("\n[LOAD] Loading FACT_SALES...")
    
    fact_sales = sales_df[[
        'Transaction_ID', 'Date', 'Year', 'Month', 'Doc_ID',
        'Drug_Category', 'Specialty', 'Sales_Volume', 'Region',
        'Territory_ID', 'Doctor_Potential', 'Latitude', 'Longitude'
    ]].copy()
    
    fact_sales.columns = [
        'TRANSACTION_ID', 'TRANSACTION_DATE', 'YEAR', 'MONTH', 'DOCTOR_ID',
        'DRUG_CATEGORY', 'SPECIALTY', 'UNITS_SOLD', 'REGION',
        'TERRITORY_ID', 'DOCTOR_POTENTIAL', 'LATITUDE', 'LONGITUDE'
    ]
    
    print(f"  Prepared {len(fact_sales):,} transaction records")
    
    success, nchunks, nrows, _ = write_pandas(
        conn,
        fact_sales,
        'FACT_SALES',
        auto_create_table=True,
        overwrite=True
    )
    
    print(f"  [OK] Loaded {nrows:,} records in {nchunks} chunks")
    return success

# ============================================================
# DIM_TERRITORY_ASSIGNMENTS (SCD2) LOADING
# ============================================================
def load_dim_territory_scd2(conn, sales_df):
    """Load DIM_TERRITORY_ASSIGNMENTS with SCD2 logic"""
    print("\n[LOAD] Loading DIM_TERRITORY_ASSIGNMENTS (SCD2)...")
    
    latest_assignments = sales_df.groupby('Doc_ID').agg({
        'Territory_ID': 'last'
    }).reset_index()
    
    scd2_records = pd.DataFrame({
        'DOCTOR_ID': latest_assignments['Doc_ID'],
        'TERRITORY_ID': latest_assignments['Territory_ID'],
        'START_DATE': '2025-01-01',
        'END_DATE': None,
        'IS_CURRENT': True
    })
    
    print(f"  Prepared {len(scd2_records):,} SCD2 records")
    print(f"  START_DATE: 2025-01-01, END_DATE: NULL, IS_CURRENT: TRUE")
    
    success, nchunks, nrows, _ = write_pandas(
        conn,
        scd2_records,
        'DIM_TERRITORY_ASSIGNMENTS',
        auto_create_table=True,
        overwrite=True
    )
    
    print(f"  [OK] Loaded {nrows:,} records in {nchunks} chunks")
    return success

# ============================================================
# VERIFICATION QUERIES
# ============================================================
def verify_data(conn):
    """Run verification queries"""
    print("\n[VERIFY] Running verification queries...")
    cursor = conn.cursor()
    
    queries = {
        'DIM_DOCTORS': 'SELECT COUNT(*) as CNT FROM DIM_DOCTORS',
        'FACT_SALES': 'SELECT COUNT(*) as CNT FROM FACT_SALES',
        'DIM_TERRITORY_ASSIGNMENTS': 'SELECT COUNT(*) as CNT FROM DIM_TERRITORY_ASSIGNMENTS',
        'TOTAL_SALES': 'SELECT SUM(UNITS_SOLD) as TOTAL FROM FACT_SALES',
        'TERRITORIES': 'SELECT COUNT(DISTINCT TERRITORY_ID) as TERRITORY_CNT FROM FACT_SALES',
        'SCD2_CHECK': "SELECT COUNT(*) as CURRENT_CNT FROM DIM_TERRITORY_ASSIGNMENTS WHERE IS_CURRENT = TRUE"
    }
    
    for name, query in queries.items():
        cursor.execute(query)
        result = cursor.fetchone()
        print(f"  {name}: {result[0]:,}")
    
    cursor.close()

# ============================================================
# MAIN EXECUTION
# ============================================================
def main():
    print("=" * 70)
    print("PHASE 2: SNOWFLAKE MIGRATION ENGINE")
    print("Pharma Sales Territory Optimizer - Data Warehouse")
    print("=" * 70)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Step 1: Execute DDL
        conn = get_snowflake_connection(SNOWFLAKE_CONFIG)
        execute_ddl(conn)
        
        # Step 2: Extract Data
        sales_df = extract_data()
        
        # Step 3: Load Dimensions and Facts
        load_dim_doctors(conn, sales_df)
        load_fact_sales(conn, sales_df)
        load_dim_territory_scd2(conn, sales_df)
        
        # Step 4: Verify
        verify_data(conn)
        
        conn.close()
        
        print("\n" + "=" * 70)
        print("MIGRATION COMPLETE!")
        print("=" * 70)
        print(f"\nDatabase: {SNOWFLAKE_CONFIG['database']}")
        print(f"Schema: {SNOWFLAKE_CONFIG['schema']}")
        print("\nTables Created:")
        print("  - DIM_DOCTORS (5,000 records)")
        print("  - FACT_SALES (1,440,000 records)")
        print("  - DIM_TERRITORY_ASSIGNMENTS (5,000 records - SCD2)")
        
    except Exception as e:
        print(f"\n[ERROR] Migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
