"""
Phase 3: Doctor Tiering Analytics
Pharma Sales Territory Optimizer
Role: Data Engineer

Performs Doctor Tiering analysis:
- Reads FACT_SALES and DIM_DOCTORS from Snowflake
- Calculates Targeting_Score with normalization
- Applies Specialty_Premium multipliers
- Uses ntile for decile ranking
- Writes to STG_DOCTOR_PRIORITY table
"""

import pandas as pd
import numpy as np
import snowflake.connector
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURATION
# ============================================================
SNOWFLAKE_CONFIG = {
    'account': 'chizcdk-zm51873',
    'user': 'SANDHIYABK',
    'password': '9jcwpx9kGwfyAC6',
    'warehouse': 'COMPUTE_WH',
    'database': 'PHARMA_OS_DB',
    'schema': 'SALES_OPS'
}

OUTPUT_TABLE = 'STG_DOCTOR_PRIORITY'

SPECIALTY_PREMIUM = {
    'General Medicine': 1.0,
    'Orthopedics': 1.15,
    'Rheumatology': 1.2,
    'Neurology': 1.25,
    'Psychiatry': 1.1,
    'Pulmonology': 1.15,
    'Dermatology': 1.05
}

# ============================================================
# DATA LOADING
# ============================================================
def load_from_snowflake(query):
    """Load data from Snowflake"""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def load_data():
    """Load FACT_SALES and DIM_DOCTORS from Snowflake"""
    print("\n[LOAD] Loading FACT_SALES...")
    fact_sales = load_from_snowflake("""
        SELECT DOCTOR_ID, DRUG_CATEGORY, UNITS_SOLD, SPECIALTY, REGION
        FROM PHARMA_OS_DB.SALES_OPS.FACT_SALES
    """)
    print(f"  Loaded {len(fact_sales):,} sales records")
    
    print("\n[LOAD] Loading DIM_DOCTORS...")
    dim_doctors = load_from_snowflake("""
        SELECT DOCTOR_ID, DOCTOR_NAME, SPECIALTY, REGION, POTENTIAL_MULTIPLIER
        FROM PHARMA_OS_DB.SALES_OPS.DIM_DOCTORS
    """)
    print(f"  Loaded {len(dim_doctors):,} doctor records")
    
    return fact_sales, dim_doctors

# ============================================================
# AGGREGATE SALES BY DOCTOR
# ============================================================
def aggregate_doctor_sales(fact_sales):
    """Aggregate sales metrics by Doctor_ID"""
    print("\n[AGGREGATE] Calculating sales metrics by doctor...")
    
    doctor_sales = fact_sales.groupby('DOCTOR_ID').agg({
        'UNITS_SOLD': ['sum', 'count', 'mean', 'max'],
        'DRUG_CATEGORY': 'nunique'
    }).reset_index()
    
    doctor_sales.columns = ['DOCTOR_ID', 'TOTAL_SALES', 'TRANSACTION_COUNT', 
                            'AVG_TRANSACTION_VALUE', 'MAX_SINGLE_SALE', 'DRUG_DIVERSITY']
    
    print(f"  Aggregated {len(doctor_sales):,} doctors")
    
    return doctor_sales

# ============================================================
# FEATURE ENGINEERING
# ============================================================
def calculate_targeting_score(doctor_sales, dim_doctors):
    """Calculate Targeting_Score with normalization and specialty premium"""
    print("\n[FEATURE] Engineering Targeting Score...")
    
    df = doctor_sales.merge(dim_doctors, on='DOCTOR_ID', how='left')
    
    total_min = df['TOTAL_SALES'].min()
    total_max = df['TOTAL_SALES'].max()
    
    df['SALES_NORMALIZED'] = (df['TOTAL_SALES'] - total_min) / (total_max - total_min)
    
    df['SPECIALTY_PREMIUM'] = df['SPECIALTY'].map(SPECIALTY_PREMIUM).fillna(1.0)
    
    df['TARGETING_SCORE'] = (
        df['SALES_NORMALIZED'] * 
        df['SPECIALTY_PREMIUM'] * 
        df['POTENTIAL_MULTIPLIER'] * 100
    ).round(2)
    
    print(f"  Score range: {df['TARGETING_SCORE'].min():.2f} - {df['TARGETING_SCORE'].max():.2f}")
    
    return df

# ============================================================
# DECILE RANKING
# ============================================================
def apply_decile_ranking(df):
    """Apply ntile-like decile ranking"""
    print("\n[RANK] Applying decile ranking...")
    
    df['DECILE'] = pd.qcut(df['TARGETING_SCORE'], q=10, labels=False, duplicates='drop') + 1
    
    df['TIER'] = df['DECILE'].apply(lambda x: 
        'Platinum' if x <= 2 else 
        'Gold' if x <= 4 else 
        'Silver' if x <= 6 else 
        'Bronze'
    )
    
    df['OVERALL_RANK'] = df['TARGETING_SCORE'].rank(ascending=False, method='dense').astype(int)
    
    df['RANK_IN_DECILE'] = df.groupby('DECILE')['TARGETING_SCORE'].rank(ascending=False, method='dense').astype(int)
    
    return df

# ============================================================
# PREPARE OUTPUT
# ============================================================
def prepare_output(df):
    """Select columns for output table"""
    print("\n[PREP] Preparing output DataFrame...")
    
    output_df = df[[
        'DOCTOR_ID', 'SPECIALTY', 'REGION', 'TOTAL_SALES', 'TRANSACTION_COUNT',
        'AVG_TRANSACTION_VALUE', 'MAX_SINGLE_SALE', 'DRUG_DIVERSITY',
        'SALES_NORMALIZED', 'SPECIALTY_PREMIUM', 'POTENTIAL_MULTIPLIER',
        'TARGETING_SCORE', 'DECILE', 'RANK_IN_DECILE', 'TIER', 'OVERALL_RANK'
    ]].copy()
    
    output_df['SALES_NORMALIZED'] = output_df['SALES_NORMALIZED'].round(4)
    output_df['SPECIALTY_PREMIUM'] = output_df['SPECIALTY_PREMIUM'].round(2)
    output_df['POTENTIAL_MULTIPLIER'] = output_df['POTENTIAL_MULTIPLIER'].round(3)
    
    print(f"  Output columns: {list(output_df.columns)}")
    
    return output_df

# ============================================================
# WRITE TO SNOWFLAKE
# ============================================================
def write_to_snowflake(df, table_name):
    """Write DataFrame to Snowflake using write_pandas"""
    print(f"\n[WRITE] Writing to {table_name}...")
    
    from snowflake.connector.pandas_tools import write_pandas
    
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=True,
        overwrite=True
    )
    
    conn.close()
    
    print(f"  Wrote {nrows:,} records to {table_name}")

# ============================================================
# ANALYTICS SUMMARY
# ============================================================
def print_summary(df):
    """Print analytics summary"""
    print("\n" + "=" * 70)
    print("ANALYTICS SUMMARY")
    print("=" * 70)
    
    print("\n[BY TIER]")
    tier_summary = df.groupby('TIER').agg({
        'DOCTOR_ID': 'count',
        'TARGETING_SCORE': ['sum', 'mean']
    })
    tier_summary.columns = ['COUNT', 'TOTAL_SCORE', 'AVG_SCORE']
    tier_summary = tier_summary.reindex(['Platinum', 'Gold', 'Silver', 'Bronze'])
    print(tier_summary.to_string())
    
    print("\n[BY SPECIALTY]")
    specialty_summary = df.groupby('SPECIALTY').agg({
        'DOCTOR_ID': 'count',
        'TARGETING_SCORE': 'sum'
    }).sort_values('TARGETING_SCORE', ascending=False)
    print(specialty_summary.to_string())
    
    print("\n[TOP 10 PLATINUM DOCTORS]")
    top_platinum = df[df['TIER'] == 'Platinum'].nlargest(10, 'TARGETING_SCORE')
    print(top_platinum[['DOCTOR_ID', 'SPECIALTY', 'REGION', 'TOTAL_SALES', 'TARGETING_SCORE']].to_string(index=False))
    
    print("\n[DECILE DISTRIBUTION]")
    decile_dist = df.groupby(['DECILE', 'TIER']).size().unstack(fill_value=0)
    print(decile_dist.to_string())
    
    print("=" * 70)

# ============================================================
# MAIN EXECUTION
# ============================================================
def main():
    print("=" * 70)
    print("PHASE 3: DOCTOR TIERING ANALYTICS")
    print("Pharma Sales Territory Optimizer")
    print("=" * 70)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        fact_sales, dim_doctors = load_data()
        
        doctor_agg = aggregate_doctor_sales(fact_sales)
        
        df_scored = calculate_targeting_score(doctor_agg, dim_doctors)
        
        df_ranked = apply_decile_ranking(df_scored)
        
        df_output = prepare_output(df_ranked)
        
        print_summary(df_output)
        
        write_to_snowflake(df_output, OUTPUT_TABLE)
        
        print("\n" + "=" * 70)
        print("PHASE 3 COMPLETE!")
        print(f"Output Table: {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{OUTPUT_TABLE}")
        print(f"Total Doctors Ranked: {len(df_output):,}")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n[ERROR] Doctor Tiering failed: {e}")
        raise

if __name__ == "__main__":
    main()
