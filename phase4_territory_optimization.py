"""
Phase 4: Territory Optimization Engine
Pharma Sales Territory Optimizer
Role: Operations Research (OR) Specialist

Optimizes doctor-to-rep assignments using PuLP.
"""

import pandas as pd
import numpy as np
import snowflake.connector
from pulp import (
    LpProblem, LpVariable, LpMaximize, LpBinary, 
    lpSum, LpStatus, value, PULP_CBC_CMD
)
import warnings
import time
from datetime import datetime

warnings.filterwarnings('ignore')

SNOWFLAKE_CONFIG = {
    'account': 'rwcfeut-wb78109',
    'user': 'SANDHIYABK',
    'password': 'k66T4jKv_LQDHXe',
    'warehouse': 'COMPUTE_WH',
    'database': 'PHARMA_OS_DB',
    'schema': 'SALES_OPS'
}

MIN_DOCTORS_PER_REP = 80
MAX_DOCTORS_PER_REP = 120
TARGET_TERRITORIES = 50

def load_data():
    print("\n[DATA] Loading data from Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    
    df = pd.read_sql("""
        SELECT 
            dp.DOCTOR_ID,
            dp.TARGETING_SCORE,
            dp.TIER,
            dd.REGION,
            dd.SPECIALTY
        FROM PHARMA_OS_DB.SALES_OPS.STG_DOCTOR_PRIORITY dp
        JOIN PHARMA_OS_DB.SALES_OPS.DIM_DOCTORS dd
        ON dp.DOCTOR_ID = dd.DOCTOR_ID
    """, conn)
    conn.close()
    
    print(f"  Loaded {len(df):,} doctor records")
    return df

def run_optimization(df):
    print("\n[OPTIMIZE] Building OR Model...")
    
    doctors = df['DOCTOR_ID'].tolist()
    doctor_scores = dict(zip(df['DOCTOR_ID'], df['TARGETING_SCORE']))
    doctor_regions = dict(zip(df['DOCTOR_ID'], df['REGION']))
    doctor_tiers = dict(zip(df['DOCTOR_ID'], df['TIER']))
    
    chennai_doctors = [d for d in doctors if doctor_regions.get(d) == 'Chennai']
    madurai_doctors = [d for d in doctors if doctor_regions.get(d) == 'Madurai']
    
    chennai_territories = [f"Chennai_{i}" for i in range(TARGET_TERRITORIES // 2)]
    madurai_territories = [f"Madurai_{i}" for i in range(TARGET_TERRITORIES - TARGET_TERRITORIES // 2)]
    all_territories = chennai_territories + madurai_territories
    
    print(f"  Doctors: {len(doctors)} (Chennai: {len(chennai_doctors)}, Madurai: {len(madurai_doctors)})")
    print(f"  Territories: {len(all_territories)}")
    
    prob = LpProblem("Pharma_Territory_Optimization", LpMaximize)
    
    x = LpVariable.dicts("assign", [(d, t) for d in doctors for t in all_territories], cat=LpBinary)
    y = LpVariable.dicts("active", all_territories, cat=LpBinary)
    
    prob += lpSum([doctor_scores[d] * x[(d, t)] for d in doctors for t in all_territories])
    
    for d in doctors:
        prob += lpSum([x[(d, t)] for t in all_territories]) == 1, f"Single_{d}"
    
    for t in all_territories:
        prob += lpSum([x[(d, t)] for d in doctors]) >= MIN_DOCTORS_PER_REP * y[t]
        prob += lpSum([x[(d, t)] for d in doctors]) <= MAX_DOCTORS_PER_REP * y[t]
    
    for d in chennai_doctors:
        for t in madurai_territories:
            prob += x[(d, t)] == 0
    
    for d in madurai_doctors:
        for t in chennai_territories:
            prob += x[(d, t)] == 0
    
    prob += lpSum([y[t] for t in all_territories]) == TARGET_TERRITORIES
    
    print("\n[SOLVE] Running CBC solver...")
    start = time.time()
    status = prob.solve(PULP_CBC_CMD(msg=0, timeLimit=300))
    print(f"  Solve time: {time.time() - start:.1f}s, Status: {LpStatus[status]}")
    
    assignments = []
    for d in doctors:
        for t in all_territories:
            if value(x[(d, t)]) == 1:
                assignments.append({'DOCTOR_ID': d, 'OPTIMIZED_TERRITORY': t, 'TIER': doctor_tiers[d], 'TARGETING_SCORE': doctor_scores[d]})
                break
    
    return pd.DataFrame(assignments)

def save_results(df):
    print("\n[SAVE] Saving to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS PHARMA_OS_DB.SALES_OPS.FINAL_OPTIMIZED_TERRITORIES")
    
    cursor.execute("""
        CREATE TABLE PHARMA_OS_DB.SALES_OPS.FINAL_OPTIMIZED_TERRITORIES (
            DOCTOR_ID VARCHAR(20),
            OPTIMIZED_TERRITORY VARCHAR(30),
            TIER VARCHAR(20),
            TARGETING_SCORE FLOAT
        )
    """)
    
    from snowflake.connector.pandas_tools import write_pandas
    write_pandas(conn, df, 'FINAL_OPTIMIZED_TERRITORIES', auto_create_table=False, overwrite=False)
    
    conn.close()
    print(f"  Saved {len(df):,} records")

def main():
    print("=" * 60)
    print("PHASE 4: TERRITORY OPTIMIZATION ENGINE")
    print("=" * 60)
    
    df = load_data()
    
    baseline_score = df['TARGETING_SCORE'].sum()
    print(f"\n[BASELINE] Phase 1 Score: {baseline_score:,.2f}")
    
    assignments_df = run_optimization(df)
    
    optimized_score = assignments_df['TARGETING_SCORE'].sum()
    efficiency_gain = ((optimized_score - baseline_score) / baseline_score) * 100
    
    print(f"\n[RESULT] Optimized Score: {optimized_score:,.2f}")
    print(f"[RESULT] Efficiency Gain: {efficiency_gain:+.2f}%")
    
    tier_summary = assignments_df.groupby('TIER').agg({'TARGETING_SCORE': ['sum', 'count']})
    print("\n[TIER COVERAGE]")
    print(tier_summary)
    
    workload = assignments_df.groupby('OPTIMIZED_TERRITORY').size()
    print(f"\n[WORKLOAD] Min: {workload.min()}, Max: {workload.max()}, Avg: {workload.mean():.1f}")
    
    save_results(assignments_df)
    
    print("\n" + "=" * 60)
    print("PHASE 4 COMPLETE!")
    print("=" * 60)

if __name__ == "__main__":
    main()
