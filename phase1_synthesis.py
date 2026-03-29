"""
Phase 1: Advanced Hybrid Data Synthesis
Pharma Sales Territory Optimizer
"""

import pandas as pd
import numpy as np
from faker import Faker
from sklearn.cluster import KMeans
import plotly.express as px
import plotly.graph_objects as go
import warnings
import os
warnings.filterwarnings('ignore')

fake = Faker()
Faker.seed(42)
np.random.seed(42)

print("=" * 60)
print("PHASE 1: ADVANCED HYBRID DATA SYNTHESIS")
print("Pharma Sales Territory Optimizer")
print("=" * 60)

# STEP 1: Load and Extract Market Demand Baseline
print("\n[STEP 1] Loading salesmonthly.csv and extracting Market Demand baseline...")
sales_monthly = pd.read_csv('D:/pharma_project/data/salesmonthly.csv')
drug_columns = ['M01AB', 'M01AE', 'N02BA', 'N02BE', 'N05B', 'N05C', 'R03', 'R06']

monthly_avg = {}
for col in drug_columns:
    monthly_avg[col] = sales_monthly[sales_monthly[col] > 0][col].mean()
    print(f"  {col}: {monthly_avg[col]:.2f}")

print(f"\n  Total Market Avg: {sum(monthly_avg.values()):.2f}")

# STEP 2: Synthesize Doctor Master (5,000 Doctors)
print("\n[STEP 2] Generating 5,000 Doctors using Faker...")

specialty_mapping = {
    'M01AB': 'Orthopedics',
    'M01AE': 'Rheumatology', 
    'N02BA': 'Neurology',
    'N02BE': 'General Medicine',
    'N05B': 'Psychiatry',
    'N05C': 'Psychiatry',
    'R03': 'Pulmonology',
    'R06': 'Dermatology'
}

chennai_center = (13.0827, 80.2707)
madurai_center = (9.9252, 78.1198)

doctors_data = []
for i in range(5000):
    if i < 2500:
        lat = np.random.normal(chennai_center[0], 0.5)
        lon = np.random.normal(chennai_center[1], 0.5)
        region = 'Chennai'
    else:
        lat = np.random.normal(madurai_center[0], 0.4)
        lon = np.random.normal(madurai_center[1], 0.4)
        region = 'Madurai'
    
    primary_specialty = np.random.choice(drug_columns)
    doctor = {
        'Doc_ID': f'DOC{i+1:05d}',
        'Name': fake.name(),
        'Specialty': specialty_mapping[primary_specialty],
        'Primary_Category': primary_specialty,
        'Latitude': lat,
        'Longitude': lon,
        'Region': region,
        'Doctor_Potential_Multiplier': round(np.random.uniform(0.7, 2.5), 3)
    }
    doctors_data.append(doctor)

doctors_df = pd.DataFrame(doctors_data)
print(f"  Generated {len(doctors_df)} doctors")
print(f"  Chennai: {len(doctors_df[doctors_df['Region']=='Chennai'])}")
print(f"  Madurai: {len(doctors_df[doctors_df['Region']=='Madurai'])}")

# STEP 4: K-Means Clustering for Territory Assignment (BEFORE transaction generation)
print("\n[STEP 3] Applying K-Means Clustering to create 50 territories...")

coords = doctors_df[['Latitude', 'Longitude']].values
kmeans = KMeans(n_clusters=50, random_state=42, n_init=10)
doctors_df['Territory_ID'] = kmeans.fit_predict(coords) + 1

territory_centers = pd.DataFrame(kmeans.cluster_centers_, columns=['Center_Lat', 'Center_Lon'])
territory_centers['Territory_ID'] = range(1, 51)

print(f"  Created {doctors_df['Territory_ID'].nunique()} territories")
territory_sizes = doctors_df.groupby('Territory_ID').size()
print(f"  Avg doctors per territory: {territory_sizes.mean():.1f}")
print(f"  Min: {territory_sizes.min()}, Max: {territory_sizes.max()}")

# STEP 3: Generate Transactional Scale (1M+ records)
print("\n[STEP 4] Generating 1,440,000 sales transactions...")

months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
years = [2019, 2020, 2021]

# Create doctor lookup for faster access
doctor_lookup = doctors_df.set_index('Doc_ID').to_dict('index')

transaction_records = []
total_records = 0

for doc_id, doctor_info in doctor_lookup.items():
    territory_id = doctor_info['Territory_ID']
    potential = doctor_info['Doctor_Potential_Multiplier']
    lat = doctor_info['Latitude']
    lon = doctor_info['Longitude']
    region = doctor_info['Region']
    specialty = doctor_info['Specialty']
    
    for year in years:
        for month_idx, month in enumerate(months):
            base_date = f"{year}-{month_idx+1:02d}-15"
            
            for drug_col in drug_columns:
                base_sales = monthly_avg[drug_col] / len(drug_columns)
                seasonal_factor = np.random.uniform(0.8, 1.2)
                sales_volume = int(base_sales * potential * seasonal_factor)
                
                record = {
                    'Transaction_ID': f'TXN{str(total_records+1).zfill(10)}',
                    'Doc_ID': doc_id,
                    'Date': base_date,
                    'Year': year,
                    'Month': month,
                    'Drug_Category': drug_col,
                    'Specialty': specialty,
                    'Latitude': lat,
                    'Longitude': lon,
                    'Region': region,
                    'Territory_ID': territory_id,
                    'Sales_Volume': max(0, sales_volume),
                    'Doctor_Potential': potential
                }
                transaction_records.append(record)
                total_records += 1

transactions_df = pd.DataFrame(transaction_records)
print(f"  Generated {len(transactions_df):,} transaction records")

# STEP 5: Data Quality Check
print("\n[STEP 5] Data Quality Checks...")

null_checks = {
    'Null Latitude': transactions_df['Latitude'].isnull().sum(),
    'Null Longitude': transactions_df['Longitude'].isnull().sum(),
    'Null Territory': transactions_df['Territory_ID'].isnull().sum(),
    'Null Sales Volume': transactions_df['Sales_Volume'].isnull().sum()
}

for check, count in null_checks.items():
    status = "PASS" if count == 0 else f"FAIL ({count} nulls)"
    print(f"  {check}: {status}")

expected_total = sum(monthly_avg.values()) * 5000 * 3 * 1.6
actual_total = transactions_df['Sales_Volume'].sum()
variance = abs(actual_total - expected_total) / expected_total * 100
print(f"\n  Sales Volume Alignment Check:")
print(f"    Expected (estimated): {expected_total:,.0f}")
print(f"    Actual: {actual_total:,.0f}")
print(f"    Variance: {variance:.1f}%")

# STEP 6: Export Final Dataset
print("\n[STEP 6] Exporting final_pharma_enriched_data.csv.gz...")

output_path = 'D:/pharma_project/final_pharma_enriched_data.csv.gz'
transactions_df.to_csv(output_path, index=False, compression='gzip')

file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
print(f"  Exported: {output_path}")
print(f"  File size: {file_size_mb:.2f} MB")
print(f"  Records: {len(transactions_df):,}")

# STEP 7: Generate Map Visualization
print("\n[STEP 7] Creating territory visualization map...")

fig = px.scatter_mapbox(
    doctors_df,
    lat='Latitude',
    lon='Longitude',
    color='Territory_ID',
    hover_name='Doc_ID',
    hover_data={'Specialty': True, 'Region': True, 'Doctor_Potential_Multiplier': True},
    title='50 Pharma Sales Territories - Chennai & Madurai Region',
    mapbox_style='carto-positron',
    zoom=6,
    center={'lat': 11.5, 'lon': 79.2},
    color_continuous_scale=px.colors.qualitative.Set3
)

fig.update_layout(
    height=800,
    width=1200,
    legend_title_text='Territory ID'
)

map_path = 'D:/pharma_project/territory_map.html'
fig.write_html(map_path)
print(f"  Interactive map saved: {map_path}")

# Create static version
static_map_path = 'D:/pharma_project/territory_map.png'
try:
    fig.write_image(static_map_path, width=1400, height=900, scale=2)
    print(f"  Static map saved: {static_map_path}")
except Exception as e:
    print(f"  Static image generation skipped: {str(e)[:60]}")

# Summary Statistics
print("\n" + "=" * 60)
print("SUMMARY STATISTICS")
print("=" * 60)
print(f"Total Doctors: {len(doctors_df):,}")
print(f"Total Transactions: {len(transactions_df):,}")
print(f"Total Territories: {doctors_df['Territory_ID'].nunique()}")
print(f"Total Sales Volume: {transactions_df['Sales_Volume'].sum():,.0f}")
print(f"Output File: {output_path}")
print("=" * 60)
print("PHASE 1 COMPLETE!")
print("=" * 60)
