# 🏥 Pharma Sales Territory Optimizer

**Live Demo:** https://pharmasalesterritoryoptimizer-iheracruaouep3jdy4ghad.streamlit.app/

[![Deploy Status](https://img.shields.io/badge/Streamlit-Deployed-brightgreen)](https://pharmasalesterritoryoptimizer-iheracruaouep3jdy4ghad.streamlit.app/)

A comprehensive end-to-end data engineering project for pharmaceutical sales territory optimization using Snowflake, Python, PuLP, and Streamlit.

## Project Overview

This project demonstrates scalable data engineering skills by:
1. Generating 1.44M synthetic sales records from 5,000 doctors
2. Migrating data to Snowflake cloud data warehouse
3. Performing ML-based doctor tiering with PySpark
4. Optimizing territory assignments using Operations Research (PuLP)
5. Building an interactive Streamlit dashboard for visualization

## Architecture

```
Phase 1          Phase 2          Phase 3          Phase 4          Phase 5
Data Synthesis → Snowflake Migration → Doctor Tiering → OR Optimization → Dashboard
   (Python)         (Snowflake)         (PySpark)         (PuLP)          (Streamlit)
```

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/sandhiyabk/Pharma_sales_territory_optimizer.git
cd Pharma_sales_territory_optimizer
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Snowflake Secrets
Create `.streamlit/secrets.toml`:
```toml
[secrets]
SF_ACCOUNT = "rwcfeut-wb78109"
SF_USER = "SANDHIYABK"
SF_PASSWORD = "your_password"
SF_WAREHOUSE = "COMPUTE_WH"
SF_DATABASE = "PHARMA_OS_DB"
SF_SCHEMA = "SALES_OPS"
```

### 4. Run Phases in Order
```bash
python phase1_synthesis.py
python phase2_snowflake_migration.py
python phase3_doctor_tiering.py
python phase4_territory_optimization.py
streamlit run phase5_dashboard.py
```

## Deployment

### Deploy to Streamlit Cloud (Free)

1. Push code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Click "Deploy an app"
4. Select your GitHub repo
5. Add secrets in the Advanced settings:
   - `SF_ACCOUNT` = your snowflake account
   - `SF_USER` = your username
   - `SF_PASSWORD` = your password
   - `SF_WAREHOUSE` = COMPUTE_WH
   - `SF_DATABASE` = PHARMA_OS_DB
   - `SF_SCHEMA` = SALES_OPS
6. Click Deploy!

### Deploy to Render (Free)

1. Create `render.yaml` with your app config
2. Connect GitHub repo
3. Add environment variables
4. Deploy

### 2. Update Credentials
Edit each phase file and update the Snowflake password:
```python
'password': 'YOUR_PASSWORD_HERE',
```

### 3. Run Phases in Order

**Phase 1: Data Synthesis** (Local)
```bash
python phase1_synthesis.py
```
- Generates 5,000 doctors using Faker
- Creates 1.44M sales transactions
- Applies K-Means clustering for 50 territories
- Output: `final_pharma_enriched_data.csv.gz`

**Phase 2: Snowflake Migration**
```bash
python phase2_snowflake_migration.py
```
- Creates PHARMA_OS_DB.SALES_OPS database/schema
- Loads DIM_DOCTORS, FACT_SALES, DIM_TERRITORY_ASSIGNMENTS
- Uses write_pandas for high-speed ingestion

**Phase 3: Doctor Tiering** (PySpark)
```bash
spark-submit --packages net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.4 \
    phase3_doctor_tiering.py
```
- Reads from Snowflake via Spark
- Calculates TARGETING_SCORE
- Applies ntile(10) for decile ranking
- Output: STG_DOCTOR_PRIORITY

**Phase 4: Territory Optimization**
```bash
python phase4_territory_optimization.py
```
- Builds PuLP optimization model
- Maximizes TARGETING_SCORE capture
- Enforces 80-120 workload constraints
- Output: FINAL_OPTIMIZED_TERRITORIES

**Phase 5: Streamlit Dashboard**
```bash
streamlit run phase5_dashboard.py --server.port 8501
```

## Key Metrics

| Metric | Value |
|--------|-------|
| Total Doctors | 5,000 |
| Transaction Records | 1,440,000 |
| Territories | 50 |
| Regions | Chennai + Madurai |
| Efficiency Gain | ~25% improvement |

## Database Schema

```
PHARMA_OS_DB.SALES_OPS
├── DIM_DOCTORS (5,000 records)
│   └── Doctor_ID, Name, Specialty, Location
├── FACT_SALES (1,440,000 records)
│   └── Transaction details with FK to DIM_DOCTORS
├── DIM_TERRITORY_ASSIGNMENTS (SCD2)
│   └── Historical territory assignments
├── STG_DOCTOR_PRIORITY
│   └── Tiering scores and decile ranks
└── FINAL_OPTIMIZED_TERRITORIES
    └── Optimized doctor-to-rep assignments
```

## The "ZS Pitch"

> "I built a scalable system using Snowflake and PySpark that optimized 5,000 doctor assignments across 50 territories, achieving a 25% simulated improvement in sales coverage efficiency."

## Project Files

| File | Description |
|------|-------------|
| `phase1_synthesis.py` | Data synthesis with Faker + K-Means |
| `phase2_snowflake_migration.py` | Snowflake DDL + batch loading |
| `phase2_ddl.sql` | Standalone DDL script |
| `phase3_doctor_tiering.py` | PySpark ML tiering |
| `phase4_territory_optimization.py` | PuLP OR optimization |
| `phase5_dashboard.py` | Streamlit visualization |
| `final_pharma_enriched_data.csv.gz` | Synthetic dataset |
| `territory_map.html` | Static map visualization |

## Dashboard Features

- **KPI Cards**: Total Score, Efficiency Gain, Platinum Coverage
- **Interactive Map**: 5,000 doctors plotted with tier colors
- **Filters**: Region, Tier, Specialty, Territory
- **Charts**: Workload distribution, Tier breakdown, Specialty performance
- **Data Table**: Sortable/filterable doctor assignments

## Technologies Used

| Category | Tools |
|----------|-------|
| Data Generation | Python, Faker, NumPy |
| Cloud Data Warehouse | Snowflake |
| Big Data Processing | PySpark |
| Optimization | PuLP (CBC Solver) |
| Visualization | Plotly, Folium |
| Dashboard | Streamlit |

## License

MIT License - Feel free to use for learning and portfolio purposes.
