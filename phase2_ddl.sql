-- ============================================================
-- PHARMA_OS_DB: DDL Script
-- Database: PHARMA_OS_DB | Schema: SALES_OPS
-- Generated for Pharma Sales Territory Optimizer
-- ============================================================

-- Create Database
CREATE DATABASE IF NOT EXISTS PHARMA_OS_DB;

-- Create Schema
CREATE SCHEMA IF NOT EXISTS PHARMA_OS_DB.SALES_OPS;

-- Use the schema
USE DATABASE PHARMA_OS_DB;
USE SCHEMA SALES_OPS;

-- ============================================================
-- DIMENSION: DIM_DOCTORS
-- Slowly Changing Dimension - Type 1 (Overwrite on changes)
-- ============================================================
CREATE OR REPLACE TABLE PHARMA_OS_DB.SALES_OPS.DIM_DOCTORS (
    DOCTOR_ID VARCHAR(20) PRIMARY KEY,
    DOCTOR_NAME VARCHAR(100),
    SPECIALTY VARCHAR(50),
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    REGION VARCHAR(20),
    PRIMARY_CATEGORY VARCHAR(20),
    POTENTIAL_MULTIPLIER FLOAT,
    CREATED_DATE DATE DEFAULT CURRENT_DATE(),
    UPDATED_DATE DATE DEFAULT CURRENT_DATE()
);

-- ============================================================
-- FACT: FACT_SALES
-- Transactional fact table (grain: individual sales transactions)
-- ============================================================
CREATE OR REPLACE TABLE PHARMA_OS_DB.SALES_OPS.FACT_SALES (
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
);

-- Add foreign key reference
ALTER TABLE PHARMA_OS_DB.SALES_OPS.FACT_SALES
ADD CONSTRAINT fk_doctor
FOREIGN KEY (DOCTOR_ID) REFERENCES PHARMA_OS_DB.SALES_OPS.DIM_DOCTORS(DOCTOR_ID);

-- ============================================================
-- DIMENSION: DIM_TERRITORY_ASSIGNMENTS (SCD Type 2)
-- Slowly Changing Dimension - Type 2 (Track historical changes)
-- ============================================================
CREATE OR REPLACE TABLE PHARMA_OS_DB.SALES_OPS.DIM_TERRITORY_ASSIGNMENTS (
    DOCTOR_ID VARCHAR(20),
    TERRITORY_ID INTEGER,
    START_DATE DATE,
    END_DATE DATE,
    IS_CURRENT BOOLEAN,
    PRIMARY KEY (DOCTOR_ID, START_DATE)
);

-- Add foreign key reference
ALTER TABLE PHARMA_OS_DB.SALES_OPS.DIM_TERRITORY_ASSIGNMENTS
ADD CONSTRAINT fk_doctor_territory
FOREIGN KEY (DOCTOR_ID) REFERENCES PHARMA_OS_DB.SALES_OPS.DIM_DOCTORS(DOCTOR_ID);

-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================
CREATE OR REPLACE INDEX idx_fact_doctor ON FACT_SALES(DOCTOR_ID);
CREATE OR REPLACE INDEX idx_fact_date ON FACT_SALES(TRANSACTION_DATE);
CREATE OR REPLACE INDEX idx_fact_territory ON FACT_SALES(TERRITORY_ID);
CREATE OR REPLACE INDEX idx_fact_drug ON FACT_SALES(DRUG_CATEGORY);
CREATE OR REPLACE INDEX idx_dim_current ON DIM_TERRITORY_ASSIGNMENTS(IS_CURRENT);

-- ============================================================
-- SAMPLE QUERIES FOR VALIDATION
-- ============================================================

-- 1. Total sales by drug category
-- SELECT DRUG_CATEGORY, SUM(UNITS_SOLD) as TOTAL_SALES
-- FROM FACT_SALES
-- GROUP BY DRUG_CATEGORY
-- ORDER BY TOTAL_SALES DESC;

-- 2. Sales by territory
-- SELECT TERRITORY_ID, COUNT(*) as TRANSACTIONS, SUM(UNITS_SOLD) as TOTAL_SALES
-- FROM FACT_SALES
-- GROUP BY TERRITORY_ID
-- ORDER BY TERRITORY_ID;

-- 3. Doctor count per territory
-- SELECT TERRITORY_ID, COUNT(DISTINCT DOCTOR_ID) as DOCTOR_COUNT
-- FROM FACT_SALES
-- GROUP BY TERRITORY_ID
-- ORDER BY DOCTOR_COUNT DESC;

-- 4. SCD2 validation - check all doctors have current assignment
-- SELECT COUNT(*) as TOTAL_DOCTORS,
--        SUM(CASE WHEN IS_CURRENT = TRUE THEN 1 ELSE 0 END) as CURRENT_ASSIGNMENTS
-- FROM DIM_TERRITORY_ASSIGNMENTS;

-- 5. Regional analysis
-- SELECT REGION, COUNT(*) as TRANSACTIONS, SUM(UNITS_SOLD) as TOTAL_SALES
-- FROM FACT_SALES
-- GROUP BY REGION;
