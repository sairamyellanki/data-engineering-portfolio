# 🏦 Enterprise Data Engineering & Financial Regulatory Systems

**Organization:** HDFC Bank | **Domain:** Banking & Financial Services (BFSI)  
**Location:** Mumbai, India | **Period:** October 2021 – Present  

---

## 📌 Executive Overview
Designed, optimized, and automated production-grade data pipelines supporting critical Reserve Bank of India (RBI) regulatory reporting mandates and core balance-sheet analytics. Key technical highlights include migrating legacy ETL architectures to scalable PySpark pipelines, optimizing large-scale SQL query execution, and enforcing rigorous data validation frameworks.

---

## 🚀 Key Project Portfolio

### 1. Financial Soundness Indicators (FSI) Automation Engine
* **Duration:** 2025 – Present
* **Core Technologies:** `PySpark`, `Apache Spark`, `Enterprise SQL`, `Regulatory Reporting`
* **Strategic Objective:** Build end-to-end data processing pipelines to calculate and validate regulatory capital ratios and balance-sheet health metrics.
* **Key Deliverables:**
  * **Scalable Data Architecture:** Engineered PySpark workflows to aggregate risk-weighted assets (RWA) and Tier-1 capital across operational subsidiaries.
  * **Data Integrity & Lineage:** Implemented strict schema validation rules and automated reconciliation checks to guarantee zero-variance submission accuracy under RBI guidelines.
  * **Performance Optimization:** Optimized partition strategy and memory usage for high-volume financial data processing.

---

### 2. Report on Operational Subsidiaries (ROOS) Data Pipeline
* **Duration:** 2025 – Present
* **Core Technologies:** `PySpark`, `Oracle SQL`, `Databricks`, `UNIX Shell Scripting`
* **Strategic Objective:** Streamline data ingestion, transformation, and consolidation for multi-entity subsidiary financial statements.
* **Key Deliverables:**
  * **Metadata-Driven Ingestion:** Designed modular ingestion patterns to harmonize disparate data sources from various operational units.
  * **ETL Standardization:** Replaced manual reconciliation workflows with automated validation rules, reducing quarterly processing cycles.
  * **Workflow Orchestration:** Integrated execution jobs into enterprise scheduling systems to ensure timely quarterly completion.

---

### 3. Interest Rate Sensitivity (IRS) Risk Analytics Pipeline
* **Duration:** Jan 2025 – Present
* **Core Technologies:** `PySpark`, `SQL`, `Apache Spark`, `Asset-Liability Management (ALM)`
* **Strategic Objective:** Develop automated pipelines for interest rate risk metrics and gap analysis modeling across balance-sheet accounts.
* **Key Deliverables:**
  * **Repricing Analysis Engine:** Built PySpark transformation logic to classify transactional and balance-sheet assets into designated repricing time buckets.
  * **Risk Metric Computation:** Automated calculations for weighted interest rate exposures and sensitivity gap metrics.
  * **Batch Efficiency:** Leveraged broadcast joins and dynamic partition pruning to process multi-million record datasets within established SLA windows.

---

### 4. Basic Statistical Returns II (BSR-II) Modernization & Migration
* **Duration:** Oct 2023 – Jan 2025
* **Core Technologies:** `PySpark`, `Informatica PowerCenter`, `UNIX Shell Scripting`, `Autosys`, `Oracle SQL`
* **Strategic Objective:** Migrate legacy Informatica PowerCenter data flows to PySpark and automate quarterly RBI BSR-II submissions.
* **Key Deliverables:**
  * **Legacy Framework Migration:** Formulated a structured conversion pattern to translate PowerCenter mappings into maintainable PySpark ETL jobs.
  * **Batch Execution Optimization:** Refactored SQL transformation logic and tuned Spark shuffle parameters, reducing quarterly execution runtime by ~35%.
  * **Enterprise Automation:** Built modular UNIX shell scripts and Autosys job flows for end-to-end automated execution.
  * **Technical Documentation:** Developed complete data lineage diagrams, mapping specifications, and standard operating procedures (SOPs).

---

### 5. Financial Inclusion Development Department (FIDD) Reporting Framework
* **Duration:** Oct 2021 – Oct 2023
* **Core Technologies:** `Oracle SQL`, `PL/SQL`, `UNIX Shell Scripting`, `Autosys`, `ETL Pipelines`
* **Strategic Objective:** Automate corporate-level data collection and transformation for mandatory RBI FIDD quarterly submissions.
* **Key Deliverables:**
  * **Multi-Layer Pipeline Development:** Engineered robust ETL jobs to extract, sanitize, and stage raw customer account data from core banking sources.
  * **Query Optimization & Database Tuning:** Diagnosed performance bottlenecks, restructured complex Oracle SQL queries, and optimized execution plans to reduce CPU and I/O overhead during peak processing runs.
  * **Production Support & Incident Management:** Supported SIT/UAT cycles, defect triage, and deployment management for mission-critical compliance pipelines.

---

## 📂 Code Sample Preview
To view a sanitized, production-grade PySpark ETL template featuring schema validation, broadcast joins, and regulatory metrics aggregation (IRS, ROOS, FSI), check out [`src/etl_regulatory_pipeline.py`](./src/etl_regulatory_pi peline.py).

---

> **Security & Compliance Note:**  
> *All code repositories, technical descriptions, and architectural diagrams contained herein are sanitized, generic representations designed for portfolio presentation. No proprietary bank code, internal server configurations, connection credentials, or real customer data (PII) are stored or disclosed in this repository.*
