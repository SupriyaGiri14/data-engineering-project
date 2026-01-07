
<h1 style="color:#c62828;">
End-to-End Data Engineering Project with Microsoft Fabric
</h1>

This project demonstrates a **fully functional end-to-end data engineering and analytics solution** using **Microsoft Fabric**, leveraging a **Medallion Architecture (Bronze → Silver → Gold)**.  
The pipeline automates data ingestion, transformation, and visualization, providing **business-ready insights**.

---

## 📂 Project Files

- [Project Report (PDF)](https://github.com/SupriyaGiri14/data-engineering-project/blob/main/End-to-End%20Data%20Engineering%20Project%20with%20Microsoft%20Fabric.pdf) – Detailed report with methodology, architecture, and results  
- Notebooks – PySpark notebooks for data transformation and pipeline processing
  - [Raw to Landing](https://github.com/SupriyaGiri14/data-engineering-project/blob/main/notebooks/Raw%20to%20Landing.ipynb)
  - [Landing to Bronze Transformation](https://github.com/SupriyaGiri14/data-engineering-project/blob/main/notebooks/Landing_to_Bronzetable.ipynb)
  - [Bronze to Silver Transformation](https://github.com/SupriyaGiri14/data-engineering-project/blob/main/notebooks/Silver_Transformation.ipynb)
  - [Silver to Gold Transformation](https://github.com/SupriyaGiri14/data-engineering-project/blob/main/notebooks/Silver%20to%20Gold%20transformation.ipynb)


---

## 🖼 Pipeline Image

![Pipeline Run](https://raw.githubusercontent.com/SupriyaGiri14/data-engineering-project/main/screenshots/pipeline_run1.png)
---
## End-to-End Pipeline Logic (Microsoft Fabric)

This project implements a fully automated end-to-end data pipeline in **Microsoft Fabric** to move data seamlessly from **Raw files to Gold-layer business tables** using a Medallion Architecture.

---

### 1. Raw to Landing Pipeline

**Purpose**  
- Detect newly arrived files in the Raw folder  
- Pass dynamic file names and processing dates to notebooks  
- Load incremental data into the Landing folder  

**Activities Used**
- Get Metadata
- ForEach
- Notebook Activity

**Pipeline Flow**
- Get Metadata reads the Raw folder and retrieves file details  
- ForEach iterates over each file  
- Notebook processes each file and writes data to the Landing folder  

**Parameters Passed to Notebook**

| `p_file_name` - Name of the incoming raw file |
| `p_processing_date` - Current processing date |

**Outcome**
- Latest monthly file is moved from Raw to Landing  
- Incremental data ingestion is maintained  

---

### 2. Landing to Bronze Pipeline

**Purpose**  
- Convert landing data into structured Delta tables  
- Persist raw historical data in Bronze layer  

**Activities Used**
- Notebook Activity

**Processing Logic**
- Read data from Landing folder  
- Enforce schema and data types  
- Add audit columns (processing date)  
- Write data as a Bronze Delta table  

**Outcome**
- Structured Bronze table created  
- Raw data preserved for traceability  

---

### 3. Bronze to Silver Pipeline

**Purpose**  
- Clean, validate, and enrich data  
- Apply business transformation rules  

**Activities Used**
- Notebook Activity

**Processing Logic**
- Remove duplicates  
- Handle null and invalid values  
- Create derived columns:
  - Delivery Date
  - Profit Margin
- Write cleaned data to Silver layer  

**Outcome**
- Clean and enriched Silver table  
- Data prepared for analytical modeling  

---

### 4. Silver to Gold Pipeline

**Purpose**  
- Create business-consumable datasets  
- Implement dimensional data modeling  

**Activities Used**
- Notebook Activity

**Processing Logic**
- Create Dimension Tables:
  - Dim_Customer
  - Dim_Product
  - Dim_Date
- Create Fact Table:
  - Fact_Sales
- Apply surrogate keys and aggregations  

**Outcome**
- Star schema created  
- Gold tables optimized for Power BI reporting  

---

### 5. End-to-End Orchestration Pipeline

**Purpose**  
- Automate complete data movement from Raw to Gold  
- Enable single-click execution  

**Pipeline Execution Order**
1. Execute Raw to Landing Pipeline  
2. Landing to Bronze Notebook  
3. Bronze to Silver Notebook  
4. Silver to Gold Notebook  

**Execution Flow**
Raw Folder
↓
Landing Folder
↓
Bronze Table
↓
Silver Table
↓
Gold Tables (Fact & Dimensions)
## 🚀 Highlights

- **Automated Data Pipelines:** Raw → Bronze → Silver → Gold layer using Microsoft Fabric  
- **Data Transformation:** PySpark notebooks for cleaning, enrichment, and business logic  
- **Business-Ready Insights:** Gold-layer tables feeding Power BI dashboards  
- **Data Quality & Governance:** Validation, reconciliation, and monitoring at each stage  
- **Scalability:** Designed to handle increasing data volumes efficiently  


---

Thank you for visiting!  

This project demonstrates how modern data engineering techniques can deliver **scalable, reliable, and actionable insights**.
