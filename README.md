# AUCA Big Data Analytics Final Project

**Distributed Multi-Model Analytics for E-commerce Data**  
Technologies: **MongoDB** + **HBase** + **Apache Spark**  
Due: **11:59am, 11-11-2026** (individual project)

This repository is a complete, runnable implementation of all four required parts, plus a technical report draft and presentation guide.

---

## 1. What this project does (big picture)

```text
dataset_generator
      │
      ▼
 JSON files (users, products, categories, sessions, transactions)
      │
      ├──────────────► MongoDB  → catalog / users / transactions + aggregations
      │
      ├──────────────► HBase    → time-series sessions + product daily metrics
      │
      └──────────────► Spark    → cleaning, recommendations, SQL, integrated analytics
                                    │
                                    ▼
                              Visualizations + Report insights
```

| Part | Weight | What we built |
|---|---|---|
| **Part 1** Data modeling & storage | 25% | MongoDB schemas/load/aggregations + HBase schemas/load/queries |
| **Part 2** Spark processing | 30% | Cleaning, also-bought / viewed-together, Spark SQL, optional cohort |
| **Part 3** Integration | 25% | Funnel conversion + CLV across Mongo-style + HBase-style data via Spark |
| **Part 4** Visualization & insights | 20% | 4 static charts for report/presentation |

---

## 2. Project structure

```text
finalproject/
├── config.py                      # DEMO/FULL sizes, ports, limits
├── docker-compose.yml             # MongoDB + HBase
├── requirements.txt
├── dataset_generator.py           # original provided notebook/script (reference)
├── 01_data_generation/
│   └── generate_dataset.py        # runnable generator (DEMO/FULL)
├── 02_mongodb/
│   ├── schema_design.md
│   ├── load_mongodb.py
│   └── aggregations.py
├── 03_hbase/
│   ├── schema_design.md
│   ├── hbase_utils.py
│   ├── create_tables.py
│   ├── load_hbase.py
│   └── queries.py
├── 04_spark/
│   ├── spark_common.py
│   ├── batch_cleaning.py
│   ├── recommendations.py
│   ├── cohort_analysis.py
│   └── spark_sql_analytics.py
├── 05_integration/
│   └── integrated_analytics.py
├── 06_visualization/
│   └── create_visualizations.py
├── 07_report/
│   ├── Technical_Report.md
│   └── PRESENTATION_GUIDE.md
├── scripts/
│   └── run_pipeline.py
├── data/                          # generated JSON (gitignored)
└── outputs/
    ├── figures/                   # charts for report
    └── results/                   # JSON/parquet results
```

---

## 3. Prerequisites (your machine)

Already verified / expected:

- Python 3.10+ (you have 3.12)
- Java 17 (required by PySpark)
- Docker Desktop running (for MongoDB + HBase)

Install Python packages:

```powershell
cd "d:\AUCA\Big Data Analytics\exam\finalproject"
pip install -r requirements.txt
```

---

## 4. Step-by-step: run everything (recommended order)

### STEP A — Start databases

```powershell
cd "d:\AUCA\Big Data Analytics\exam\finalproject"
docker compose up -d
```

Wait ~1–2 minutes for HBase Thrift (`localhost:9090`) to become ready.  
Check:

```powershell
docker ps
```

You should see `auca_mongo` and `auca_hbase`.

Mongo UI not required. HBase Master UI: http://localhost:16010

---

### STEP B — Generate dataset

**Use DEMO first** (fast, good for development + presentation).  
FULL matches the assignment sizes (very heavy: ~2M sessions).

```powershell
python 01_data_generation/generate_dataset.py --mode DEMO
```

Outputs land in `data/`:

- `users.json`, `products.json`, `categories.json`, `transactions.json`
- `sessions_0.json` (and more chunks in FULL)
- `dataset_meta.json`

To switch default mode later, edit `DATASET_MODE` in `config.py`.

---

### STEP C — Part 1: MongoDB

```powershell
python 02_mongodb/load_mongodb.py
python 02_mongodb/aggregations.py
```

Results:

- `outputs/results/mongo_revenue_by_category.json`
- `outputs/results/mongo_user_segmentation.json`
- `outputs/results/mongo_top_selling_products.json`

Read the design rationale in `02_mongodb/schema_design.md`.

---

### STEP D — Part 1: HBase

```powershell
python 03_hbase/create_tables.py
python 03_hbase/load_hbase.py
python 03_hbase/queries.py
```

Results:

- `outputs/results/hbase_user_sessions.json`
- `outputs/results/hbase_product_metrics.json`
- `outputs/results/hbase_shell_examples.txt`

Design notes: `03_hbase/schema_design.md`.

---

### STEP E — Part 2: Spark

```powershell
python 04_spark/batch_cleaning.py
python 04_spark/recommendations.py
python 04_spark/spark_sql_analytics.py
# optional alternative analysis:
python 04_spark/cohort_analysis.py
```

Results under `outputs/results/` (`spark_*.json`, cleaned parquet).

---

### STEP F — Part 3: Integrated analytics

```powershell
python 05_integration/integrated_analytics.py
```

Produces funnel + CLV in `outputs/results/integrated_analytics.json`.

---

### STEP G — Part 4: Visualizations

```powershell
python 06_visualization/create_visualizations.py
```

Figures:

1. `outputs/figures/01_revenue_by_category.png`
2. `outputs/figures/02_user_segments.png`
3. `outputs/figures/03_conversion_funnel.png`
4. `outputs/figures/04_clv_tiers.png`

---

### One-command pipeline (after Docker is up)

```powershell
python scripts/run_pipeline.py --all --mode DEMO
```

Or selective:

```powershell
python scripts/run_pipeline.py --steps generate,spark_clean,spark_rec,spark_sql,integrate,viz --mode DEMO
```

---

## 5. Deliverables checklist (from the PDF)

### GitHub repository
- [x] Dataset generator
- [x] MongoDB load + aggregations
- [x] HBase schema/load/queries
- [x] Spark applications
- [x] Integration scripts
- [x] Visualization scripts
- [x] README with reproduction steps

### Technical report (PDF, 6–8 pages)
- Draft markdown: `07_report/Technical_Report.md`
- Convert to PDF (Word / Pandoc / VS Code PDF) and embed the 4 figures
- Presentation talking points: `07_report/PRESENTATION_GUIDE.md`

---

## 6. DEMO vs FULL

| Setting | Users | Products | Sessions | Transactions | When to use |
|---|---:|---:|---:|---:|---|
| **DEMO** (default) | 2,000 | 800 | 40,000 | 15,000 | Develop, demo, present |
| **FULL** | 10,000 | 5,000 | 2,000,000 | 500,000 | Final heavy run if machine allows |

Mongo/HBase still load representative subsets via `MONGO_LOAD_LIMITS` / `HBASE_LOAD_LIMITS` in `config.py` (allowed by the assignment).

---

## 7. How to present (short)

1. Show architecture diagram (Mongo vs HBase vs Spark).  
2. Justify schemas from `schema_design.md` files.  
3. Live or recorded: one Mongo aggregation + one HBase user scan.  
4. Show Spark recommendation pairs + SQL join across “Mongo” and “HBase” views.  
5. Walk funnel + CLV insights with the 4 charts.  
6. Close with scalability + limitations from the report.

Full script: `07_report/PRESENTATION_GUIDE.md`.

---

## 8. Common issues

| Problem | Fix |
|---|---|
| `pymongo` connection refused | `docker compose up -d` and wait for Mongo healthy |
| HappyBase / Thrift timeout | HBase container still starting — wait 1–2 min, rerun |
| Spark / Java errors | Confirm `java -version` is 17; set `JAVA_HOME` if needed |
| Out of memory on FULL | Stay on DEMO, or raise `spark.driver.memory` in `spark_common.py` |
| Port 27017 / 9090 busy | Stop conflicting services or change ports in `docker-compose.yml` + `config.py` |

Stop containers when done:

```powershell
docker compose down
```
