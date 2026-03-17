# LLM-Powered Data Quality Monitor

> An end-to-end data quality pipeline that detects anomalies in transaction data,
> explains them using Anthropic Claude AI, and delivers plain-English alerts to Slack.

![Pipeline](assets/pipeline-diagram.png)
![Slack Alert](assets/slack-alert.png)

---

## What it does

Most data pipelines tell you **that** something broke. This one tells you **why** it broke,
**what** the business impact is, and **how** to fix it — automatically, in plain English.

**The problem it solves:**
When a data quality check fails, engineers get a raw JSON alert nobody can interpret quickly.
This pipeline adds an AI layer: failed checks are sent to Claude API which generates a
structured root cause analysis delivered directly to Slack.

---

## Architecture
```
AWS S3 (raw data)
     │
     ▼
Airflow DAG (@daily)
     │
     ├── Task 1: Download CSV from S3
     │
     ├── Task 2: Run 9 validation rules (pandas)
     │              │
     │         If failures exist
     │              │
     ├── Task 3: Call Claude API → root cause analysis
     │
     └── Task 4: Send formatted alert to Slack
```

**Short-circuit logic:** If all checks pass, Tasks 3 and 4 are skipped — no noise.

---

## Tech Stack

| Tool | Role |
|------|------|
| Apache Airflow 2.8 | Pipeline orchestration and scheduling |
| Python + pandas | Data validation engine (9 custom rules) |
| Anthropic Claude API | LLM-powered anomaly explanation |
| AWS S3 | Cloud data lake — raw file storage |
| Slack Incoming Webhooks | Alert delivery |
| Docker + Docker Compose | Containerised local deployment |
| PostgreSQL | Airflow metadata store |

---

## Data Quality Rules

| Rule | Column | Check |
|------|--------|-------|
| Not null | tx_id | Primary key must never be null |
| Not null | amount | Amount must never be null |
| Range check | amount | Must be between 0 and 10,000 |
| Set check | status | Must be success, failed, or pending |
| Not null | user_id | User ID must never be null |
| Not null | timestamp | Timestamp must never be null |
| Set check | is_international | Must be 0 or 1 only |
| Row count | table | Must have 100 to 1,000,000 rows |
| Schema check | table | All 7 required columns must exist |

---

## Sample Slack Alert
```
🚨 Data Quality Alert: transactions
3 of 9 checks FAILED.

Failed Checks:
- tx_id — column values to not be null: 100 null values found
- amount — column values to be between: 378 values outside [0, 10000]
- status — column values to be in set: 466 invalid values (e.g. INVALID, unknown)

Claude's Analysis:

1. ROOT CAUSE
Null tx_ids suggest a race condition in the ID generation service.
Negative amounts indicate the payment gateway returning refunds without sanitisation.
Invalid status values point to an undocumented schema change in the upstream CRM.

2. BUSINESS IMPACT
Revenue reports will show incorrect totals due to negative amounts.
Customer 360 joins will silently break on null tx_ids.
Dashboard filters will drop invalid status rows without warning.

3. RECOMMENDED FIX
Immediate: Quarantine affected rows before warehouse load.
Add abs() transform on amount in ingestion layer.
Next sprint: Add status enum validation at the API gateway level.

🤖 Powered by Anthropic Claude | llm-dq-monitor
```

---

## Quickstart

### Prerequisites
- Docker Desktop running
- AWS account with S3 bucket
- Anthropic API key
- Slack workspace with Incoming Webhook

### 1. Clone the repo
```bash
git clone https://github.com/Balasubramanyam367/llm-dq-monitor.git
cd llm-dq-monitor
```

### 2. Set up environment variables
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```env
ANTHROPIC_API_KEY=sk-ant-your-key-here
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
S3_BUCKET=your-bucket-name
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/your/webhook
```

### 3. Generate and upload sample data
```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
.\venv\Scripts\activate         # Windows
pip install -r requirements.txt
python data/generate_data.py
```

### 4. Start Airflow
```bash
docker-compose up airflow-init
docker-compose up -d airflow-webserver airflow-scheduler postgres
```

### 5. Trigger the pipeline

Open **http://localhost:8080** → login `admin/admin` → trigger `dq_monitor_pipeline`.

Check your Slack channel for the alert.

---

## Project Structure
```
llm-dq-monitor/
├── dags/
│   └── dq_pipeline.py          # Airflow DAG — 4 tasks
├── data/
│   └── generate_data.py        # Synthetic dataset generator + S3 upload
├── expectations/
│   └── validate.py             # 9 pandas-based DQ rules
├── notifier/
│   ├── claude_explainer.py     # Anthropic Claude API integration
│   └── slack_sender.py         # Slack Block Kit alert sender
├── tests/
│   └── test_explainer.py       # Unit tests
├── Dockerfile                  # Custom Airflow image with dependencies
├── docker-compose.yml          # Full stack — Airflow + Postgres
├── requirements.txt
└── .env.example
```

---

## Key Design Decisions

**Why pandas validation instead of Great Expectations?**
Great Expectations 0.18 does not support Python 3.14. Building a custom validation
engine gives full control over rule definitions and produces cleaner JSON output
for the LLM prompt — no parsing overhead.

**Why Claude API instead of GPT-4?**
Claude Sonnet offers 200K token context, strong instruction following for structured
3-section output, and comparable cost to GPT-4o-mini. The large context window means
the full validation report can be passed without truncation.

**Why ShortCircuitOperator?**
Prevents unnecessary Claude API calls and Slack noise when data is clean.
Only engineers when failures exist — smart alerting, not spam alerting.

---

## Results

- 9 data quality rules covering nulls, ranges, set membership, schema and row count
- Claude API response time under 2 seconds per explanation
- Estimated production cost under $20/month for daily runs on 100 datasets
- Full pipeline runtime under 60 seconds end to end

---

## Future Improvements

- [ ] Add dbt tests post-transformation for warehouse-layer validation
- [ ] Store validation history in PostgreSQL for trend analysis
- [ ] Add Streamlit dashboard showing DQ health over time
- [ ] Parameterise DAG to handle multiple datasets via config file
- [ ] Add PagerDuty integration for critical severity failures
- [ ] Deploy Airflow to AWS MWAA for production cloud hosting

---

## Author

**Nitturi Balasubramanyam** — Data Engineer
[LinkedIn](https://www.linkedin.com/in/nitturi/) •
[GitHub](https://github.com/Balasubramanyam367) •
[Portfolio](https://www.balasubramanyam.info)

*Open to Data Engineer opportunities in the Bay Area*
