
## 🧑‍💼 **Investor Persona: "Alex, the Tech-Sector Growth Investor"**

### 📌 Background:

Alex is a data-driven investor focused on **medium- to long-term growth in the tech sector**. He invests in U.S.-based tech giants and promising innovators — not day trading, but he monitors daily trends and macro signals to **refine his strategy and manage risk**.

---

## 🏢 **Business Scenario**:

Alex manages a personal portfolio of tech stocks. He wants a daily automated system that helps him answer:

> **"Are any of my key stocks showing early signs of a breakout, downturn, or increased volatility that could inform a buy/hold/sell decision?"**

---

## 🎯 **Business Requirements**

### 1. **Portfolio Scope**:

Focus on 5 key tech stocks:

* Apple (`AAPL`)
* Microsoft (`MSFT`)
* NVIDIA (`NVDA`)
* Tesla (`TSLA`)
* Amazon (`AMZN`)

> *(These tickers will be pulled daily from Alpha Vantage)*

---

### 2. **Data Requirements**:

* Daily closing prices
* Volume
* Price change %
* 7-day and 30-day moving averages
* Simple volatility metric (rolling standard deviation)
* Optional: RSI or MACD if time allows

---

### 3. **Trigger Conditions for Actionable Alerts**:

These will be stored and shown on the dashboard later:

| Condition                      | Actionable Insight                      |
| ------------------------------ | --------------------------------------- |
| Daily % Change > 4% or < -4%   | Significant movement worth reviewing    |
| 7-day MA crosses 30-day MA     | Bullish/bearish trend signal            |
| Volatility > 2× 30-day average | Possible market uncertainty or breakout |

---

### 4. **Report Goals (Output)**:

Alex wants a **daily dashboard/report** showing:

* Price chart per stock (7-day + 30-day MA overlay)
* List of triggered signals per stock
* Tabular summary of:

  * Open, High, Low, Close
  * % Change
  * MA(7), MA(30)
  * Volatility
* Exportable CSV

---

### 5. **System Requirements**:

* Fully automated pipeline (Airflow)
* Historical data stored in PostgreSQL
* Portable and reproducible (Docker)
* GitHub versioned and documented

---

### 📦 Pipeline Summary:

| Stage     | Action                                                         |
| --------- | -------------------------------------------------------------- |
| Ingest    | Pull daily time series data from Alpha Vantage (for 5 tickers) |
| Process   | Calculate % change, moving averages, volatility                |
| Store     | Save raw & processed data to PostgreSQL                        |
| Analyze   | Compare against trigger thresholds                             |
| Visualize | Dashboard/report (with alerts)                                 |
| Automate  | Schedule daily with Airflow                                    |
| Package   | Dockerize for local/dev deployment                             |



Business Problem Scenario:
"How can we provide actionable daily insights on stock performance trends to support timely investment decisions and risk management?"

🧠 Problem Breakdown:
🎯 Target Users:
Portfolio Managers

Retail Investors

Financial Analysts

Risk Officers

💼 Business Needs:
Track stock price movement daily.

Detect patterns like volatility spikes, sudden drops/rises.

Calculate key indicators (e.g., moving averages, returns).

Trigger alerts based on thresholds or trends.

Generate daily or weekly investment performance reports.

Reduce manual overhead in market data monitoring.

🛠️ Implementation Workflow with Your Stack:
1. Data Ingestion (Python + Requests + Airflow)
Pull stock price data daily using an API (e.g., Alpha Vantage, Yahoo Finance API).

Parse and normalize the JSON response.

2. Data Processing (Python + Pandas)
Clean and transform the data.

Compute derived metrics like:

Daily % Change

7-day & 30-day Moving Averages

Volatility (Standard Deviation)

RSI or MACD (for advanced users)

3. Data Storage (PostgreSQL)
Store raw and processed data in structured tables:

stocks_raw

stocks_metrics

alerts_triggered

4. Reporting and Dashboarding
Build a dashboard using a BI tool (Metabase, Superset, Power BI, or even Streamlit).

Show visual insights like:

Stock trends and comparisons

Volatility charts

Triggered alerts table

Portfolio performance simulation

5. Automation (Apache Airflow + Docker)
Schedule the entire ETL workflow as a DAG:

Task 1: Fetch & process stock data.

Task 2: Store in PostgreSQL.

Task 3: Trigger reports/dashboard update.

