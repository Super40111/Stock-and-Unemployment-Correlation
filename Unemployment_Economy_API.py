from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import json
import os

SOURCE_UNEMPLOYMENT = 2 
SOURCE_STOCK = 15

API_BASE = "https://api.worldbank.org/v2"

SERIES_UNEMPLOYMENT = "SL.UEM.TOTL.ZS" #API KEYS
SERIES_STOCK = "DSTKMKTXD"

default_args = {
    "owner": "User",
    "start_date": datetime(2026, 6, 13),
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG( 
    dag_id="Unemployed_Economy_API",
    default_args=default_args,
    description="Parallel API pipelines for unemployment and stock data",
    schedule="@weekly",
    catchup=False
)

def fetch_indicator_from_api(indicator_code, source_id): #Function used to pull a specific dataset using the API
    all_rows = []
    page = 1
    pages = 1

    while page <= pages:
        url = f"{API_BASE}/country/all/indicator/{indicator_code}" #Creates the API URL

        params = {
            "source": source_id,
            "format": "json",
            "per_page": 20000,
            "page": page
        } #Sets query Parameters

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status() 

        data = response.json() #Fetches the request and makes sure a proper request is received. If good data is recieved, it is stored as a variable.

        if not isinstance(data, list) or len(data) < 2: #Throws if data is bad
            raise ValueError(f"Unexpected API response for indicator {indicator_code}: {data}")

        metadata = data[0]
        rows = data[1] or []

        all_rows.extend(rows) #Logs the correct data and appends the rows of data

        pages = int(metadata.get("pages", 1)) #Sets the max page to the correct value and increments the current page.
        page += 1

    if not all_rows: #Makes sure that the data returned is not empty
        raise ValueError(f"No API data returned for indicator {indicator_code}")

    return all_rows

def extract_indicator(indicator_code, source_id, output_path):
    os.makedirs("/opt/airflow/data", exist_ok=True) #Creates a data directory
    rows = fetch_indicator_from_api(indicator_code, source_id) #Pulls raw data using previous function

    df = pd.json_normalize(rows) #Converts the results into a dataframe

    required_cols = ["date", "country.value", "value"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing expected columns from API response: {missing_cols}")

    df = df[required_cols] #Grabs the required columns and removes anything else

    df = df.rename(columns={
        "date": "Year",
        "country.value": "Country",
        "value": "Value"
    }) 

    df["Value"] = pd.to_numeric(df["Value"], errors="coerce") 

    df = df.pivot_table(
        index="Year",
        columns="Country",
        values="Value",
        aggfunc="first"
    ) #Converts the data set so rows are Years and columns are Countries

    df = df.reset_index()
    df = df.sort_values("Year").reset_index(drop=True)
    df = df.fillna("N/A") #Sorts the data by year and makes any empty data 'N/A'
    df.columns = df.columns.map(str) #Makes sure all column names are strings.

    df.to_csv(output_path, index=False) #Takes the current data and converts it into a csv
    return output_path

def extract_Unemployment(): #Puts the raw data from the first data set into a csv file
    return extract_indicator(
        SERIES_UNEMPLOYMENT,
        SOURCE_UNEMPLOYMENT,
        "/opt/airflow/data/unemployment_raw.csv"
    )

def extract_Stock(): #Puts the raw data from the second data set into a csv file
    return extract_indicator(
        SERIES_STOCK,
        SOURCE_STOCK,
        "/opt/airflow/data/stock_raw.csv"
    )

def remove_NA(task_id, output_path, **context):
    path = context["ti"].xcom_pull(task_ids=task_id)
    df = pd.read_csv(path)

    cols_to_drop = [
    col for col in df.columns
    if col != "Year" and (df[col].iloc[1:].isna().all() or(df[col].iloc[1:] == "N/A").all())
    ] #Gets all columns with only "N/A" in order to remove them

    df = df.drop(columns=cols_to_drop)
    df.to_csv(output_path, index=False) #Drops targeted columns and creates a csv file with the remaining data.
    return output_path

def remove_NA_Unemployment(**context): #Removes empty columns from the first data set
    return remove_NA(
        "extract_Unemployment",
        "/opt/airflow/data/unemployment_na_removed.csv",
        **context
    )

def remove_NA_Stock(**context): #Removes empty columns from the second data set
    return remove_NA(
        "extract_Stock",
        "/opt/airflow/data/stock_na_removed.csv",
        **context
    )

def compute_shift(task_id, output_path, **context): #Computes the shift over time
    path = context["ti"].xcom_pull(task_ids=task_id)
    df = pd.read_csv(path) #Pulls the data from the saved context

    df_new = df.copy() #Creates a new df and copies the old data into the new one.

    numeric_cols = [col for col in df.columns if col != "Year"]
    converted = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    for col in numeric_cols:
        prev_value = converted[col].ffill().shift(1)
        result = converted[col] - prev_value
        result = result.where(prev_value.notna(), 0)
        result = result.where(converted[col].notna(), "N/A")
        df_new[col] = result #For each column, calculate the difference between it and the data point above it and saves it into the new dataframe

    df_new = df_new.iloc[1:].reset_index(drop=True)

    df_new.to_csv(output_path, index=False)
    return output_path

def shift_Unemployment(**context): #Creates a file for the first dataset that shows the shift from year to year
    return compute_shift(
        "remove_NA_Unemployment",
        "/opt/airflow/data/unemployment_year-over-year.csv",
        **context
    )

def shift_Stock(**context): #Creates a file for the second dataset that shows the shift from year to year
    return compute_shift(
        "remove_NA_Stock",
        "/opt/airflow/data/stock_year-over-year.csv",
        **context
    )

def compute_correlation(**context):
    ti = context["ti"]

    path_u = ti.xcom_pull(task_ids="shift_Unemployment")
    path_s = ti.xcom_pull(task_ids="shift_Stock") 

    df_u = pd.read_csv(path_u).set_index("Year")
    df_s = pd.read_csv(path_s).set_index("Year") 
    common_cols = df_u.columns.intersection(df_s.columns) 

    df_u = df_u[common_cols].apply(pd.to_numeric, errors="coerce")
    df_s = df_s[common_cols].apply(pd.to_numeric, errors="coerce") #Pulls the files from both datasets and reads them. Any non-matching countries are filtered out

    results = []

    for col in common_cols:
        paired = pd.concat([df_u[col], df_s[col]], axis=1)
        paired.columns = ["Unemployment_Change", "Stock_Change"]
        paired = paired.dropna() #Concatenates the values for each table and checks for any missing data

        if len(paired) < 2:
            correlation = "N/A"
            reason = "Not enough overlapping numeric data"
        elif paired["Unemployment_Change"].nunique() <= 1:
            correlation = "N/A"
            reason = "Unemployment values do not vary"
        elif paired["Stock_Change"].nunique() <= 1:
            correlation = "N/A"
            reason = "Stock values do not vary"
        else:
            correlation = paired["Unemployment_Change"].corr(paired["Stock_Change"])
            reason = ""
        #Checks if the values can be correlated. If so, uses the Pearson correlation coefficient
        results.append({
            "Country": col,
            "Correlation": correlation,
            "Valid_Overlapping_Rows": len(paired),
            "Reason_If_NA": reason
        }) #Adds a row to the output data before continuing the loop

    result = pd.DataFrame(results)

    out_path = "/opt/airflow/data/unemployment_stock_correlation.csv"
    result.to_csv(out_path, index=False)
    return out_path

def generate_chart():
    df_corr = pd.read_csv("/opt/airflow/data/unemployment_stock_correlation.csv") #Reads the correlation file

    df_corr["Valid_Overlapping_Rows"] = df_corr["Valid_Overlapping_Rows"].apply(lambda x: int(x)) #Converts the third column into integers to prevent errors involving floats

    df_corr = df_corr[
        (df_corr["Correlation"] != "N/A") &
        (df_corr["Valid_Overlapping_Rows"] >= 10)
    ] #Filters out any values with either only 'N/A' values or any country with less than 10 values.

    df_corr["Correlation"] = pd.to_numeric(df_corr["Correlation"], errors="coerce")
    df_corr = df_corr.dropna(subset=["Correlation"])
    df_corr = df_corr.sort_values("Correlation") #Converts values to numeric values and drops anything that can't be converted before soring them in increasing order

    countries = df_corr["Country"].tolist()
    correlations = [round(float(c), 2) for c in df_corr["Correlation"].tolist()]
    colors = ['#185FA5' if c < 0 else '#993C1D' for c in correlations] #Gets the list of countries, rounds all values to 2 decimal places, and styles values based on positive or negative correlation

    countries_json = json.dumps(countries)
    correlations_json = json.dumps(correlations)
    colors_json = json.dumps(colors) #Converts the lists to json strings so the HTML can read them.

    bar_height = 28
    chart_height = max(400, len(countries) * (bar_height + 4) + 80) #Additional styling for the height of both each individual bar and the overall chart.

    #Generates the HTML for the html file.
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Unemployment vs Stock Market Correlation</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
  body {{ font-family: sans-serif; margin: 40px; color: #2C2C2A; background: #fff; }}
  .controls {{ display: flex; gap: 24px; margin-bottom: 16px; flex-wrap: wrap; align-items: flex-end; }}
  .control-group {{ display: flex; flex-direction: column; gap: 4px; }}
  label {{ font-size: 13px; color: #5F5E5A; }}
  input {{ font-size: 13px; padding: 4px 8px; border: 0.5px solid #B4B2A9; border-radius: 6px; }}
  .stats {{ display: flex; gap: 12px; margin-bottom: 24px; flex-wrap: wrap; }}
  .stat {{ background: #F1EFE8; border-radius: 8px; padding: 10px 16px; min-width: 90px; }}
  .stat-label {{ font-size: 12px; color: #5F5E5A; }}
  .stat-value {{ font-size: 22px; font-weight: 500; }}
  .legend {{ display: flex; gap: 24px; margin-bottom: 16px; font-size: 13px; color: #5F5E5A; }}
  .legend-item {{ display: flex; align-items: center; gap: 8px; }}
  .legend-swatch {{ width: 14px; height: 14px; border-radius: 2px; }}
  .callout {{ font-size: 13px; color: #5F5E5A; margin-bottom: 24px; line-height: 1.6; max-width: 600px; }}
</style>
</head>
<body>

<div class="controls">
  <div class="control-group">
    <label>Min overlapping years: <strong id="min-out">10</strong></label>
    <input type="range" min="2" max="40" value="10" step="1" id="min-rows" oninput="update()" style="width:200px;">
  </div>
</div>

<div class="stats" id="stats"></div>

<div class="legend">
  <div class="legend-item">
    <div class="legend-swatch" style="background:#185FA5;"></div>
    <span>Stocks tend to fall when unemployment rises</span>
  </div>
  <div class="legend-item">
    <div class="legend-swatch" style="background:#993C1D;"></div>
    <span>Stocks tend to rise when unemployment rises</span>
  </div>
</div>

<p class="callout">Each bar shows how closely a country's stock market moves in relation to its unemployment rate.
Bars pointing left (blue) mean stocks generally fall when unemployment rises (Negative Correlation).
Bars pointing right (red) mean they tend to move in the same direction (Positive Correlation).
The min overlapping years lets you filter the countries that show up based on the amount of years that there is recorded data of that country.</p>

<div style="position:relative;width:100%;height:{chart_height}px;">
  <canvas id="bar" role="img" aria-label="Horizontal bar chart showing correlation between unemployment and stock market change per country">
    Correlation between unemployment change and stock market change per country.
  </canvas>
</div>

<script>
const allData = {{
  countries: {countries_json},
  correlations: {correlations_json},
  colors: {colors_json},
  rows: {json.dumps(df_corr["Valid_Overlapping_Rows"].tolist())}
}};

let chart = null;

function update() {{
  const minRows = parseInt(document.getElementById('min-rows').value);
  document.getElementById('min-out').textContent = minRows;

  const indices = allData.rows.map((r, i) => [i, r]).filter(([i, r]) => r >= minRows).map(([i]) => i);
  const countries = indices.map(i => allData.countries[i]);
  const correlations = indices.map(i => allData.correlations[i]);
  const colors = indices.map(i => allData.colors[i]);
  const rows = indices.map(i => allData.rows[i]);

  const negCount = correlations.filter(c => c < 0).length;
  const posCount = correlations.filter(c => c >= 0).length;
  const avg = correlations.length
    ? (correlations.reduce((s, c) => s + c, 0) / correlations.length).toFixed(2)
    : 'N/A';

  document.getElementById('stats').innerHTML = `
    <div class="stat"><div class="stat-label">Countries shown</div><div class="stat-value">${{countries.length}}</div></div>
    <div class="stat"><div class="stat-label">Average correlation</div><div class="stat-value">${{avg}}</div></div>
    <div class="stat"><div class="stat-label">Stocks fall with unemployment</div><div class="stat-value">${{negCount}}</div></div>
    <div class="stat"><div class="stat-label">Stocks rise with unemployment</div><div class="stat-value">${{posCount}}</div></div>
  `;

  if (chart) chart.destroy();
  chart = new Chart(document.getElementById('bar'), {{
    type: 'bar',
    data: {{
      labels: countries,
      datasets: [{{
        data: correlations,
        backgroundColor: colors,
        borderWidth: 0,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            title: (items) => items[0].label,
            label: (item) => {{
              const direction = item.parsed.x < 0
                ? 'Stocks tend to fall when unemployment rises'
                : 'Stocks tend to rise when unemployment rises';
              return [
                `Correlation: ${{item.parsed.x.toFixed(2)}}`,
                `Based on ${{rows[item.dataIndex]}} years of data`,
                direction
              ];
            }}
          }}
        }}
      }},
      scales: {{
        x: {{
          min: -1,
          max: 1,
          title: {{ display: true, text: 'Correlation (-1 = strong negative, 0 = none, +1 = strong positive)', color: '#888780', font: {{ size: 12 }} }},
          grid: {{ color: 'rgba(136,135,128,0.15)' }},
          ticks: {{ color: '#888780', font: {{ size: 11 }} }},
        }},
        y: {{
          ticks: {{ color: '#2C2C2A', font: {{ size: 11 }} }},
          grid: {{ display: false }},
        }}
      }}
    }}
  }});
}}

update();
</script>
</body>
</html>"""

    with open("/opt/airflow/data/correlation_chart.html", "w") as f:
        f.write(html)

t1U = PythonOperator(dag=dag, task_id="extract_Unemployment", python_callable=extract_Unemployment)
t2U = PythonOperator(dag=dag, task_id="remove_NA_Unemployment", python_callable=remove_NA_Unemployment)
t3U = PythonOperator(dag=dag, task_id="shift_Unemployment", python_callable=shift_Unemployment)

t1S = PythonOperator(dag=dag, task_id="extract_Stock", python_callable=extract_Stock)
t2S = PythonOperator(dag=dag, task_id="remove_NA_Stock", python_callable=remove_NA_Stock)
t3S = PythonOperator(dag=dag, task_id="shift_Stock", python_callable=shift_Stock)

t6 = PythonOperator(dag=dag, task_id="compute_correlation", python_callable=compute_correlation)
t7 = PythonOperator(dag=dag, task_id="generate_chart", python_callable=generate_chart)

t1U >> t2U >> t3U
t1S >> t2S >> t3S

t3U >> t6
t3S >> t6
t6 >> t7