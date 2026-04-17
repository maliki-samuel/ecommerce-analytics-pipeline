# ==============================
# GOOGLE COLAB SETUP
# ==============================

from google.colab import drive, auth
drive.mount('/content/drive')

auth.authenticate_user()

# ==============================
# IMPORTS
# ==============================

import os
import re
import pandas as pd
from google.cloud import bigquery

# ==============================
# INPUTS
# ==============================

project_id = "de-project-493506"
dataset = "analytics_db"
base_name = "ecommerce_sales_customer_analytics"
data_source_type = "multiple_files"

folder_path = "/content/drive/MyDrive/data-engineering-projects/ecommerce_sales_customer_analytics/data/raw"

file_list = [
    "raw_endpoints.json",
    "user_activity_events.json",
    "product_reviews.csv",
    "shipping_logs.csv",
    "payment_records.csv",
    "order_line_items.csv",
    "order_transactions.csv",
    "catalog_products.csv",
    "users_data.csv"
]

raw_data_list = [
    "raw_endpoints",
    "user_activity_events",
    "product_reviews",
    "shipping_logs",
    "payment_records",
    "order_line_items",
    "order_transactions",
    "catalog_products",
    "users_data"
]

# ==============================
# VALIDATION
# ==============================

def validate_inputs(data_source_type, folder_path, file_list, raw_data_list):
    if data_source_type == "multiple_files":
        if not folder_path:
            raise ValueError("folder_path MUST exist for multiple_files")

        if not raw_data_list:
            raise ValueError("raw_data_list MUST exist for multiple_files")

        if len(file_list) != len(raw_data_list):
            raise ValueError("file_list and raw_data_list must have same length")

        # Strip values
        file_list[:] = [f.strip() for f in file_list]
        raw_data_list[:] = [r.strip() for r in raw_data_list]

    else:
        raise ValueError("Only multiple_files supported in this script")

# ==============================
# LOAD DATA
# ==============================

def load_data(folder_path, file_list):
    dataframes = {}

    for file in file_list:
        full_path = os.path.join(folder_path, file)

        if not os.path.exists(full_path):
            raise FileNotFoundError(f"{full_path} not found")

        if file.endswith(".csv"):
            df = pd.read_csv(full_path)

        elif file.endswith(".json"):
            df = pd.read_json(full_path, lines=True)

        elif file.endswith(".parquet"):
            df = pd.read_parquet(full_path)

        else:
            raise ValueError(f"Unsupported file type: {file}")

        dataframes[file] = df

    return dataframes

# ==============================
# CLEAN STRUCTURE
# ==============================

def clean_structure(df):
    # Lowercase columns
    df.columns = [col.lower() for col in df.columns]

    # Replace spaces with underscores
    df.columns = [col.replace(" ", "_") for col in df.columns]

    # Remove non-alphanumeric (except underscore)
    df.columns = [re.sub(r"[^\w]", "", col) for col in df.columns]

    # Ensure unique column names
    seen = {}
    new_cols = []

    for col in df.columns:
        if col not in seen:
            seen[col] = 0
            new_cols.append(col)
        else:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")

    df.columns = new_cols

    # Drop completely empty rows
    df = df.dropna(how='all')

    return df

# ==============================
# LOAD TO BIGQUERY
# ==============================

def load_to_bigquery(project_id, dataset, dataframes, raw_data_list):
    client = bigquery.Client(project=project_id)

    for i, (file, df) in enumerate(dataframes.items()):
        table_name = raw_data_list[i]
        table_id = f"{project_id}.{dataset}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print(f"Loaded {file} → {table_id}")

# ==============================
# MAIN EXECUTION
# ==============================

def main():
    validate_inputs(data_source_type, folder_path, file_list, raw_data_list)

    dataframes = load_data(folder_path, file_list)

    cleaned_dataframes = {}
    for file, df in dataframes.items():
        cleaned_df = clean_structure(df)
        cleaned_dataframes[file] = cleaned_df

    load_to_bigquery(project_id, dataset, cleaned_dataframes, raw_data_list)

# ==============================
# RUN
# ==============================

if __name__ == "__main__":
    main()