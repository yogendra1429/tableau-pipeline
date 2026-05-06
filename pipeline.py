import os
import sys
import pandas as pd
import requests
import tableauserverclient as TSC
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from tableauhyperapi import (
    HyperProcess, Telemetry, Connection, CreateMode,
    TableDefinition, SqlType, TableName, escape_string_literal
)

load_dotenv()

CONFIG = {
    "SERVER_URL": os.getenv("TABLEAU_SERVER_URL"),
    "SITE_NAME": os.getenv("TABLEAU_SITE_NAME"),
    "TOKEN_NAME": os.getenv("TABLEAU_PAT_KEY"),
    "TOKEN_VALUE": os.getenv("TABLEAU_PAT_SECRET"),
    "MASTER_HYPER": "master_analytics.hyper",
    "TABLE_NAME": TableName("Extract", "Extract"),
    "RETENTION_DAYS": 180,
    "CHUNK_SIZE": 200000 
}

def publish_to_tableau():
    print("Connecting to Tableau Cloud...")
    auth = TSC.PersonalAccessTokenAuth(CONFIG["TOKEN_NAME"], CONFIG["TOKEN_VALUE"], CONFIG["SITE_NAME"])
    server = TSC.Server(CONFIG["SERVER_URL"], use_server_version=True)

    with server.auth.sign_in(auth):
        all_ds, _ = server.datasources.get()
        ds = next((d for d in all_ds if d.name == "AppAnalytics_Master"), None)
        
        if not ds:
            print("Datasource not found. Ensure 'AppAnalytics_Master' exists on Tableau.")
            return

        print(f"Publishing {CONFIG['MASTER_HYPER']}...")
        # Overwrite the datasource
        server.datasources.publish(ds, CONFIG["MASTER_HYPER"], mode=TSC.Server.PublishMode.Overwrite)
        
        # --- CHANGED: Force Dashboard Refresh ---
        server.datasources.refresh(ds)
        # ----------------------------------------
        print("Publish and Refresh Triggered.")

def run_pipeline(job_id, csv_url):
    raw_file = f"raw_{job_id}.csv"
    temp_batch = f"temp_batch_{job_id}.csv"
    
    try:
        print(f"Downloading: {csv_url[:50]}...")
        r = requests.get(csv_url, stream=True, timeout=900)
        r.raise_for_status()
        with open(raw_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

        master_exists = os.path.exists(CONFIG["MASTER_HYPER"])
        mode = CreateMode.CREATE_AND_REPLACE if not master_exists else CreateMode.NONE

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            # --- REPLACE THIS SECTION ---
                with Connection(endpoint=hyper.endpoint, database=CONFIG["MASTER_HYPER"], create_mode=mode) as conn:
    
    # Correct way to ensure schema exists safely
                        conn.execute_command('CREATE SCHEMA IF NOT EXISTS "Extract"')
    
    # Standard Hyper API check for table existence
                            table_exists = conn.catalog.has_table(CONFIG["TABLE_NAME"])
# -----------------------------    
                # --------------------------------------------
                
                for chunk in pd.read_csv(raw_file, chunksize=CONFIG["CHUNK_SIZE"], low_memory=False):
                    
                    if "timestamp_derived" in chunk.columns:
                        chunk["timestamp_derived"] = pd.to_datetime(chunk["timestamp_derived"], errors="coerce", utc=True)
                        cutoff = pd.Timestamp.now(timezone.utc) - pd.Timedelta(days=CONFIG["RETENTION_DAYS"])
                        chunk = chunk[chunk["timestamp_derived"] >= cutoff]

                    if chunk.empty:
                        continue

                    # --- CHANGED: Schema Creation logic moved inside chunk loop ---
                    if not table_exists:
                        tdef = TableDefinition(table_name=CONFIG["TABLE_NAME"])
                        for col in chunk.columns:
                            tdef.add_column(col, SqlType.timestamp() if col == "timestamp_derived" else SqlType.text())
                        conn.catalog.create_table(tdef)
                        table_exists = True
                    # -------------------------------------------------------------

                    chunk.to_csv(temp_batch, index=False)
                    conn.execute_command(
                        f"COPY {CONFIG['TABLE_NAME']} FROM {escape_string_literal(os.path.abspath(temp_batch))} "
                        f"WITH (format csv, header true)"
                    )
                    if os.path.exists(temp_batch): os.remove(temp_batch)

                print("Cleaning up data older than 180 days...")
                cutoff_str = (datetime.now(timezone.utc) - timedelta(days=CONFIG["RETENTION_DAYS"])).strftime('%Y-%m-%d %H:%M:%S')
                conn.execute_command(f"DELETE FROM {CONFIG['TABLE_NAME']} WHERE \"timestamp_derived\" < '{cutoff_str}'")

        publish_to_tableau()

    except Exception as e:
        print(f"Pipeline Error: {e}")
        sys.exit(1)
    finally:
        if os.path.exists(raw_file): os.remove(raw_file)
        if os.path.exists(temp_batch): os.remove(temp_batch)

if __name__ == "__main__":
    if len(sys.argv) > 2:
        run_pipeline(sys.argv[1], sys.argv[2])
