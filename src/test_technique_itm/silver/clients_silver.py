# Import Modules

from ..common import pd, StringIO, datetime, ZoneInfo, os, bigquery, client

def process_clients_azure_to_silver(container_client):
    clients_file_name = "clients.csv"
    blob_client = container_client.get_blob_client(clients_file_name)  # Download the file from the Azure storage container
    blob_data_client = blob_client.download_blob().readall()  # Read the entire content of the downloaded file into memory

    df_clients = pd.read_csv(StringIO(blob_data_client.decode('utf-8')), sep=';')  # Load customer data into a DataFrame from the CSV file
    
    # We add some data here to track when the data was loaded and whether it is usable, important for dimension tables in general
    now = datetime.now(ZoneInfo("Europe/Paris")).strftime('%Y-%m-%d %H:%M:%S') 
    df_clients['createdDate'] = now
    df_clients['modifiedDate'] = now
    df_clients['isEnabled'] = True 

    # Data cleaning because some clients have the same ID / To be further discussed with the team
    df_clients_silver = df_clients[~df_clients.duplicated(subset='id', keep='first')].copy()
    
    # Changing the id data type from integer to string
    df_clients_silver['id'] = df_clients_silver['id'].astype(str)
    df_clients_silver['account_id'] = df_clients_silver['account_id'].astype(str)
    
    # Importing customer data into the Silver dataset in Google BigQuery.
    target_table = "inthememory.Silver.clients"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # We only have one file, so we import everything directly.
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df_clients_silver, target_table, job_config=job_config)
    job.result()  # To confirm that the load is complete before continuing

    return print(f"✅ {job.output_rows} rows successfully imported into {target_table}")
