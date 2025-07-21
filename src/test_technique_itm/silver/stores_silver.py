# Import Modules

from ..common import pd, StringIO, datetime, ZoneInfo, os, bigquery, client

def format_hour(val):
    """Convert the hour format from int to HH:MM format"""
    val = int(val)
    if val < 100:  
        hour = val
        minute = 0
    else:
        hour = val // 100
        minute = val % 100
    return f"{hour:02d}:{minute:02d}"



def process_stores_azure_to_silver(container_client):
    stores_file_name = "stores.csv"
    blob_stores = container_client.get_blob_client(stores_file_name) # Download the file from the Azure storage container
    blob_data_stores = blob_stores.download_blob().readall() # Read the entire content of the downloaded file into memory

    # Charge dans DataFrame
    df_stores = pd.read_csv(StringIO(blob_data_stores.decode('utf-8')), sep=';') # Load store data into a DataFrame from the CSV file
    
    # We add some data here to track when the data was loaded and whether it is usable, important for dimension tables in general
    now = datetime.now(ZoneInfo("Europe/Paris")).strftime('%Y-%m-%d %H:%M:%S')
    df_stores['createdDate'] = now
    df_stores['modifiedDate'] = now
    df_stores['isEnabled'] = True 

    df_stores[['latitude', 'longitude']] = df_stores['latlng'].str.strip('()').str.split(',', expand=True).astype(float) #Split the latlng column into separate latitude and longitude columns for easier use
    
    
    df_stores = df_stores.copy()
    df_stores["opening"] = df_stores["opening"].apply(format_hour) # Convert the time format for better clarity
    df_stores["closing"] = df_stores["closing"].apply(format_hour)

    df_stores_silver = df_stores[['id','latitude','longitude','opening','closing','type','modifiedDate',"createdDate",'isEnabled']].copy() # Reorganize the columns

    # Changing the id data type from integer to string
    df_stores_silver['id'] = df_stores['id'].astype(str)
    df_stores_silver['type'] = df_stores_silver['type'].astype(str)
    
    # Importing store data into the Silver dataset in Google BigQuery.
    target_table = "inthememory.Silver.stores"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # We only have one file, so we import everything directly.
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df_stores_silver, target_table, job_config=job_config)
    job.result()   #To confirm that the load is complete before continuing
   
    return print(f"✅ {job.output_rows} rows successfully imported into {target_table}")
