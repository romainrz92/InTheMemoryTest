# Import Modules

from ..common import pd, StringIO, datetime, ZoneInfo, os, bigquery, client


def process_products_azure_to_silver(container_client):
    products_file_name = "products.csv"

    blob_products = container_client.get_blob_client(products_file_name)  # Download the file from the Azure storage container
    blob_data_products = blob_products.download_blob().readall()  # Read the entire content of the downloaded file into memory

    df_products = pd.read_csv(StringIO(blob_data_products.decode('utf-8')), sep=';')  # Load product data into a DataFrame from the CSV file
    
    # We add some data here to track when the data was loaded and whether it is usable, important for dimension tables in general
    now = datetime.now(ZoneInfo("Europe/Paris")).strftime('%Y-%m-%d %H:%M:%S')
    df_products['createdDate'] = now
    df_products['modifiedDate'] = now
    df_products['isEnabled'] = True 

    # Changing the id data type from integer to string
    df_products['id'] = df_products['id'].astype(str)
    df_products['ean'] = df_products['ean'].astype(str)

    # Importing product data into the Silver dataset in Google BigQuery.
    target_table = "inthememory.Silver.products"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # We only have one file, so we import everything directly.
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df_products, target_table, job_config=job_config)
    job.result()  # To confirm that the load is complete before continuing

    return print(f"✅ {job.output_rows} rows successfully imported into {target_table}")
