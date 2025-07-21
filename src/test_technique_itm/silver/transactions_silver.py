# Import Modules

from ..common import pd, StringIO, datetime, os, bigquery, re, client 


def extract_datetime_from_filename(filename):
    """Extract the date and time from the filename"""
    match = re.search(r'transactions_(\d{4}-\d{1,2}-\d{1,2})_(\d{1,2})\.csv', filename)
    if match:
        date_str = match.group(1)
        hour_str = match.group(2)
        return datetime.strptime(f"{date_str} {hour_str}:00:00", "%Y-%m-%d %H:%M:%S")
    return None


def extract_date_from_source_file(file_name):
    """Extract the date from column source_file"""
    match = re.search(r'transactions_(\d{4}-\d{1,2}-\d{1,2})_\d+\.csv', file_name)
    return match.group(1) if match else None


def process_transactions_azure_to_silver(container_client):

    target_table = "inthememory.Silver.transactions"  # Target table name
    dataset_id, table_id = target_table.split(".")[1], target_table.split(".")[2]

    # Check if the table exists in Google BigQuery
    table_exists = True
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        client.get_table(table_ref)
    except Exception:
        table_exists = False

    # If the table exists, retrieve the latest timestamp from it
    last_timestamp = None
    if table_exists:
        query = f"SELECT MAX(timestamp) AS max_ts FROM `{target_table}`"
        df_max = client.query(query).result().to_dataframe()
        last_timestamp = df_max['max_ts'].iloc[0]

    blobs_list = container_client.list_blobs()  # List of blobs

    df_list = []  # Store DataFrames here

    for blob in blobs_list:
        if blob.name.startswith("transactions_") and blob.name.endswith(".csv"):  # Files with correct format
            file_datetime = extract_datetime_from_filename(blob.name)  # Extract date from filename
            if file_datetime is None:
                print(f"Unable to extract the date from the file: {blob.name}")
                continue
            if last_timestamp and file_datetime <= last_timestamp:
                continue  # Skip older or equal files
            #if "2023-11-30" in blob.name:
            #    print(f"File ignored (manually filtered): {blob.name}")
            #    continue

            blob_client = container_client.get_blob_client(blob.name)  # Download file
            blob_data = blob_client.download_blob().readall()  # Read file content

            df = pd.read_csv(StringIO(blob_data.decode('utf-8')), sep=';', comment='#')  # Skip commented lines

            # Validate required columns
            expected_cols = ['transaction_id', 'client_id', 'date', 'hour', 'minute', 'product_id', 'quantity', 'store_id']
            missing_cols = [col for col in expected_cols if col not in df.columns]
            if missing_cols:
                print(f"Warning, missing columns in {blob.name}: {missing_cols}")
                continue

            df = df[expected_cols]
            df['source_file'] = blob.name
            df_list.append(df)

    # Merge all loaded files into a single DataFrame
    if df_list:
        df_all_transactions = pd.concat(df_list, ignore_index=True)
        print(f"{len(df_list)} files loaded")
    else:
        return print("No new transaction files found.")
    
    # Alert: Quantities equal to 0 or negative
    nb_zero_qty = (df_all_transactions['quantity'] == 0).sum()
    nb_negative_qty = (df_all_transactions['quantity'] < 0).sum()

    if nb_zero_qty > 0 or nb_negative_qty > 0:
        print(f"Warning: {nb_zero_qty} rows with quantity = 0 and {nb_negative_qty} rows with quantity < 0 detected")


    # Exclude rows where quantity is 0    
    df_all_transactions = df_all_transactions[df_all_transactions['quantity'] > 0]

    # Apply correction only where date == "1999-01-01"
    mask = df_all_transactions['date'] == '1999-01-01'
    df_all_transactions.loc[mask, 'date'] = df_all_transactions.loc[mask, 'source_file'].apply(extract_date_from_source_file)

    # Create a unique transaction ID (concatenate transaction_id and client_id)
    df_all_transactions['id'] = df_all_transactions['transaction_id'].astype(str) + df_all_transactions['client_id'].astype(str)

    # Create a timestamp column
    df_all_transactions['timestamp'] = pd.to_datetime(
        df_all_transactions['date'].astype(str) + ' ' +
        df_all_transactions['hour'].astype(str).str.zfill(2) + ':' +
        df_all_transactions['minute'].astype(str).str.zfill(2) + ':00',
        format='%Y-%m-%d %H:%M:%S'
    )

    # Cast columns to string
    df_all_transactions['client_id'] = df_all_transactions['client_id'].astype(str)
    df_all_transactions['transaction_id'] = df_all_transactions['transaction_id'].astype(str)
    df_all_transactions['product_id'] = df_all_transactions['product_id'].astype(str)
    df_all_transactions['store_id'] = df_all_transactions['store_id'].astype(str)

    # Load client data to add account_id column to transactions
    query = """
        SELECT 
            id as clientId,
            account_id
        FROM `inthememory.Silver.clients`
        WHERE isEnabled = true
    """

    query_job = client.query(query)
    df_silver_clients = query_job.result().to_dataframe()

    # Join transactions with client data
    df_all_transactions = df_all_transactions.merge(
        df_silver_clients,
        left_on='client_id',
        right_on='clientId',
        how='left'
    )

    df_transactions_silver = df_all_transactions[['id', 'transaction_id', 'client_id', 'account_id', 'product_id', 'store_id', 'quantity', 'timestamp', 'source_file']].copy()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # Append because this is a fact table updated daily
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED"
    )

    job = client.load_table_from_dataframe(df_transactions_silver, target_table, job_config=job_config)
    job.result()  # Wait for the load to complete

    return print(f"✅ {job.output_rows} rows successfully imported into {target_table}")
