# Import Modules

from ..common import os, bigquery, client

def process_transactions_silver_to_gold():
     
    target_table = "inthememory.Gold.transactions" # Target table name
    dataset_id, table_id = target_table.split(".")[1], target_table.split(".")[2]

     # Check if the table exists in Google BigQuery
    table_exists = True
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        client.get_table(table_ref)
    except Exception:
        table_exists = False
    
    # If the table exists, retrieve the latest timestamp from it
    if table_exists:
        query_max_timestamp = """
        SELECT 
            MAX(timestamp) as max_timestamp
        FROM `inthememory.Gold.transactions`
        """
        query_job = client.query(query_max_timestamp)  # Execute the query
        df_max = query_job.result().to_dataframe()  
        max_timestamp = df_max['max_timestamp'].iloc[0]
        
        # Select only new data from the Silver table where the date is after the maximum date in the Gold table
        query = f"""
        SELECT 
            id as transactionIdUnique,
            transaction_id as transactionId, 
            client_id as clientId,
            account_id as accountId,
            product_id as productId,
            store_id as storeId,
            quantity,
            timestamp
        FROM `inthememory.Silver.transactions`
        WHERE timestamp> '{max_timestamp}'
        """
        
    else:
        #Select all data  with some column renaming
        query = """
        SELECT 
            id as transactionIdUnique,
            transaction_id as transactionId, 
            client_id as clientId,
            account_id as accountId,
            product_id as productId,
            store_id as storeId,
            quantity,
            timestamp
        FROM `inthememory.Silver.transactions`
        """
    

    query_job = client.query(query)  # Execute the query
    df_gold_transactions = query_job.result().to_dataframe() # Retrieve the result into a pandas DataFrame

    target_table = "inthememory.Gold.transactions"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append because this is a fact table updated daily
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED"
    )

    job = client.load_table_from_dataframe(df_gold_transactions, target_table, job_config=job_config)
    job.result()  # Wait for the load to complete

    return print(f"✅ {job.output_rows} rows successfully imported into {target_table}")
