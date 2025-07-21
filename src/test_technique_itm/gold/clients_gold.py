# Import Modules

from ..common import os, bigquery, client 

def process_clients_silver_to_gold():
    """
    Retrieve client data from Silver dataset and load it into Gold dataset.
    Mostly identical, with some column renaming and timestamp additions.
    """
    
    query = """
        SELECT 
            id as clientId,
            name, 
            job,
            email,
            account_id as accountId,
            FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP(), "Europe/Paris") as createdDate,
            FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP(), "Europe/Paris") as modifiedDate
        FROM `inthememory.Silver.clients`
        WHERE isEnabled = true
    """

    query_job = client.query(query)  # Execute the query
    df_gold_clients = query_job.result().to_dataframe()  # Retrieve the result into a pandas DataFrame

    target_table = "inthememory.Gold.clients"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace the entire table with new data
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df_gold_clients, target_table, job_config=job_config)
    job.result()   # To confirm that the load is complete before continuing

    print(f"✅ {job.output_rows} rows successfully imported into {target_table}")
