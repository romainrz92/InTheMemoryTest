# Import Modules

from ..common import os, bigquery, client 

def process_products_silver_to_gold():
    """
    Retrieve product data from Silver dataset and load it into Gold dataset.
    Mostly identical, with some column renaming and timestamp additions.
    """

    query = """
        SELECT 
            id as productId,
            ean as eanProductNumber, 
            brand,
            description,
            FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP(), "Europe/Paris") as createdDate,
            FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP(), "Europe/Paris") as modifiedDate
        FROM `inthememory.Silver.products`
        WHERE isEnabled = true
    """

    query_job = client.query(query)  # Execute the query
    df_product_gold = query_job.result().to_dataframe()  # Retrieve the result into a pandas DataFrame

    target_table = "inthememory.Gold.products"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace the entire table with new data,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df_product_gold, target_table, job_config=job_config)
    job.result()   # To confirm that the load is complete before continuing

    return print(f"✅ {job.output_rows} rows successfully imported into {target_table}")
