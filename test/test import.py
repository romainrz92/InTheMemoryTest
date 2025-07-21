import pandas as pd
from io import StringIO
from datetime import datetime
from zoneinfo import ZoneInfo
import os
#pip install google-cloud-bigquery-storage


from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gbqkey/inthememory-d591655b1691.json"

client = bigquery.Client()

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



query_job = client.query(query)
df_gold_clients = query_job.result().to_dataframe()
print(df_gold_clients)