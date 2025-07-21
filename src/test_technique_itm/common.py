import pandas as pd
from io import StringIO
from datetime import datetime
from zoneinfo import ZoneInfo
import os
from azure.storage.blob import BlobServiceClient
from google.cloud import bigquery
import re

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/gbqkey/inthememory-d591655b1691.json" # Load credentials to connect to Google BigQuery datasets

client = bigquery.Client() # Initialize BigQuery client
