import unittest
from unittest.mock import MagicMock, patch

from src.test_technique_itm.silver.stores_silver import process_stores_azure_to_silver


class TestProcessStores(unittest.TestCase):
    @patch("src.test_technique_itm.silver.stores_silver.client")
    @patch("src.test_technique_itm.silver.stores_silver.bigquery")
    def test_latitude_longitude_transformation(self, mock_bigquery, mock_client):
        # Simulated CSV data
        mock_csv = (
            "id;latlng;opening;closing;type\n"
            "1;(48.8566,2.3522);9;19;1\n"
            "2;(43.6045,1.4440);830;20;0\n"
        )
        encoded_csv = mock_csv.encode("utf-8")

        # Mock Azure blob
        mock_blob = MagicMock()
        mock_blob.download_blob.return_value.readall.return_value = encoded_csv

        # Mock container client
        mock_container_client = MagicMock()
        mock_container_client.get_blob_client.return_value = mock_blob

        # Mock BigQuery job
        mock_job = MagicMock()
        mock_job.output_rows = 2
        mock_job.result.return_value = None
        mock_client.load_table_from_dataframe.return_value = mock_job

        # Call the function under test
        with patch("src.test_technique_itm.silver.stores_silver.datetime") as mock_datetime:
            mock_now = MagicMock()
            mock_now.strftime.return_value = "2024-01-01 12:00:00"
            mock_datetime.datetime.now.return_value = mock_now

            process_stores_azure_to_silver(mock_container_client)

        # Check the DataFrame passed to BigQuery
        args, kwargs = mock_client.load_table_from_dataframe.call_args
        df = args[0]  # first argument = DataFrame

        # Ensure latitude and longitude are extracted and converted correctly
        self.assertIn("latitude", df.columns)
        self.assertIn("longitude", df.columns)

        self.assertAlmostEqual(df.loc[0, "latitude"], 48.8566)
        self.assertAlmostEqual(df.loc[0, "longitude"], 2.3522)
        self.assertAlmostEqual(df.loc[1, "latitude"], 43.6045)
        self.assertAlmostEqual(df.loc[1, "longitude"], 1.4440)

        # Check the time formatting
        self.assertEqual(df.loc[0, "opening"], "09:00")
        self.assertEqual(df.loc[1, "opening"], "08:30")
        self.assertEqual(df.loc[0, "closing"], "19:00")
        self.assertEqual(df.loc[1, "closing"], "20:00")


if __name__ == "__main__":
    unittest.main()
