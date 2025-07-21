
from test_technique_itm.common import BlobServiceClient
from test_technique_itm.silver.clients_silver import process_clients_azure_to_silver
from test_technique_itm.gold.clients_gold import process_clients_silver_to_gold  
from test_technique_itm.silver.products_silver import process_products_azure_to_silver
from test_technique_itm.gold.products_gold import process_products_silver_to_gold 
from test_technique_itm.silver.stores_silver import process_stores_azure_to_silver
from test_technique_itm.gold.stores_gold import process_stores_silver_to_gold 
from test_technique_itm.silver.transactions_silver import process_transactions_azure_to_silver
from test_technique_itm.gold.transactions_gold import process_transactions_silver_to_gold 
from concurrent.futures import ThreadPoolExecutor, wait



def get_connection_string():
    with open("credentials/connection_string.txt", "r") as f:
        return f.read().strip()
    
def get_container_client(connection_string, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    return container_client

CONNECTION_STRING = get_connection_string()
CONTAINER_NAME = "76byj86oc9kf"
container_client = get_container_client(CONNECTION_STRING, CONTAINER_NAME)
    

def main():
    with ThreadPoolExecutor() as executor:  # Parallelization and orchestration of launching
        silver_to_gold_futures = []

        # Launch the 3 functions azure to silver for clients, products, stores in parallel
        clients_azure_to_silver = executor.submit(process_clients_azure_to_silver, container_client)
        products_azure_to_silver = executor.submit(process_products_azure_to_silver, container_client)
        stores_azure_to_silver = executor.submit(process_stores_azure_to_silver, container_client)

        # As soon as process_clients_azure_to_silver finishes, launch process_clients_silver_to_gold
        def client_callback(future):
            fut = executor.submit(process_clients_silver_to_gold)
            silver_to_gold_futures.append(fut)
        clients_azure_to_silver.add_done_callback(client_callback)

        # As soon as process_products_azure_to_silver finishes, launch process_products_silver_to_gold
        def product_callback(future):
            fut = executor.submit(process_products_silver_to_gold)
            silver_to_gold_futures.append(fut)
        products_azure_to_silver.add_done_callback(product_callback)

        # As soon as process_stores_azure_to_silver finishes, launch process_stores_silver_to_gold
        def store_callback(future):
            fut = executor.submit(process_stores_silver_to_gold)
            silver_to_gold_futures.append(fut)
        stores_azure_to_silver.add_done_callback(store_callback)

        # Wait for the 3 functions (clients/products/stores) to finish before launching transactions_azure_to_silver
        wait([clients_azure_to_silver, products_azure_to_silver, stores_azure_to_silver])

        # Wait for all silver_to_gold functions to finish
        wait(silver_to_gold_futures)

        # Launch transactions_azure_to_silver now that the 3 functions are done
        transactions_azure_to_silver = executor.submit(process_transactions_azure_to_silver, container_client)

        # As soon as transactions_azure_to_silver finishes, launch transactions_silver_to_gold
        def transactions_callback(future):
            process_transactions_silver_to_gold()
        transactions_azure_to_silver.add_done_callback(transactions_callback)

if __name__ == "__main__":
    main()