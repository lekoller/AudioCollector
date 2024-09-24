import datetime
import logging
import os
from azure.storage.blob import BlobServiceClient
import azure.functions as func

app = func.FunctionApp()
logging.basicConfig(level=logging.INFO)


@app.schedule(
    schedule="0 */5 * * * *", 
    arg_name="myTimer", 
    run_on_startup=True,
    use_monitor=False
) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    
    logging.info('Python timer trigger function started.')
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    container_name = os.getenv('CONTAINER_NAME')
    logging.info(f'Container name: {container_name}')
    storage_name = os.getenv('STORAGE_NAME')
    connection_string = os.getenv('CONNECTION_STRING')

    if not container_name or not storage_name or not container_name:
        logging.error('Environment variables CONTAINER_NAME, CONNECTION_STRING, or STORAGE_NAME are not set')
        return

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        blob_list = container_client.list_blobs()
        for blob in blob_list:
            blob_client = container_client.get_blob_client(blob)
            blob_data = blob_client.download_blob().readall()

            blob_name = blob.name
            logging.info(f"BLOB NAME = {blob_name}")

            processed_container_name = "processed"
            processed_container_client = blob_service_client.get_container_client(processed_container_name)
            if not processed_container_client.exists():
                processed_container_client.create_container()
                
            processed_blob_client = processed_container_client.get_blob_client(blob_name)
            processed_blob_client.upload_blob(blob_data, overwrite=True)

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    logging.info('Python timer trigger function executed.')