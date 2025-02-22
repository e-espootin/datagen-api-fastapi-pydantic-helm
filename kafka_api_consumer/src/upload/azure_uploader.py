from azure.storage.blob import BlobServiceClient
import json
import pandas as pd
from io import StringIO
from utils.logger import setup_logger
from dotenv import load_dotenv

#
logger = setup_logger()

# Load environment variables from .env file
load_dotenv()


class AzureUploader:
    def __init__(self, connection_string, container_name):
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        self.container_client = self.blob_service_client.get_container_client(
            container_name
        )

    # export_messages_datalake_Azure_Blob

    def export_messages_datalake_Azure_Blob(self, messages: list):
        try:
            # convert messages to DataFrame
            data = []
            for message in messages:
                data.append(json.loads(message))

            df = pd.DataFrame(data)

            # file name with date - time stamp
            filename = f'sensor_{self.topic_name}_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv'
            directory = (
                f"{self.BLOB_NAME}/{self.transformed_type}/{self.topic_name}/{filename}"
            )
            print(f"directory : {directory}")
            # Convert DataFrame to CSV in memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Initialize Azure Blob client
            blob_service_client = BlobServiceClient.from_connection_string(
                self.CONNECTION_STRING
            )
            blob_client = blob_service_client.get_blob_client(
                container=self.CONTAINER_NAME, blob=directory
            )

            # Upload CSV to Azure Blob
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

            print(
                f"DataFrame successfully written to Azure Blob storage: {self.CONTAINER_NAME}/{directory}"
            )

        except Exception as e:
            print(f"Failed to store messages in Azure Blob storage: {e}")
