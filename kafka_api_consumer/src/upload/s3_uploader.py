import boto3
import json
import os
import pandas as pd
from datetime import datetime
from io import StringIO
from dotenv import load_dotenv
from utils.logger import setup_logger


#
logger = setup_logger()

# Load environment variables from .env file
load_dotenv()


class S3Uploader:
    def __init__(
        self,
        bucket_name: str = "ee-zm-dev-gernalp-frank",
        local_path: str = "./output_parquet/",
        upload_path: str = "etl-raw-data-p1-dev",
    ):
        self.bucket_name = bucket_name
        self.local_path = local_path
        self.upload_path = upload_path
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        #
        logger.info(
            f"bucket_name: {self.bucket_name} upload_path: {self.upload_path}")
        #
        if self.aws_access_key_id is None or self.aws_secret_key is None:
            logger.warning(
                "AWS credentials not found in environment variables")
            # raise ValueError(
            #     "AWS credentials not found in environment variables")

    def parse_datetime(self, date_string) -> datetime:
        '''Parse a datetime string into a datetime object'''
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            '%Y-%m-%d',
            '%d.%m.%Y',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y %I:%M %p'
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue

    def export_messages_datalake_S3(self, messages: list, topic: str):
        """CSV file upload to S3"""
        try:
            # convert messages to DataFrame
            data = []
            for message in messages:
                data.append(json.loads(message))

            df = pd.DataFrame(data)

            # file name with date - time stamp
            filename = f'{topic}_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv'

            # Initialize S3 client
            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_key,
            )
            # Convert DataFrame to CSV in memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            # Upload CSV to S3
            target_filename = f"{self.upload_path}/{filename}"
            s3.put_object(
                Bucket=self.bucket_name, Key=target_filename, Body=csv_buffer.getvalue()
            )

            print(
                f"file successfully written to s3://{self.bucket_name}/{target_filename}"
            )

        except Exception as e:
            print(f"Failed to store messages!! : {e}")
            print(f"tried path s3://{self.bucket_name}/{target_filename}")

    def export_messages_datalake_S3_parquet(
        self, df: pd.DataFrame, topic: str, partition_data_col: str = None
    ):
        """Parquet file upload to S3"""
        try:
            logger.info(f"Exporting messages to S3 as Parquet: {topic}")

            # looking for datetime column
            if not partition_data_col:
                # datetime_cols = [
                #     col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
                # logger.info(f"datetime_cols: {datetime_cols}")
                candidate_datetime_cols = [col for col in df.columns if col in [
                    "log_datetime", "event_time", "created_at", "updated_at", "timecreated", "timemodified"]]
                logger.info(
                    f"candidate_datetime_cols: {candidate_datetime_cols}")
                candidate_col_name = candidate_datetime_cols[0]
            else:
                candidate_col_name = partition_data_col

            # set partition column
            if candidate_col_name:
                df[candidate_col_name] = df[candidate_col_name].apply(
                    lambda x: pd.to_datetime(self.parse_datetime(x)))

                df["year"] = df[candidate_col_name].dt.year
                df["month"] = df[candidate_col_name].dt.month
                df["day"] = df[candidate_col_name].dt.day
                partition_cols = ["year", "month", "day"]

            # File name with date-time stamp
            # filename = f'{topic}_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.parquet'
            target_filename = f"{self.upload_path}/{topic}"
            logger.debug(f"Writing target_file:{target_filename}")
            # Initialize S3 client
            # s3 = boto3.client(
            #     "s3",
            #     aws_access_key_id=self.aws_access_key_id,
            #     aws_secret_access_key=self.aws_secret_key,
            # )

            # Create buffer for Parquet file
            # parquet_buffer = StringIO()
            # parquet_buffer = io.BytesIO()

            # If partition columns are provided, construct the partition path
            if candidate_col_name:
                # Validate partition columns exist in DataFrame
                if "year" not in df.columns:
                    raise ValueError(
                        f"Partition column {'year'} not found in DataFrame"
                    )

                # Convert DataFrame to Parquet with partitioning
                logger.info(
                    f"Writing Parquet file with partitioning: {partition_cols}"
                )
                df.to_parquet(
                    f"s3://{self.bucket_name}/{target_filename}",
                    engine="pyarrow",
                    compression="snappy",
                    index=False,
                    partition_cols=partition_cols,
                    storage_options={
                        "key": self.aws_access_key_id,
                        "secret": self.aws_secret_key,
                    },
                )
            else:
                logger.info("Writing Parquet file without partitioning")
                df.to_parquet(
                    f"s3://{self.bucket_name}/{target_filename}",
                    engine="pyarrow",
                    compression="snappy",
                    index=False,
                    storage_options={
                        "key": self.aws_access_key_id,
                        "secret": self.aws_secret_key,
                    },
                )

            # Upload to S3
            # s3.put_object(Bucket=self.bucket_name, Key=file_key, Body=parquet_buffer.getvalue())
            logger.info(
                f"Parquet file successfully written to s3://{self.bucket_name}/{target_filename}"
            )

        except Exception as e:
            logger.error(f"Failed to store messages as Parquet: {e}")
            logger.error(
                f"Attempted path: s3://{self.bucket_name}/{target_filename}")
            raise e
