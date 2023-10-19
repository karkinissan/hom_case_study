import pandas as pd
from datetime import datetime
from google.cloud import storage
from activity_processor import ActivityProcessor


def list_bucket_files(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()

    blobs = storage_client.list_blobs(bucket_name)

    return [blob.name for blob in blobs if "raw_data" in blob.name]


def get_logs():
    query = "select filename from `thinking-heaven-281113.activity_tables.processing_logs`"
    df_logs = pd.read_gbq(query)
    return df_logs['filename'].values


def add_ingestion_timestamp(df):
    dt = datetime.utcnow()
    df['ingested_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
    df['ingested_at'] = pd.to_datetime(df['ingested_at'])
    return df


def process_activities(df):
    processor_instance = ActivityProcessor()

    df_quiz = processor_instance.processor(df, 'Quiz')

    df_challenge = processor_instance.processor(df, 'Challenge')

    df_video = processor_instance.processor(df, 'Video')

    df_quiz = add_ingestion_timestamp(df_quiz)
    df_challenge = add_ingestion_timestamp(df_challenge)
    df_video = add_ingestion_timestamp(df_video)

    return df_quiz, df_challenge, df_video


def upload_to_bq(df, table_name):
    df.to_gbq(f"thinking-heaven-281113.activity_tables_gcs.{table_name}", if_exists="append", progress_bar=False)


def update_logs(file_name):
    records = [{"filename": file_name}]
    df = pd.DataFrame(records)
    df = add_ingestion_timestamp(df)
    df.to_gbq(f"thinking-heaven-281113.activity_tables_gcs.processing_logs", if_exists="append", progress_bar=False)


def activity_process_gcs_to_bq(event, context):
    bucket_name = event['bucket']
    file_name = event['name']
    if bucket_name != "hom_case_study" or "raw_data" not in file_name:
        return {"Status": f"No relevant files to ingest."}
    file_path = f"gs://{bucket_name}/{file_name}"
    print(f"File to ingest: {file_path}")

    print(f"Processing file: {file_name}")
    df = pd.read_csv(file_path)
    df_quiz, df_challenge, df_video = process_activities(df)
    print("Uploading to BigQuery")
    upload_to_bq(df_quiz, f"quiz_table")
    upload_to_bq(df_challenge, f"challenge_table")
    upload_to_bq(df_video, f"video_table")

    print("Uploading logs")
    update_logs(file_name)
    print("Complete")
    return {"Status": f"Files ingested: {len(file_name)}"}


if __name__ == "__main__":
    activity_process_gcs_to_bq()
