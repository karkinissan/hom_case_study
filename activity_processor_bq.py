import pandas as pd
from datetime import datetime
from google.cloud import storage
from activity_processor import ActivityProcessor

bucket_name = "hom_case_study"


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
    df.to_gbq(f"thinking-heaven-281113.activity_tables.{table_name}", if_exists="append", progress_bar=False)


def update_logs(file_name):
    records = [{"filename": file_name}]
    df = pd.DataFrame(records)
    df = add_ingestion_timestamp(df)
    df.to_gbq(f"thinking-heaven-281113.activity_tables.processing_logs", if_exists="append", progress_bar=False)


def activity_process_to_bq():
    print("Getting file list in bucket")
    files_in_gcp = list_bucket_files(bucket_name)

    print("Getting file list in logs table")
    processed_files = get_logs()

    new_files = list(set(files_in_gcp) - set(processed_files))

    if len(new_files) == 0:
        print("No new files to ingest")
        return {"Status": "No files to ingest."}
    print(f"New files to ingest: {len(new_files)}")
    for file_name in new_files:
        print(f"Processing file: {file_name}")
        df = pd.read_csv(f"gs://{bucket_name}/{file_name}")
        df_quiz, df_challenge, df_video = process_activities(df)
        print("Uploading to BigQuery")
        upload_to_bq(df_quiz, f"quiz_table")
        upload_to_bq(df_challenge, f"challenge_table")
        upload_to_bq(df_video, f"video_table")

        print("Uploading logs")
        update_logs(file_name)
    print("Complete")
    return {"Status": f"Files ingested {len(new_files)}"}


if __name__ == "__main__":
    activity_process_to_bq()
