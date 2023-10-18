from data_generator import DataGenerator
import random
import numpy as np
from google.cloud import storage
import time

seed = int(time.time())
random.seed(seed)
np.random.seed = seed


def generate_data():
    print(f"Dummy generation started. Seed: {seed}")
    start_date = "2023-01-01"
    end_date = "2023-01-31"

    data_generator = DataGenerator(start_date, end_date)
    df = data_generator.generate_records(100)

    client = storage.Client()
    bucket = client.get_bucket('hom_case_study')

    dest = f'raw_data/data_{seed}.csv'
    print(f"Uploading generated table to: {dest}")
    bucket.blob(dest).upload_from_string(df.to_csv(), 'text/csv')


if __name__ == "__main__":
    generate_data()
