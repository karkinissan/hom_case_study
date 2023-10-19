from data_generator import DataGenerator
import random
import numpy as np
from google.cloud import storage
import time


def generate_data(request=None):
    seed = int(time.time())
    random.seed(seed)
    np.random.seed = seed
    print(f"Dummy generation started. Seed: {seed}")
    start_date = "2023-01-01"
    end_date = "2023-01-31"

    data_generator = DataGenerator(start_date, end_date)
    df = data_generator.generate_records(100)

    client = storage.Client()
    bucket = client.get_bucket('hom_case_study')

    dest = f'raw_data/data_{seed}.csv'
    print(f"Uploading generated table to: {dest}")
    bucket.blob(dest).upload_from_string(df.to_csv(index=False), 'text/csv')
    return {"Status": "Success"}


if __name__ == "__main__":
    generate_data()
