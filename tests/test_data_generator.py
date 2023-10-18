import pytest
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from data_generator import DataGenerator


def set_seed():
    seed = 1234
    random.seed(seed)
    np.random.seed = seed


@pytest.fixture(autouse=True)
def reset_parameters():
    set_seed()
    yield
    set_seed()


@pytest.mark.parametrize('generator', [DataGenerator("2023-10-01", "2023-10-31")])
class TestGenerator:
    def test_get_activity(self, generator):
        activity = generator.get_activity()
        assert activity in ['Quiz', 'Video', 'Challenge']

    def test_get_stage(self, generator):
        stage = generator.get_stage()
        assert stage in ['start', 'complete', 'abandon', None]

    def test_get_timestamp(self, generator):
        timestamp = generator.random_timestamp()
        start_datetime = datetime.strptime(generator.start_date, "%Y-%m-%d")
        end_datetime = datetime.strptime(generator.end_date, "%Y-%m-%d")

        assert start_datetime <= timestamp <= end_datetime

    def test_update_timestamp(self, generator):
        date_string = "2023-10-29 23:50:47.564514"
        date_format = "%Y-%m-%d %H:%M:%S.%f"
        test_timestamp = datetime.strptime(date_string, date_format)

        updated_timestamp = generator.update_timestamp(test_timestamp)

        assert test_timestamp + timedelta(seconds=30) <= updated_timestamp <= test_timestamp + timedelta(seconds=120)

    def test_generate_user_ids(self, generator):
        num_ids = 10
        user_ids = generator.generate_user_ids(num_ids)
        assert isinstance(user_ids, list)
        assert len(user_ids) == num_ids
        for id in user_ids:
            assert 100000 <= id <= 999999

    def test_generate_records(self, generator):
        num_users = 10
        records_df = generator.generate_records(num_users)

        assert isinstance(records_df, pd.DataFrame)

        expected_columns = ['activity_id', 'timestamp', 'user_id', 'activity_stage', 'activity_type', 'score']
        assert all(col in records_df.columns for col in expected_columns)




