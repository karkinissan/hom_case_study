import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta
import time


class DataGenerator:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    @staticmethod
    def get_activity():
        """
        A function to get a random activity type

        Returns:
            str: A randomly selected activity type.
        """
        activities = ['Quiz', 'Video', 'Challenge']
        act_weights = [0.4, 0.3, 0.3]
        return random.choices(activities, act_weights)[0]  # random.choices returns a list

    @staticmethod
    def get_stage():
        """
        A function to get a random activity stage
        Returns:
            str: A randomly selected stage type.
        """

        # We add a None stage to simulate an incomplete record.
        stages = ['start', 'complete', 'abandon', None]
        stg_weights = [0.0, 0.7, 0.2, 0.1]
        return random.choices(stages, stg_weights)[0]  # random.choices returns a list

    # Define a function to generate a random timestamp between two given dates

    def random_timestamp(self):
        """
        A function to generate a random timestamp between two given dates

        Returns:
            datetime.datetime: A randomly generated timestamp.
        """
        # Convert the input dates to datetime objects
        start_datetime = datetime.strptime(self.start_date, "%Y-%m-%d")
        end_datetime = datetime.strptime(self.end_date, "%Y-%m-%d")

        # Calculate the time range in seconds
        time_range = (end_datetime - start_datetime).total_seconds()

        # Generate a random number of seconds within the time range
        random_seconds = random.uniform(0, time_range)

        # Create a timedelta object with the random number of seconds
        random_timedelta = timedelta(seconds=random_seconds)

        # Add the random timedelta to the start datetime to get the random datetime
        random_datetime = start_datetime + random_timedelta

        return random_datetime

    @staticmethod
    def update_timestamp(timestamp):
        """
        Updates a given timestamp by adding a random timedelta between 30 seconds to 2 minutes.

        Args:
            timestamp (datetime.datetime): The timestamp to be updated.

        Returns:
            datetime.datetime: The updated timestamp.
        """
        # Create a timedelta object with seconds in the range of 30 seconds to 2 minutes.
        random_timedelta = timedelta(seconds=random.randint(30, 120))

        # Add the timedelta to the timestamp
        timestamp = timestamp + random_timedelta

        return timestamp

    @staticmethod
    def generate_user_ids(num):
        """
        Generates a list of random user IDs.

        Args:
            num (int): The number of user IDs to generate.

        Returns:
            list: A list of randomly generated user IDs.
        """
        return [random.randint(100000, 999999) for x in range(num)]

    def generate_records(self, num_users):
        """
        Generates a list of activity records for a specified number of users.

        Args:
            num_users (int): The number of users for which records will be generated.

        Returns:
            pandas.DataFrame: A DataFrame containing the generated activity records.
        """
        # Generate a list of 100 random user IDs
        user_ids = self.generate_user_ids(num_users)

        # Initialize an empty list to store activity records
        records = []

        # Loop through each user ID
        for user_id in user_ids:
            # Generate a random number of activities (between 1 and 3)
            num_activities = random.randint(1, 3)

            # Loop through each activity
            for act_idx in range(num_activities):
                # Generate a random activity ID
                activity_id = random.randint(10 ** 9, 10 ** 10 - 1)

                # Get a random activity type
                act = self.get_activity()

                # Generate a random timestamp between start_date and end_date
                timestamp = self.random_timestamp()

                # Loop through two stages (start and one additional stage)
                for i in range(2):
                    if i == 0:
                        stg = "start"
                    else:
                        stg = self.get_stage()
                    if not stg:
                        # The activity is left unfinished. Update the timestamp.
                        timestamp = self.update_timestamp(timestamp)
                        continue

                    # Generate an activity_stage string
                    act_stg = act + "_" + stg

                    # Create a dictionary with activity record information
                    rec = {"activity_id": activity_id,
                           "timestamp": int(time.mktime(timestamp.timetuple())),
                           "user_id": user_id,
                           "activity_stage": act_stg,
                           "activity_type": act,
                           "score": random.randint(0, 100) if act in ['Quiz',
                                                                      'Challenge'] and stg == 'complete' else None}

                    # Add the record to the list of records
                    records.append(rec)

                    # Update the timestamp for the next stage
                    timestamp = self.update_timestamp(timestamp)

        return pd.DataFrame(records)


if __name__ == "__main__":
    # Example Usage
    start_date = "2023-01-01"
    end_date = "2023-01-31"

    seed = 1234
    random.seed(seed)
    np.random.seed = seed

    data_generator = DataGenerator(start_date, end_date)
    df = data_generator.generate_records(100)
    print(df)
    df.to_csv("activity_logs.csv", index=False, sep="\t")