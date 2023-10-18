import pandas as pd


class ActivityProcessor:
    def __init__(self):
        pass

    def filter_activity_type(self, df, activity):
        """
        Filter DataFrame based on activity type.

        Args:
            df (pd.DataFrame): Input DataFrame.
            activity (str): The type of activity to filter.

        Returns:
            pd.DataFrame: Filtered DataFrame.

        """
        return df[df['activity_type'] == activity]

    def sort_dataframe(self, df):
        """
        Sort DataFrame based on activity_id and timestamp.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Sorted DataFrame.

        """
        return df.sort_values(['activity_id', 'timestamp'])

    def add_lead_columns(self, df, is_scorable_activity):
        """
        Add 'ts_lead', 'act_stg_lead', and 'score_lead' columns.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with lead columns.

        """
        df['ts_lead'] = df.groupby('activity_id')['timestamp'].shift(-1)
        df['ts_lead'] = df['ts_lead'].astype("Int64")
        df['act_stg_lead'] = df.groupby('activity_id')['activity_stage'].shift(-1)
        if is_scorable_activity:
            df['score_lead'] = df.groupby('activity_id')['score'].shift(-1)
        return df

    def remove_null_stages(self, df):
        """
        Remove rows where 'act_stg_lead' is null.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with null stages removed.

        """
        return df[~pd.isnull(df['act_stg_lead'])]

    def remove_invalid_quiz_rows(self, df):
        """
        Remove rows where 'act_stg_lead' is 'Quiz_complete' and 'score_lead' is null.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with invalid quiz rows removed.

        """
        return df[~(((df['act_stg_lead'] == "Quiz_complete") | (
                df['act_stg_lead'] == "Challenge_complete")) & pd.isnull(df['score_lead']))]

    def calculate_activity_duration(self, df):
        """
        Calculate 'activity_duration' as the difference between 'ts_lead' and 'timestamp'.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with activity duration calculated.

        """
        df['activity_duration'] = df['ts_lead'] - df['timestamp']

        return df

    def extract_status(self, df):
        """
        Extract 'status' from 'act_stg_lead'.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with 'status' column added.

        """
        df['status'] = df['act_stg_lead'].apply(lambda x: x.split("_")[1])

        return df

    def drop_score_column(self, df):
        """
        Drop the 'score' column.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with 'score' column dropped.

        """
        return df.drop(['score'], axis=1)

    def rename_columns(self, df):
        """
        Rename columns for clarity.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with renamed columns.

        """
        return df.rename(columns={"timestamp": "start_timestamp", "score_lead": "score"})

    def select_final_columns(self, df, is_scorable_activity):
        """
        Select the final set of columns based on activity type.

        Args:
            df (pd.DataFrame): Input DataFrame.
            is_quiz_activity (bool): Indicates if the activity is a Quiz.

        Returns:
            pd.DataFrame: DataFrame with final columns selected.

        """
        final_columns = ['activity_id', 'user_id', 'start_timestamp', 'activity_duration', 'status']
        if is_scorable_activity:
            final_columns.append("score")

        return df[final_columns]

    def processor(self, df, activity):
        """
        Process a DataFrame to extract relevant information based on the specified activity.

        Args:
            df (pd.DataFrame): Input DataFrame.
            activity (str): The type of activity to process.

        Returns:
            pd.DataFrame: Processed DataFrame.

        Raises:
            ValueError: If activity type is not one of ['Quiz', 'Challenge', 'Video'].

        """
        is_scorable_activity = activity in ["Quiz", "Challenge"]

        df_act = self.filter_activity_type(df, activity)
        df_act = self.sort_dataframe(df_act)
        df_act = self.add_lead_columns(df_act, is_scorable_activity)

        df_act = self.remove_null_stages(df_act)

        if is_scorable_activity:
            df_act = self.remove_invalid_quiz_rows(df_act)

        df_act = self.calculate_activity_duration(df_act)
        df_act = self.extract_status(df_act)
        df_act = self.drop_score_column(df_act)
        df_act = self.rename_columns(df_act)

        return self.select_final_columns(df_act, is_scorable_activity)


if __name__ == "__main__":
    df = pd.read_csv("activity_logs.csv", sep='\t')
    processor_instance = ActivityProcessor()
    # df_quiz = processor_instance.processor(df, 'Quiz')
    # print(df_quiz)

    # df_challenge = processor_instance.processor(df, 'Challenge')
    # print(df_challenge)

    df_video = processor_instance.processor(df, 'Video')
    print(df_video)

