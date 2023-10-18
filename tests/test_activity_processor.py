import pytest
import pandas as pd

from activity_processor import ActivityProcessor


@pytest.fixture(autouse=True)
def df():
    df = pd.read_csv("activity_logs_test.csv", sep="\t")
    yield df


@pytest.mark.parametrize('processor', [ActivityProcessor()])
class TestProcessor:

    def test_get_activity(self, df, processor):
        df_act = processor.filter_activity_type(df, "Quiz")
        assert df_act['activity_type'].nunique() == 1
        assert df_act['activity_type'].unique()[0] == "Quiz"
        assert len(df_act) == 18

    def test_add_lead_columns_quiz(self, df, processor):
        df_lead = processor.add_lead_columns(df=df, is_scorable_activity=True)
        assert "ts_lead" in df_lead.columns
        assert "act_stg_lead" in df_lead.columns
        assert "score_lead" in df_lead.columns

    def test_add_lead_columns_video(self, df, processor):
        df_lead = processor.add_lead_columns(df=df, is_scorable_activity=False)
        assert "ts_lead" in df_lead.columns
        assert "act_stg_lead" in df_lead.columns
        assert "score_lead" not in df_lead.columns

    def test_remove_null_stages(self, df, processor):
        df_lead = processor.add_lead_columns(df=df, is_scorable_activity=True)
        df_no_null = processor.remove_null_stages(df=df_lead)
        assert sum(pd.isnull(df_no_null['act_stg_lead'])) == 0

    def test_activity_duration(self, df, processor):
        df_quiz = processor.filter_activity_type(df, "Quiz")
        df_quiz = processor.sort_dataframe(df_quiz)
        df_lead = processor.add_lead_columns(df=df_quiz, is_scorable_activity=True)
        df_duration = processor.calculate_activity_duration(df_lead)
        df_no_null = processor.remove_null_stages(df=df_duration)
        expected_results = pd.read_csv('activity_logs_quiz_result', sep="\t")['activity_duration'].values
        assert df_no_null['activity_duration'].values ==expected_results

    def test_drop_score_column(self, df, processor):
        assert 'score' in df.columns
        df_dropped = processor.drop_score_column(df)
        assert 'score' not in df_dropped.columns

    def test_processor_quiz(self, df, processor):
        df_quiz = processor.processor(df, "Quiz").reset_index(drop=True)
        expected_df = pd.read_csv('activity_logs_quiz_result', sep="\t")
        pd.testing.assert_frame_equal(df_quiz, expected_df)
