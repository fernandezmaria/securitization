import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from joysticksecuritizatiotl3swp.transform.limits_transform import LimitsTransform

class TestLimitsTransform(unittest.TestCase):

    def setUp(self):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.parameters = MagicMock()
        self.data_date = "2023-01-01"
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        self.limits_transform = LimitsTransform(self.logger, self.dataproc, self.parameters, self.data_date)

    @patch('joysticksecuritizatiotl3swp.transform.limits_transform.LimitsLoader.read_limits')
    @patch('joysticksecuritizatiotl3swp.utils.utilities.Utilities.drop_duplicates')
    def test_transform(self, mock_drop_duplicates, mock_read_limits):
        # Mock the read_limits method
        mock_read_limits.return_value = self.spark.createDataFrame(
            [
                ("escenario model verano IV", "2024-11-11", "portfolio_size", "amount_size", "total", None, None, "2.000.000.000,0000", 1, 1, "portfolio", 1, 0, 0, 0),
                ("escenario model verano IV", "2024-11-11", "portfolio_date", "securization_date", "2024-06-11", None, None, "1,0000", 1, 1, "portfolio", 1, 0, 0, 1),
                ("escenario model verano IV", "2024-11-11", "sts_group", "sts_flag", "1", "group_id", "total", "0,0200", 1, 1, "portfolio", 1, 0, 1, 225),
                ("escenario model verano IV", "2024-11-11", "sts", "sts_flag", "1", "group_id", "total", "0,0200",1, 1, "individual", 1, 0, 1, 225)
            ],
            schema="name_list_desc STRING, limit_date STRING, limit_type STRING, concept1_desc STRING, concept1_value STRING, concept2_desc STRING, concept2_value STRING, limit_value STRING, corporate_loan_flag INT, project_finance_flag INT, limit_scope STRING, active_flag INT, visual_order INT, complex_limit INT, id_limit INT"
        )

        # Mock the drop_duplicates method
        mock_drop_duplicates.return_value = mock_read_limits.return_value

        # Call the transform method
        result = self.limits_transform.transform()

        # Assertions
        self.assertIsInstance(result, DataFrame)
        expected_columns = [
            "name_list_desc", "limit_date", "limit_type", "concept1_desc", "concept1_value",
            "concept2_desc", "concept2_value", "limit_value", "corporate_loan_flag",
            "project_finance_flag", "limit_scope", "active_flag", "visual_order",
            "complex_limit", "id_limit"
        ]
        self.assertListEqual(result.columns, expected_columns)
