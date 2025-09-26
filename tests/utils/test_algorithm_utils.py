import unittest
from unittest import TestCase
from unittest.mock import MagicMock, patch


from pyspark.sql import SparkSession, DataFrame
from joysticksecuritizatiotl3swp.transform.portfolio_optimizer import PortfolioOptimizer
from pyspark.sql import functions as F

from joysticksecuritizatiotl3swp.utils.algorithm_utils import SecuritizationUtils


class TestSecuritizationUtils(TestCase):
    def setUp(self):

        # Create a Spark session
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()

        # Mock limits_processed_df with dummy values
        data_limits_processed = [
            ("escenario model verano IV", "2024-11-11", "risk_retention", "risk_bbva_percent", "total", None, None, 0.2,
             1, 1, "individual", 1, 0, 0),
            (
                "escenario model verano IV", "2024-11-11", "portfolio_type", "product_desc", "total", None, None,
                1.0000, 1,
                0, "portfolio", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "portfolio_size", "amount_size", "total", None, None,
             2000000000.0, 1, 1, "portfolio", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "maturity_min", "num_days", "365", None, None, 0.0, 1, 1,
             "individual", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "bei", "bei_flag", "1", None, None, 0.0, 1, 1, "individual", 1,
             0, 0),
            ("escenario model verano IV", "2024-11-11", "group", "group_id", "total", None, None, 0.0075, 1, 1,
             "portfolio", 1, 0, 0)
        ]
        schema_limits_processed = (
            "name_list_desc STRING, limit_date STRING, limit_type STRING, concept1_desc STRING, concept1_value STRING, "
            "concept2_desc STRING, concept2_value STRING, limit_value DOUBLE, corporate_loan_flag INT, project_finance_flag INT, "
            "limit_scope STRING, active_flag INT, visual_order INT, complex_limit INT"
        )
        self.limits_processed_df = self.spark.createDataFrame(data_limits_processed, schema=schema_limits_processed)


    def test_get_securization_type(self):
        result = SecuritizationUtils.get_securization_type(self.limits_processed_df)
        self.assertEqual(result, 'project_finance')

    def test_get_securitization_escenario_and_date(self):
        result = SecuritizationUtils.get_securitization_escenario_and_date(self.limits_processed_df)
        self.assertEqual(result, ("escenario model verano IV", "2024-11-11"))


