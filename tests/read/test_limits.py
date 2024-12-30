import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from joysticksecuritizatiotl3swp.read.limits import LimitsLoader

class TestLimitsLoader(unittest.TestCase):

    def setUp(self):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.parameters = MagicMock()
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        self.limits_loader = LimitsLoader(self.logger, self.dataproc, self.parameters)

    @patch('joysticksecuritizatiotl3swp.read.limits.Utilities.last_partition')
    def test_read_limits(self, mock_last_partition):
        # Mock the return value of last_partition
        mock_last_partition.return_value = "20230101"

        # Mock the DataFrame returned by dataproc.read().csv()
        mock_df = self.spark.createDataFrame(
            [("1", "1000,00"), ("2", "2000,00")],
            schema="id STRING, limit_value STRING"
        )
        self.dataproc.read().option().option().option().csv.return_value = mock_df

        # Call the method
        result = self.limits_loader.read_limits()

        # Assertions
        self.assertIsInstance(result, DataFrame)
        self.assertTrue("limit_value" in result.columns)
        self.logger.info.assert_called_with("Reading limit launchpad data for date 20230101")

    @patch('joysticksecuritizatiotl3swp.read.limits.Utilities.get_last_value_partition_table')
    def test_read_limit_field_relation(self, mock_get_last_value_partition_table):
        # Mock the return value of get_last_value_partition_table
        mock_get_last_value_partition_table.return_value = "20230101"

        # Mock the DataFrame returned by dataproc.read().table()
        mock_df = self.spark.createDataFrame(
            [("field1", "value1"), ("field2", "value2")],
            schema="field STRING, value STRING"
        )
        self.dataproc.read().table.return_value.filter.return_value = mock_df

        # Call the method
        result = self.limits_loader.read_limit_field_relation()

        # Assertions
        self.assertIsInstance(result, DataFrame)
        self.assertTrue("field" in result.columns)
        self.logger.info.assert_called_with("Read limit field relation for last available date 20230101")
