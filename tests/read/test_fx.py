import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import desc
from rubik.load.markets import Markets

from joysticksecuritizatiotl3swp.read.fx import FX

class TestFX(unittest.TestCase):
    def setUp(self):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    @patch.object(Markets, 'read_fx_eod')
    def test_generic(self, mock_read_fx_eod):
        fx = self.spark.createDataFrame(
            [("USD", 0.85), ("EUR", 1.0)],
            schema="currency STRING, fx DOUBLE"
        )
        mock_read_fx_eod.return_value = fx
        dict_cols_to_convert = {"amount": "amount_in_eur"}
        df = self.spark.createDataFrame(
            [(1, "USD", 100.0), (2, "EUR", 200.0)],
            schema="id INT, currency_id STRING, amount DOUBLE"
        )
        self.fx_instance = FX(self.logger, self.dataproc)

        result = self.fx_instance.generic(df, dict_cols_to_convert)

        expected_data = [("EUR",2, 200.0, 1.0),("USD", 1, 85.0,0.85)]
        expected_df = self.spark.createDataFrame(
            expected_data,
            schema="currency_id STRING, id INT, amount_in_eur DOUBLE,fx DOUBLE"
        )

        self.assertEqual(result.collect(), expected_df.collect())
