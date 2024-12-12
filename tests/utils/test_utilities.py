from unittest import TestCase
from unittest.mock import MagicMock, patch

from pyspark.sql import functions as F, Window as W
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from joysticksecuritizatiotl3swp.utils.utilities import Utilities

class TestUtilities(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()
        cls.key_cols = ['id']

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_drop_duplicates(self):
        data = [
            (1, '2023-01-01', 100),
            (1, '2023-01-02', 200),
            (2, '2023-01-01', 300),
            (2, '2023-01-03', 400)
        ]
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("limit_date", StringType(), True),
            StructField("value", IntegerType(), True)])
        df = self.spark.createDataFrame(data, schema)
        df = df.withColumn('limit_date', F.col('limit_date').cast('date'))

        result_df = Utilities.drop_duplicates(self,df)

        result_data = result_df.collect()

        expected_data = [
            (1, '2023-01-02', 200),
            (2, '2023-01-03', 400)
        ]

        self.assertEqual(len(result_data), len(expected_data))
        for row, expected in zip(result_data, expected_data):
            self.assertEqual(row['id'], expected[0])
            self.assertEqual(row['limit_date'].strftime('%Y-%m-%d'), expected[1])
            self.assertEqual(row['value'], expected[2])

