import decimal
import unittest
from datetime import date, datetime
from tokenize import String

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from unittest.mock import MagicMock, patch

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType, FloatType, \
    DoubleType
from rubik.load.markets import Markets

from joysticksecuritizatiotl3swp.transform.clan import ItemsBalance  # Replace with actual module name


class TestItemsBalance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        cls.logger = MagicMock()
        cls.dataproc = MagicMock()
        cls.items_balance = ItemsBalance(cls.logger, cls.dataproc)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_cruce(self):
        items_data = [(1, 2, "A", "B"), (2, 3, "C", "D")]
        balance_data = [(1, 2, "A", "B", 100), (2, 3, "C", "D", 200)]

        items = self.spark.createDataFrame(items_data,
                                           ["delta_file_id", "delta_file_band_id", "entity_id", "branch_id"])
        balance = self.spark.createDataFrame(balance_data,
                                             ["delta_file_id", "delta_file_band_id", "entity_id", "branch_id",
                                              "amount"])

        result = self.items_balance.cruce(items, balance)

        expected_schema = StructType([
            StructField("delta_file_id", IntegerType(), True),
            StructField("delta_file_band_id", IntegerType(), True),
            StructField("entity_id", StringType(), True),
            StructField("branch_id", StringType(), True),
            StructField("amount", IntegerType(), True)
        ])

        # Create the data (list of tuples)
        expected_data = [
            (1, 2, "A", "B", 100),
            (2, 3, "C", "D", 200)
        ]

        # Create the DataFrame
        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

        self.assertEqual(expected_df.collect(), result.collect())
        self.assertEqual(result.count(), 2)

    def test_narrow_down_movs(self):
        schema = StructType([
            StructField("delta_file_id", IntegerType(), True),
            StructField("delta_file_band_id", IntegerType(), True),
            StructField("delta_file_band_desc", StringType(), True),
            StructField("branch_id", StringType(), True),
            StructField("entity_id", StringType(), True),
            StructField("currency_id", StringType(), True),
            StructField("deal_signing_date", StringType(), True),
            StructField("expiration_date", StringType(), True),
            StructField("movement_class_name", StringType(), True),
            StructField("item_schedule_desc", StringType(), True),
            StructField("item_start_date", StringType(), True),
            StructField("item_end_date", StringType(), True),
            StructField("settlement_date", StringType(), True),
            StructField("movement_id", IntegerType(), True),
            StructField("base_item_id", IntegerType(), True),
            StructField("item_accounting_currency_amount", FloatType(), True),  # Example Decimal Type
            StructField("item_base_currency_amount", FloatType(), True),  # Example Decimal Type
            StructField("item_amount", FloatType(), True),  # Example Decimal Type
        ])

        # Create dummy data (list of tuples) - Adjust data types and values as needed
        data = [
            (1, 2, "Band 1", "Branch A", "Entity X", "USD", "2024-01-15", "2025-01-15", "Drawdown", "Schedule 1",
             "2024-02-01", "2024-02-15", "2024-02-20", 101, 201, 100.50, 110.25, 120.00),
            (2, 3, "Band 2", "Branch B", "Entity Y", "EUR", "2024-02-20", "2025-02-20", "Dummy", "Schedule 2",
             "2024-03-01", "2024-03-15", "2024-03-20", 102, 202, 200.00, 220.50, 240.25),
            # Add more dummy data as needed
        ]

        items = self.spark.createDataFrame(data, schema=schema)

        result = self.items_balance.narrow_down_movs(items)
        expected_data = [
            (1, 2, "Band 1", "Branch A", "Entity X", "USD", "2024-01-15", "2025-01-15", "Drawdown", "Schedule 1",
             "2024-02-01", "2024-02-15", "2024-02-20", 101, 201, 100.50, 110.25, 120.00)]

        expected_df = self.spark.createDataFrame(expected_data, schema = schema)
        result.printSchema()

        self.assertEqual(expected_df.collect(), result.collect())

    @patch.object(Markets, 'read_fx_eod')  # Patch Markets.read_fx_eod
    def test_balance_evolution(self, mock_read_fx_eod):  # Correct patch target
        # Sample FX data (as you provided it)
        schema_fx_dummy = StructType([
            StructField("currency", StringType(), True),
            StructField("fx", DecimalType(26, 6), True),
            StructField("ex_rate_date", DateType(), True)
        ])
        data_fx_dummy = [
            ("EUR", decimal.Decimal(1.000000), datetime(2024, 10, 13)),
            ("JPY", decimal.Decimal(0.006137), datetime(2024, 10, 13)),
            ("USD", decimal.Decimal(0.914244), datetime(2024, 10, 13))
        ]
        df_dummy_fx = self.spark.createDataFrame(data_fx_dummy, schema_fx_dummy)
        mock_read_fx_eod.return_value = df_dummy_fx  # Set the return value of the mock

        # Sample evol_saldos data (as you provided it)
        evol_saldos_data = [
            (1, 2, "Band 1", "Branch A", "Entity X", "USD", "2024-01-15", "2025-01-15", "Drawdown", "Schedule 1",
             "2024-02-01", "2024-02-15", "2024-02-20", 101, 201, 100.50, 110.25, 120.00)]
        evol_saldos_schema = StructType([
            StructField("delta_file_id", IntegerType(), True),
            StructField("delta_file_band_id", IntegerType(), True),
            StructField("delta_file_band_desc", StringType(), True),
            StructField("branch_id", StringType(), True),
            StructField("entity_id", StringType(), True),
            StructField("currency_id", StringType(), True),
            StructField("deal_signing_date", StringType(), True),
            StructField("expiration_date", StringType(), True),
            StructField("movement_class_name", StringType(), True),
            StructField("item_schedule_desc", StringType(), True),
            StructField("item_start_date", StringType(), True),
            StructField("item_end_date", StringType(), True),
            StructField("settlement_date", StringType(), True),
            StructField("movement_id", IntegerType(), True),
            StructField("base_item_id", IntegerType(), True),
            StructField("item_accounting_currency_amount", FloatType(), True),  # Example Decimal Type
            StructField("item_base_currency_amount", FloatType(), True),  # Example Decimal Type
            StructField("item_amount", FloatType(), True),  # Example Decimal Type
        ])

        evol_saldos_df = self.spark.createDataFrame(evol_saldos_data, schema=evol_saldos_schema)

        fecha_valor = "2024-01-15"
        result_df = self.items_balance.balance_evolution(evol_saldos_df, fecha_valor)

        expected_schema = StructType([
            StructField("delta_file_id", IntegerType(), True),
            StructField("delta_file_band_id", IntegerType(), True),
            StructField("branch_id", StringType(), True),
            StructField("deal_signing_date", StringType(), True),
            StructField("expiration_date", StringType(), True),
            StructField("item_start_date", StringType(), True),
            StructField("var_total", DoubleType(), True),
            StructField("var_dispuesto", DoubleType(), True),
            StructField("runoff", DoubleType(), True),
            StructField("var_total_acum", DoubleType(), True),
            StructField("var_dispuesto_acum", DoubleType(), True),
            StructField("item_end_date", StringType(), True),
            StructField("runoff_acum", DoubleType(), True),
        ])

        # Define the data
        expected_data = [
            (1, 2, "Branch A", "2024-01-15", "2025-01-15", "2024-01-15", 0.0, 0.0, 0.0, 0.0, 0.0, "2024-02-01", 0.0),
            (1, 2, "Branch A", "2024-01-15", "2025-01-15", "2024-02-01", 0.0, 0.0, 0.0, 0.0, 0.0, "2025-01-15", 0.0),
        ]

        # Create the DataFrame
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        self.assertEqual(expected_df.collect(), result_df.collect())

    def test_runoff(self):
        # Sample evol_saldos1 data (Corrected Schema and Data)
        data = [
            (1, 2, "Branch A", date(2024, 1, 15), date(2025, 1, 15), date(2024, 2, 1), 100.0, 50.0, 20.0),
            (1, 2, "Branch A", date(2024, 1, 15), date(2025, 1, 15), date(2024, 3, 1), 200.0, 100.0, 30.0),
            (1, 3, "Branch B", date(2024, 1, 15), date(2025, 1, 15), date(2024, 4, 1), 300.0, 150.0, 40.0)
        ]
        schema = StructType([
            StructField("delta_file_id", IntegerType(), True),
            StructField("delta_file_band_id", IntegerType(), True),
            StructField("branch_id", StringType(), True),
            StructField("deal_signing_date", DateType(), True),
            StructField("expiration_date", DateType(), True),
            StructField("item_start_date", DateType(), True),
            StructField("var_total", DoubleType(), True),
            StructField("var_dispuesto", DoubleType(), True),
            StructField("runoff", DoubleType(), True),
        ])
        evol_saldos1_df = self.spark.createDataFrame(data, schema)

        fecha_valor = "2024-01-31"  # Example date
        col_year = [f"imp_amortizado_y{i}" for i in range(11)]
        list_year = [2024 + i for i in range(11)]

        runoff1_df, runoff2_df = self.items_balance.runoff(evol_saldos1_df, fecha_valor, col_year, list_year)

        # Define the common schema
        expected_schema = StructType([
            StructField("delta_file_id", IntegerType(), True),
            StructField("delta_file_band_id", IntegerType(), True),
            StructField("branch_id", StringType(), True),
            StructField("importe_amortizado", DoubleType(), True),
            StructField("sumproduct", DoubleType(), True),
            StructField("imp_amortizado_y0", DoubleType(), True),
            StructField("imp_amortizado_y1", DoubleType(), True),
            StructField("imp_amortizado_y2", DoubleType(), True),
            StructField("imp_amortizado_y3", DoubleType(), True),
            StructField("imp_amortizado_y4", DoubleType(), True),
            StructField("imp_amortizado_y5", DoubleType(), True),
            StructField("imp_amortizado_y6", DoubleType(), True),
            StructField("imp_amortizado_y7", DoubleType(), True),
            StructField("imp_amortizado_y8", DoubleType(), True),
            StructField("imp_amortizado_y9", DoubleType(), True),
            StructField("imp_amortizado_y10", DoubleType(), True),
        ])

        # Data for runoff1_expected_df
        runoff1_expected_data = [
            (1, 2, "Branch A", 50.0, 3.694672E7, 50.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            (1, 3, "Branch B", 40.0, 2.955908E7, 40.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        ]

        # Create runoff1_expected_df
        runoff1_expected_df = self.spark.createDataFrame(runoff1_expected_data, expected_schema)

        # Data for runoff2_expected_df (includes vto_medio)
        runoff2_expected_data = [
            (1, 2, "Branch A", 50.0, 3.694672E7, 50.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "2024-02-18"),
            (1, 3, "Branch B", 40.0, 2.955908E7, 40.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "2024-04-01"),
        ]

        # Schema for runoff2 (includes vto_medio as StringType)
        runoff2_schema = expected_schema.add("vto_medio", StringType(), True)  # Add vto_medio

        # Create runoff2_expected_df
        runoff2_expected_df = self.spark.createDataFrame(runoff2_expected_data, runoff2_schema)

        self.assertEqual(runoff1_expected_df.collect(), runoff1_df.collect())
        self.assertEqual(runoff2_expected_df.collect(), runoff2_df.collect())
if __name__ == "__main__":
    unittest.main()
