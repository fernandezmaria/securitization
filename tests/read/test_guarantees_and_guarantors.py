import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from joysticksecuritizatiotl3swp.read.guarantees_and_guarantors import GuaranteesAndGuarantorsLoader

class TestGuaranteesAndGuarantorsLoader(unittest.TestCase):

    def setUp(self):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        self.loader = GuaranteesAndGuarantorsLoader(self.logger, self.dataproc)

    @patch('joysticksecuritizatiotl3swp.read.guarantees_and_guarantors.ContractGuarantees.get_guarantee_assignments')
    @patch('joysticksecuritizatiotl3swp.read.guarantees_and_guarantors.ContractGuarantees.get_guarantee_struc_board')
    def test_get_guarantees_information(self, mock_get_guarantee_struc_board, mock_get_guarantee_assignments):
        # Mock the return value of get_guarantee_assignments
        mock_assignments_df = self.spark.createDataFrame(
            [(1, "G1", "USD", 100.0, "20230101", "20231231", "20230630")],
            schema="g_contract_id INT, g_guarantee_id STRING, g_countervalued_currency_id STRING, gf_ctvl_curr_guartd_amount DOUBLE, gf_beginning_guarantee_date STRING, gf_end_guarantee_date STRING, gf_cutoff_date STRING"
        )
        mock_get_guarantee_assignments.return_value = mock_assignments_df

        # Mock the return value of get_guarantee_struc_board
        mock_struc_board_df = self.spark.createDataFrame(
            [("G1", "C1")],
            schema="g_guarantee_id STRING, g_customer_id STRING"
        )
        mock_get_guarantee_struc_board.return_value = mock_struc_board_df

        # Call the method
        assignments, struc_board = self.loader.get_guarantees_information("20230630")

        # Assertions
        self.assertEqual(assignments.collect(), mock_assignments_df.collect())
        self.assertEqual(struc_board.collect(), mock_struc_board_df.collect())

