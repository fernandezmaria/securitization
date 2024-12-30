import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from joysticksecuritizatiotl3swp.read.esg_linked import ESGLinkedBuilder

class TestESGLinkedBuilder(unittest.TestCase):

    def setUp(self):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        self.contract_relations = self.spark.createDataFrame([], schema="g_contract_id STRING, delta_file_id STRING, delta_file_band_id STRING, branch_id STRING, g_glob_contract_hier_lvl_type STRING")
        self.esg_linked_builder = ESGLinkedBuilder(self.logger, self.dataproc, "2023-01-01", self.contract_relations)

    @patch('joysticksecuritizatiotl3swp.read.esg_linked.ContractSustLinked.get_contract_sust_linked')
    def test_build_esg_linked_flag(self, mock_get_contract_sust_linked):
        # Mock the return value of get_contract_sust_linked
        mock_df = self.spark.createDataFrame(
            [(1, "RBES", "contract_1"), (2, "RBES", "contract_2")],
            schema="id INT, gf_sustainability_mark_id STRING, g_contract_id STRING"
        )
        mock_get_contract_sust_linked.return_value = mock_df

        # Call the method
        result = self.esg_linked_builder.build_esg_linked_flag()

        # Assertions
        mock_get_contract_sust_linked.assert_called_once()
        self.assertIsInstance(result, DataFrame)
        self.assertTrue("esg_linked" in result.columns)

if __name__ == '__main__':
    unittest.main()
