import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from joysticksecuritizatiotl3swp.read.mcyg import Maestro

class TestMaestro(unittest.TestCase):

    def setUp(self):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        self.maestro = Maestro(self.logger, self.dataproc)

    def test_read_mcyg_data(self):
        # Mock input DataFrames
        cli = self.spark.createDataFrame(
            [(1, "John", "Sector1", "123", "US", "NACE1", "SectorID1")],
            schema="g_golden_customer_id INT, gf_first_name STRING, gf_holding_activity_sector_id_cliente STRING, gf_taxpayer_id STRING, gf_country_ifo_id STRING, g_nace_activity_id STRING, gf_holding_activity_sector_id STRING"
        )
        cli_rel = self.spark.createDataFrame(
            [(1, 2)],
            schema="g_golden_customer_id INT, g_customer_id INT"
        )
        members = self.spark.createDataFrame(
            [("CP", "A", 2, 1, "Header1", "Header2", "Header3")],
            schema="g_hier_level_type STRING, gf_main_relation_group_type STRING, gf_group_child_id INT, gf_group_parent_id INT, gf_business_group_header_type STRING, gf_risk_group_header_type STRING, gf_prtcpt_gr_header_type STRING"
        )
        hold_grupo = self.spark.createDataFrame(
            [(1, "US", "Group1", "Tier1", "Type1", "Sector2", "Country1", "Desc1", "Segment1", "CRM1")],
            schema="g_holding_group_id INT, g_country_id STRING, gf_group_name STRING, gf_tier_id STRING, g_business_area_group_type STRING, g_customer_holding_activity_id STRING, g_management_country_id STRING, gf_group_full_desc STRING, g_cib_segment_id STRING, gf_crm_group_id STRING"
        )

        # Call the method
        result = self.maestro.read_mcyg_data(cli, cli_rel, members, hold_grupo)

        # Assertions
        self.assertIsInstance(result, DataFrame)
        self.assertTrue("gf_group_name" in result.columns)
        self.logger.info.assert_called_with("Maestro.mcygdata")

    @patch('joysticksecuritizatiotl3swp.read.mcyg.CustomersService.get_customer_attrs')
    def test_get_cust_id_relation(self, mock_get_customer_attrs):
        # Mock the return value of get_customer_attrs
        mock_df = self.spark.createDataFrame(
            [(2, 1)],
            schema="g_customer_id INT, g_golden_customer_id INT"
        )
        mock_get_customer_attrs.return_value = mock_df

        # Call the method
        result = self.maestro.get_cust_id_relation("20230101")

        # Assertions
        self.assertIsInstance(result, DataFrame)
        self.assertTrue("g_customer_id" in result.columns)

if __name__ == '__main__':
    unittest.main()
