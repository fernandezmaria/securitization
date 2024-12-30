import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from joysticksecuritizatiotl3swp.transform.securizations_transform import SecurizationsTransform

class TestSecuritizationsTransform(unittest.TestCase):

    @patch('joysticksecuritizatiotl3swp.read.catalogue_sector_project.CatalogueSectorProjectLoader.read_catalogue_sector_project_relation')
    def setUp(self, mock_read_catalogue_sector_project_relation):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.parameters = MagicMock()
        self.data_date = "2023-01-01"
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        self.limits_df = MagicMock()

        data = [
            ("sector1", "subsector1", "2024-12-12"),
            ("sector2", "subsector2", "2024-12-12"),
            ("sector3", "subsector3", "2024-12-12")
        ]

        # Define schema
        schema = "project_sector_desc STRING, project_subsector_desc STRING, closing_date STRING"
        df_project_sector_desc = self.spark.createDataFrame(data, schema=schema)

        # Patch the read_catalogue_sector_project_relation method
        mock_read_catalogue_sector_project_relation.return_value = df_project_sector_desc

        self.securitizations_transform = SecurizationsTransform(self.logger, self.dataproc, self.parameters,
                                                                  self.data_date, self.limits_df)

    @patch('joysticksecuritizatiotl3swp.transform.securizations_transform.Utilities.last_partition',
           return_value='2024-12-12')
    def test_build_ci_df(self,mock_last_partition):
        # Mock the raw_ci_df
        self.securitizations_transform.raw_ci_df = self.spark.createDataFrame(
            [
                ("2024-12-12""GF", 0.5, "desc1"),
                ("2024-12-12", "GF", 0.7, "desc2"),
                ("2024-12-12", "GE", 0.7, "desc2"),
            ],
            schema="gf_cutoff_date STRING, gf_business_area_id STRING, gf_customer_contract_control_per DOUBLE, gf_head_office_desc STRING"
        )

        # Call the build_ci_df method
        result = self.securitizations_transform.build_ci_df()

        # Expected result
        expected_output_df = self.spark.createDataFrame(
            [
                ( 0.5, "desc1"),
                ( 0.7, "desc2")
            ],
            schema="gf_customer_contract_control_per DOUBLE, gf_head_office_desc STRING"
        )

        # Assertions
        self.assertIsInstance(result, DataFrame)
        self.assertTrue("gf_customer_contract_control_per" in result.columns)
        self.assertTrue("gf_head_office_desc" in result.columns)
        self.assertEqual(result.collect(), expected_output_df.collect())

@patch('joysticksecuritizatiotl3swp.read.catalogue_sector_project.CatalogueSectorProjectLoader.read_catalogue_sector_project_relation')
def test_build_securization_for_algorithm(self, mock_read_catalogue_sector_project_relation):
    # Mock the catalogue_sector_project_df
    catalogue_sector_project = [
        ("sector1", "subsector1", "2024-12-12"),
        ("sector2", "subsector2", "2024-12-12"),
        ("sector3", "subsector3", "2024-12-12")
    ]
    schema_catalogue = "project_sector_desc STRING, project_subsector_desc STRING, closing_date STRING"
    df_catalogue_sector_project = self.spark.createDataFrame(catalogue_sector_project, schema=schema_catalogue)
    mock_read_catalogue_sector_project_relation.return_value = df_catalogue_sector_project

    # Mock the input securizations_df
    data_securizations = [
        ("sector1", "ICO España", "BB+1", 0.5, "S", "true", "true", 1, 1000),
        ("sector2", "Other", "BBB", 0.7, "N", "false", "false", 0, 0)
    ]
    schema_securizations = "project_sector_desc STRING, deal_purpose_type STRING, rating STRING, adj_lgd_ma_mitig_per DOUBLE, gf_pf_project_const_type STRING, sts_payment_condition STRING, sts_sm_rw_condition STRING, esg_linked INT, bei_guaranteed_amount INT"
    df_securizations = self.spark.createDataFrame(data_securizations, schema=schema_securizations)

    # Call the build_securization_for_algorithm method
    result = self.securitizations_transform.build_securization_for_algorithm(df_securizations)

    # Expected result
    expected_data = [
        ("sector1", "subsector1", "2024-12-12", 1, 0, 1, 1, 1, 1, 1, "2023-01-01"),
        ("sector2", "subsector2", "2024-12-12", 0, 0, 0, 0, 0, 0, 0, "2023-01-01")
    ]
    expected_schema = "project_sector_desc STRING, project_subsector_desc STRING, closing_date STRING, ico_flag INT, non_ig_flag INT, building_project_flag INT, workout_flag INT, sts_payment_flag INT, sts_sm_rw_flag INT, esg_linked_flag INT, bei_flag INT, data_date STRING"
    expected_output_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

    # Assertions
    self.assertIsInstance(result, DataFrame)
    self.assertEqual(result.collect(), expected_output_df.collect())
