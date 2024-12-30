import unittest
from unittest.mock import MagicMock, patch

from numpy.ma.testutils import assert_equal, assert_not_equal
from pyspark.sql import SparkSession, DataFrame
from joysticksecuritizatiotl3swp.transform.portfolio_optimizer import PortfolioOptimizer
from pyspark.sql import functions as F


class TestPortfolioOptimizer(unittest.TestCase):

    @patch('joysticksecuritizatiotl3swp.read.limits.LimitsLoader.read_limit_field_relation')
    @patch('dataproc_sdk.dataproc_sdk_catalog.datio_catalog.DatioCatalog.get')
    def setUp(self, mock_datio_catalog_get,mock_read_limit_field_relation):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.parameters = MagicMock()
        self.data_date = "2023-01-01"

        # Create a Spark session
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()

        # Mock limits_processed_df with dummy values
        data_limits_processed = [
            ("escenario model verano IV", "2024-11-11", "risk_retention", "risk_bbva_percent", "total", None, None, 0.2,
             1, 1, "individual", 1, 0, 0),
            (
            "escenario model verano IV", "2024-11-11", "portfolio_type", "product_desc", "total", None, None, 1.0000, 1,
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

        # Mock limits_processed_with_ctes_df with dummy values
        data_limits_processed_with_ctes = [
            ("escenario model verano IV", "2024-11-11", "risk_retention", "risk_bbva_percent", "total", None, None, 0.2,
             1, 1, "individual", 1, 0, 0),
            (
            "escenario model verano IV", "2024-11-11", "portfolio_type", "product_desc", "total", None, None, 1.0000, 1,
            0, "portfolio", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "portfolio_size", "amount_size", "total", None, None,
             2000000000.0, 1, 1, "portfolio", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "maturity_min", "num_days", "365", None, None, 0.0, 1, 1,
             "individual", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "bei", "bei_flag", "1", None, None, 0.0, 1, 1, "individual", 1,
             0, 0),
            ("escenario model verano IV", "2024-11-11", "group", "group_id", "total", None, None, 0.0075, 1, 1,
             "portfolio", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "constant_type", "Coupon", "total", None, None, 0.0500, 1, 1,
             "individual", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "constant_type", "B", "total", None, None, 3.5600, 1, 1,
             "portfolio", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "constant_type", "C", "total", None, None, -1.8500, 1, 1,
             "portfolio", 1, 0, 0),
            ("escenario model verano IV", "2024-11-11", "constant_type", "D", "total", None, None, 0.5500, 1, 1,
             "portfolio", 1, 0, 0)
        ]
        schema_limits_processed_with_ctes = (
            "name_list_desc STRING, limit_date STRING, limit_type STRING, concept1_desc STRING, concept1_value STRING, "
            "concept2_desc STRING, concept2_value STRING, limit_value DOUBLE, corporate_loan_flag INT, project_finance_flag INT, "
            "limit_scope STRING, active_flag INT, visual_order INT, complex_limit INT"
        )
        self.limits_processed_with_ctes_df = self.spark.createDataFrame(data_limits_processed_with_ctes,schema=schema_limits_processed_with_ctes)

        data_limit_field_relation = [
            ("portfolio_size", "gf_ma_ead_amount", "gf_ma_ead_amount", 0, 1, "limit_scope", 0, "2024-11-19"),
            ("portfolio_date", "clan_date", "clan_date", 0, 1, "limit_scope", 0, "2024-11-19"),
            ("portfolio_type", "com_product", "com_product", 0, 1, "limit_scope", 0, "2024-11-19"),
            ("risk_retention", "Total_Amount_EUR", "Total_Amount_EUR", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("non_ig", "non_ig_flag", "non_ig_flag", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("ccf", "gf_ma_ead_amount", "gf_ma_ead_amount", 0, 1, "limit_scope", 0, "2024-11-19"),
            ("rw", "gf_ma_ead_amount", "gf_ma_ead_amount", 0, 1, "limit_scope", 0, "2024-11-19"),
            (
            "building_project", "building_project_flag", "building_project_flag", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("rating_rga_esl", "gf_ma_expanded_master_scale_id", "g_lmscl_internal_ratg_type", 0, 0, "portfolio", 0,
             "2024-11-19"),
            ("rating_sp", "group_rating_sp", "gf_pf_current_ratg_id", 0, 0, "limit_scope", 1, "2024-11-19"),
            ("project_country", "project_country_desc", "project_country_desc", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("customer_country", "customer_country", "customer_country", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("project_sector_subsector", "project_sector_desc/project_subsector_desc",
             "project_sector_desc/project_subsector_desc", 1, 0, "limit_scope", 0, "2024-11-19"),
            ("customer_sector_subsector", "g_asset_allocation_sector_desc/g_asset_allocation_subsec_desc",
             "g_asset_allocation_sector_desc/g_asset_allocation_subsec_desc", 1, 0, "limit_scope", 0, "2024-11-19"),
            ("customer_sector", "g_asset_allocation_sector_desc", "g_asset_allocation_sector_desc", 0, 0, "limit_scope",
             0, "2024-11-19"),
            ("customer_subsector", "g_asset_allocation_subsec_desc", "g_asset_allocation_subsec_desc", 0, 0,
             "limit_scope", 0, "2024-11-19"),
            ("subsector", "project_subsector_desc", "project_subsector_desc", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("geo_sector", "g_asset_allocation_sector_desc/project_country_desc",
             "g_asset_allocation_sector_desc/project_country_desc", 1, 0, "limit_scope", 0, "2024-11-19"),
            ("financial_product", "financial_product_desc", "financial_product_desc", 0, 0, "limit_scope", 0,
             "2024-11-19"),
            ("divisa", "currency_id", "currency_id", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("excluded_facilities", "delta_file_id", "delta_file_id", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("excluded_borrowers", "customer_id", "customer_id", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("excluded_group", "g_holding_group_id", "g_holding_group_id", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("bei", "delta_file_id", "delta_file_id", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("ico", "ico_flag", "ico_flag", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("watchlist", "workout_flag", "workout_flag", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("maturity_min", "expiration_date", "expiration_date", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("maturity_max", "expiration_date", "expiration_date", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("group", "g_holding_group_id", "g_holding_group_id", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("project", "project_id", "project_id", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("no_esg_linked", "esg_linked_flag", "esg_linked_flag", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("sts_payment", "sts_payment_flag", "sts_payment_flag", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("sts_rw_modelo", "sts_sm_rw_flag", "sts_sm_rw_flag", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("sts_group", "g_holding_group_id", "g_holding_group_id", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("sts_project", "project_id", "project_id", 0, 0, "limit_scope", 0, "2024-11-19"),
            ("no_informed", "Total_Amount_EUR", "Total_Amount_EUR", 0, 0, "limit_scope", 0, "2024-11-19")
        ]
        schema_limit_field_relation = (
            "limit_type STRING, corporate_loan_column STRING, project_finance_column STRING, complex_flag INT, "
            "header_flag INT, imp_limit STRING, null_values INT, closing_date STRING"
        )
        limit_field_relation_df = self.spark.createDataFrame(data_limit_field_relation, schema=schema_limit_field_relation)
        mock_read_limit_field_relation.return_value = limit_field_relation_df

        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

        facilities_dummy_schema = StructType([
            StructField("project_sector_desc", StringType(), True),
            StructField("delta_file_id", StringType(), True),
            StructField("delta_file_band_id", StringType(), True),
            StructField("branch_id", StringType(), True),
            StructField("project_id", StringType(), True),
            StructField("project_country_desc", StringType(), True),
            StructField("financial_product_desc", StringType(), True),
            StructField("deal_purpose_type", StringType(), True),
            StructField("seniority_name", StringType(), True),
            StructField("insured_type", StringType(), True),
            StructField("currency_id", StringType(), True),
            StructField("expiration_date", StringType(), True),
            StructField("deal_signing_date", StringType(), True),
            StructField("syndicated_type", StringType(), True),
            StructField("sts_payment_condition", StringType(), True),
            StructField("gf_rw_sm_per", DoubleType(), True),
            StructField("sts_sm_rw_condition", StringType(), True),
            StructField("financial_product_class_desc", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("g_customer_id", StringType(), True),
            StructField("customer_country", StringType(), True),
            StructField("g_holding_group_id", StringType(), True),
            StructField("group_country_desc", StringType(), True),
            StructField("banking_entity_id", StringType(), True),
            StructField("banking_entity_desc", StringType(), True),
            StructField("m5_expanded_master_scale_id", StringType(), True),
            StructField("gf_ma_expanded_master_scale_id", StringType(), True),
            StructField("ma_expanded_master_scale_number", StringType(), True),
            StructField("pd_ma_mitig_per", DoubleType(), True),
            StructField("gf_m5_economic_ead_amount", DoubleType(), True),
            StructField("gf_ma_ead_amount", DoubleType(), True),
            StructField("pd_m5_mitig_per", DoubleType(), True),
            StructField("adj_lgd_ma_mitig_per", DoubleType(), True),
            StructField("g_asset_allocation_sector_desc", StringType(), True),
            StructField("g_asset_allocation_subsec_desc", StringType(), True),
            StructField("final_stage_type", StringType(), True),
            StructField("com_product", StringType(), True),
            StructField("bbva_drawn_amount", DoubleType(), True),
            StructField("bbva_available_amount", DoubleType(), True),
            StructField("bbva_drawn_eur_amount", DoubleType(), True),
            StructField("bbva_available_eur_amount", DoubleType(), True),
            StructField("group_rating_sp", StringType(), True),
            StructField("watch_list_clasification_type", StringType(), True),
            StructField("gf_capital_adjustment_desc", StringType(), True),
            StructField("gf_pf_project_const_type", StringType(), True),
            StructField("gf_sbprfl_mrch_risk_ind_type", StringType(), True),
            StructField("gf_pf_current_ratg_id", StringType(), True),
            StructField("gf_pf_score_ind_desc", StringType(), True),
            StructField("gf_pf_final_lgd_amount", DoubleType(), True),
            StructField("gf_pf_ratg_date", StringType(), True),
            StructField("gf_current_rating_tool_date", StringType(), True),
            StructField("g_smscl_internal_ratg_type", StringType(), True),
            StructField("g_lmscl_internal_ratg_typeg_lmscl_internal_ratg_type", StringType(), True),
            StructField("gf_facility_securitization_amount", DoubleType(), True),
            StructField("bei_guaranteed_amount", StringType(), True),
            StructField("non_bei_guaranteed_amount", StringType(), True),
            StructField("plazo_medio", DoubleType(), True),
            StructField("ind_rating", StringType(), True),
            StructField("ind_inv_grade", StringType(), True),
            StructField("esg_linked", StringType(), True),
            StructField("exchange_rate", DoubleType(), True),
            StructField("Total_Amount_CCY", DoubleType(), True),
            StructField("Total_Amount_EUR", DoubleType(), True),
            StructField("EC_per", DoubleType(), True),
            StructField("RC_per", StringType(), True),
            StructField("Risk_Weight", StringType(), True),
            StructField("EL_per", DoubleType(), True),
            StructField("Reg_EL_per", StringType(), True),
            StructField("Internal_Rating_EC_Model", StringType(), True),
            StructField("Importe_Garantizado_CCY", StringType(), True),
            StructField("Importe_Garantizado_EUR", StringType(), True),
            StructField("basemoto_date", StringType(), True),
            StructField("ifrs9_date", StringType(), True),
            StructField("project_subsector_desc", StringType(), True),
            StructField("closing_date", StringType(), True),
            StructField("ico_flag", StringType(), True),
            StructField("non_ig_flag", StringType(), True),
            StructField("building_project_flag", StringType(), True),
            StructField("workout_flag", StringType(), True),
            StructField("sts_payment_flag", StringType(), True),
            StructField("sts_sm_rw_flag", StringType(), True),
            StructField("esg_linked_flag", StringType(), True),
            StructField("bei_flag", StringType(), True),
            StructField("data_date", StringType(), True)
        ])

        facilities_dummy_data = [
            ("PF-ENE/O&G/GAS/GAS TRANSPORTATION NETWORKS", "838787", "0", "7906", "214600", "Brazil", "Term Loan",
             "General Corporate Purposes", "Senior", "Secured", "USD", "2031-09-26", "2023-09-25", "Syndicated", "true",
             0.000000, "true", "Amortizing + Balloon", "061268791", "ES0182061268791", "Brazil", "G00000000000395",
             "France", "0182", "BANCO BILBAO VIZCAYA ARGENTARIA S.A.", "BB1", "No Rating", "50", 0.000000,
             59731666.97000000, 0E-8, 0.007800, 0.450000000, "Utilities", "Gas T DSupply", "1", "Corporate Facilities",
             65000000.000000, 0.000000, 59731666.970000, 0.000000, "BBB+", "0", "No aplica", "N", "No", "BB", "Bueno",
             3.000000, "2024-09-23", "2024-09-23", "BB", "BB1", 0.000000, "null", "0E-8", 6.043835616438356, "21", "0",
             "0", 1.08819999999938, 65000000.000000, 59731666.970000, 0.005004, "null", "null", 0.000234, "null", "NA",
             "0", "0", "2024-10-31", "20241130", "GAS", "2024-11-19", "0", "1", "0", "0", "1", "1", "0", "0",
             "2024-11-11"),
            ("PF-ENE/O&G/GAS/GAS TRANSPORTATION NETWORKS", "838787", "0", "7906", "214600", "Brazil", "Term Loan",
             "General Corporate Purposes", "Senior", "Secured", "USD", "2031-09-26", "2023-09-25", "Syndicated", "true",
             0.000000, "true", "Amortizing + Balloon", "061268791", "ES0182061268791", "Brazil", "G00000000000395",
             "France", "0182", "BANCO BILBAO VIZCAYA ARGENTARIA S.A.", "BB1", "No Rating", "50", 0.000000,
             59731666.97000000, 0E-8, 0.007800, 0.450000000, "Utilities", "Gas T DSupply", "1", "Project Finance",
             65000000.000000, 0.000000, 59731666.970000, 0.000000, "BBB+", "0", "No aplica", "N", "No", "BB", "Bueno",
             3.000000, "2024-09-23", "2024-09-23", "BB", "BB1", 0.000000, "null", "0E-8", 6.043835616438356, "21", "0",
             "0", 1.08819999999938, 65000000.000000, 59731666.970000, 0.005004, "null", "null", 0.000234, "null", "NA",
             "0", "0", "2024-10-31", "20241130", "GAS", "2024-11-19", "0", "1", "0", "0", "1", "1", "0", "0",
             "2024-11-11")
        ]

        self.facilities_dummy_df =  self.spark.createDataFrame(facilities_dummy_data, schema=facilities_dummy_schema)

        # Mock the return value of DatioCatalog.get()
        mock_datio_catalog_get.return_value.listPartitionNames.return_value = ["2024-11-11", "2024-12-12"]

        self.portfolio_optimizer = PortfolioOptimizer(
            self.logger, self.dataproc, self.parameters, self.data_date,
            self.limits_processed_df, self.limits_processed_with_ctes_df
        )

    def test_get_limit_value(self):
        # Call the method to test
        result = self.portfolio_optimizer.get_limit_value(self.limits_processed_df)
        result_data = result.collect()

        for row in result_data:
            if row.limit_type == "risk_retention":
                self.assertAlmostEqual(row.limit_value, 0.800000011920929, places=7)
                self.assertIsInstance(row.limit_value, float)

    def test_calculate_limits_total(self):
        result = self.portfolio_optimizer.calculate_limits_total(self.limits_processed_df)
        self.assertIn("campo_datio", result.columns)

        null_count = result.filter(result.campo_datio.isNull()).count()
        self.assertEqual(null_count, 0)

    def test_build_facilities(self):
        result = self.portfolio_optimizer.build_facilities(self.facilities_dummy_df)
        assert_equal(result.count(),1)

    def test_build_portfolio_size(self):
        # Call the method to test
        result = self.portfolio_optimizer.build_portfolio_size(self.limits_processed_df)
        portfolio_size_exists = self.limits_processed_df.filter(F.col('limit_type') == 'portfolio_size').count() > 0
        self.assertTrue(portfolio_size_exists,
                        "limits_processed_df does not have a column limit_type with value portfolio_size")

        # Assertions
        self.assertIsInstance(result, float)
        assert_not_equal(result, 0.0)

    def test_get_individual_limits(self):
        limits_total = self.portfolio_optimizer.calculate_limits_total(self.limits_processed_df)
        result = self.portfolio_optimizer.get_individual_limits(limits_total)
        assert_equal(result.count(),3)


    def test_build_dict_individual_limits(self):
        # Call the method to test
        limits_total = self.portfolio_optimizer.calculate_limits_total(self.limits_processed_df)
        individual_limits_df = self.portfolio_optimizer.get_individual_limits(limits_total)
        result = self.portfolio_optimizer.build_dict_individual_limits(individual_limits_df)

        expected_dict = {'risk_retention': 'Total_Amount_EUR', 'maturity_min': 'expiration_date', 'bei': 'delta_file_id'}
        assert_equal(result, expected_dict)


    def test_get_individual_limits_with_null_values(self):
        # Call the method to test
        limits_total = self.portfolio_optimizer.calculate_limits_total(self.limits_processed_df)
        individual_limits_df = self.portfolio_optimizer.get_individual_limits(limits_total)
        result = self.portfolio_optimizer.get_individual_limits_with_null_values(individual_limits_df)

        expected_dict = {'risk_retention': 0, 'maturity_min': 0, 'bei': 0}

        assert_equal(result, expected_dict)

    def test_build_individual_limits_list(self):
        limits_total = self.portfolio_optimizer.calculate_limits_total(self.limits_processed_df)
        individual_limits_df = self.portfolio_optimizer.get_individual_limits(limits_total)
        individual_limits_dict = self.portfolio_optimizer.build_dict_individual_limits(individual_limits_df)

        result = self.portfolio_optimizer.build_individual_limits_list(individual_limits_dict)
        expected_list = ['limit_risk_retention', 'limit_maturity_min', 'limit_bei']
        assert_equal(expected_list, result)
