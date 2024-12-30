import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from joysticksecuritizatiotl3swp.read.securitizations import Securitizations

class TestSecuritizations(unittest.TestCase):

    def setUp(self):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        self.securitizations = Securitizations(self.logger, self.spark, self.dataproc)

    def test_securitization(self):
        # Mock the Securization class and its method
        with patch('joysticksecuritizatiotl3swp.read.securitizations.Securization') as MockSecurization:
            mock_securization_instance = MockSecurization.return_value
            mock_securization_instance.get_securization.return_value = self.spark.createDataFrame(
                [
                    (1, 1, 1, 0.5, 1, "2023-01-01"),
                    (1, 1, 1, 0.5, 2, "2023-01-02")
                ],
                schema="gf_fclty_trc_id INT, gf_facility_id INT, g_branch_id INT, gf_facility_securitization_per DOUBLE, gf_securitization_id INT, gf_odate_date_id STRING"
            )

            # Call the method
            result = self.securitizations.securitization()

            # Assertions
            self.assertIsInstance(result, DataFrame)
            self.assertTrue("gf_facility_securitization_per" in result.columns)
            self.assertTrue("gf_securitization_id" in result.columns)
            self.assertTrue("gf_odate_date_id" in result.columns)

if __name__ == '__main__':
    unittest.main()
