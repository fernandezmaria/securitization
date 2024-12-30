import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from rubik.load.ratings import Ratings

from joysticksecuritizatiotl3swp.read.core import EconRegltyCapital, IFRS9
from joysticksecuritizatiotl3swp.read.fx import FX

class TestEconRegltyCapital(unittest.TestCase):


    @patch.object(Ratings, 'get_score_letter_relations')
    def setUp(self, mock_get_score_letter_relations):
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.fx = MagicMock(spec=FX)
        self.contract_relations = MagicMock()
        mock_get_score_letter_relations.return_value = MagicMock()
        self.econ_reglty_capital = EconRegltyCapital(self.logger, self.spark, self.dataproc, "2023-01-01", self.fx, self.contract_relations)
        self.econ_reglty_capital.rtg_score_letter_rel = MagicMock()
        self.econ_reglty_capital.risk_operations = MagicMock()
        self.econ_reglty_capital.econ_capital_sel = MagicMock()
        self.econ_reglty_capital.regl_capital_sel = MagicMock()

    def test_build_econ_reglty_capital(self):
        result = self.econ_reglty_capital.build_econ_reglty_capital()
        self.assertEqual(len(result), 2)

class TestIFRS9(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.fx = MagicMock(spec=FX)
        self.contract_relations = MagicMock()
        self.ifrs9_class = IFRS9(self.logger, self.spark, self.dataproc,"2023-01-01", self.fx, self.contract_relations)
        self.ifrs9 = MagicMock()
    @patch('pyspark.sql.DataFrame.join')
    def test_build_ifrs9(self, mock_join):
        provisiones_df = self.ifrs9_class.build_ifrs9()
        self.assertEqual(mock_join.call_count, 1)
        #self.assertIsInstance(provisiones_df, DataFrame)

    def test_get_cust_watchlist(self):
        watchlist_df = self.ifrs9.get_cust_watchlist()
        self.assertIsInstance(watchlist_df, DataFrame)

if __name__ == '__main__':
    unittest.main()
