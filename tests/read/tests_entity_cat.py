import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from rubik.next.services.catalogues.entities import Entities
from joysticksecuritizatiotl3swp.read.entity_cat import EntityCatalogue

class TestEntityCatalogue(unittest.TestCase):

    def setUp(self):
        self.logger = MagicMock()
        self.dataproc = MagicMock()
        self.entity_catalogue = EntityCatalogue(self.logger, self.dataproc)

    @patch('rubik.next.services.catalogues.entities.Entities.get_entities_catalog')
    def test_get_entities_catalog(self, mock_get_entities_catalog):
        # Mock the return value of get_entities_catalog
        mock_df = MagicMock(spec=DataFrame)
        mock_get_entities_catalog.return_value = mock_df

        # Call the method
        result = self.entity_catalogue.get_entities_catalog()

        # Assertions
        mock_get_entities_catalog.assert_called_once()
if __name__ == '__main__':
    unittest.main()
