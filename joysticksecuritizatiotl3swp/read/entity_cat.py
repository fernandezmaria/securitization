"""
Module to retrieve entity catalog information
"""
from rubik.next.services.catalogues.entities import Entities


class EntityCatalogue:
    """This class retrieves the entity catalog information."""

    def __init__(self, logger, dataproc):
        """
        Constructor
        """
        self.logger = logger
        self.dataproc = dataproc

    def get_entities_catalog(self):
        """
        Get entity catalog data

        Returns:
        -------
        Spark DataFrame with entity catalog data
        """
        self.logger.info("EntityCatalogue.get_entities_catalog -start")
        entity_cat_df = Entities(self.dataproc).get_entities_catalog()
        self.logger.info("EntityCatalogue.get_entities_catalog -end-")
        return entity_cat_df
