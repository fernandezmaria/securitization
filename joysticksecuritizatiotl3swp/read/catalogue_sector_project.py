from pyspark.sql import functions as F

from joysticksecuritizatiotl3swp.read.paths import Paths
from joysticksecuritizatiotl3swp.utils.utilities import Utilities


class CatalogueSectorProjectLoader:
    """
    Class containing the function to retrieve the relation between sector and project glue table
    """

    def __init__(self, logger, dataproc, parameters):
        """
        Constructor
        """
        self.logger = logger
        self.parameters = parameters
        self.dataproc = dataproc
        self.paths = Paths(parameters=self.parameters)

        # Table read parameters
        self.sector_project_catalogue_table = self.paths.sector_project_catalogue_table
        self.sector_project_catalogue_date_field = "closing_date"

    def read_catalogue_sector_project_relation(self):
        """
        Read the relation between sector and project glue table.
        """
        last_date_available = Utilities.get_last_value_partition_table(self.sector_project_catalogue_table,
                                                                       self.sector_project_catalogue_date_field)

        catalogue_sector_project_df = (self.dataproc.read().table(self.sector_project_catalogue_table).
                                       filter(F.col(self.sector_project_catalogue_date_field) == last_date_available).
                                       withColumn('project_sector_desc', F.trim('project_sector_desc')))

        self.logger.info(f"Read sector project relation for last available date {last_date_available}")

        return catalogue_sector_project_df
