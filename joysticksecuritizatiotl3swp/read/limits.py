from pyspark.sql import functions as F

from joysticksecuritizatiotl3swp.configurations.catalogues import limits_column_mapping
from joysticksecuritizatiotl3swp.read.paths import Paths
from joysticksecuritizatiotl3swp.utils.utilities import Utilities


class LimitsLoader:
    """
    Class to retrieve limits data
    """

    def __init__(self, logger, dataproc, parameters):
        """
        Constructor
        """
        self.logger = logger
        self.dataproc = dataproc
        self.parameters = parameters
        self.paths = Paths(parameters=self.parameters)

        self.limits_datio_field_mapping_table = self.paths.limits_datio_field_mapping_table
        self.limits_datio_field_mapping_date_field = "closing_date"
        self.limits_table = self.paths.limits_launchpad_table
        self.limits_date_field = "gf_odate_date_id"

        self.mapped_columns = limits_column_mapping

    def rename_columns(self, df):
        """
        Rename columns of the DataFrame.
        """
        df = df.select([F.col(c).alias(self.mapped_columns.get(c, c)) for c in df.columns])

        return df

    def read_limits(self):
        """
        Read limits from launchpad
        """
        last_date_available = (
            Utilities.get_last_value_partition_table(
                self.limits_table,
                self.limits_date_field)
        )

        limits_df = (
            self.dataproc.read().table(self.limits_table).filter(F.col(self.limits_date_field) == last_date_available)
        )

        limits_df = (
            self.rename_columns(limits_df)
            .withColumn(
                'limit_value',
                F.regexp_replace('limit_value', ',', '.')
            )
        )

        self.logger.info(f"Reading limit launchpad data for date {last_date_available}")

        return limits_df

    def read_limit_field_relation(self):
        """
        Read the relation between limit fields and datio fields last available date..
        """
        last_date_available = (
            Utilities.get_last_value_partition_table(
                self.limits_datio_field_mapping_table,
                self.limits_datio_field_mapping_date_field)
        )

        limit_field_relation_df = (
            self.dataproc.read().table(self.limits_datio_field_mapping_table)
            .filter(F.col(self.limits_datio_field_mapping_date_field) == last_date_available)
        )

        self.logger.info(f"Read limit field relation for last available date {last_date_available}")

        return limit_field_relation_df
