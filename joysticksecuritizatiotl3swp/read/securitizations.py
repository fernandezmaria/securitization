"""
This module contains the Securitizations class.
"""
from pyspark.sql import functions as F
from rubik.load.securization import Securization


class Securitizations:
    """Class containing securization data."""

    def __init__(self, logger, spark, dataproc):
        """
        Constructor
        """

        self.spark = spark
        self.dataproc = dataproc
        self.logger = logger

    def securitization(self):
        """
        This method loads the securitization data.

        Parameters:
        ----------
        None

        Returns:
        -------
        This method returns the securitization data (pyspark.sql.DataFrame).
        """

        self.logger.info("Titu.securitization")

        sc = Securization(path="/data", dataproc=self.dataproc)
        securitization_df = (
            sc.get_securization()
            .selectExpr(
                "gf_fclty_trc_id AS delta_file_band_id",
                "gf_facility_id AS delta_file_id",
                "g_branch_id AS branch_id",
                "gf_facility_securitization_per",
                "gf_securitization_id",
                "gf_odate_date_id",
            )
            .groupBy("delta_file_id", "delta_file_band_id", "branch_id")
            .agg(
                F.sum("gf_facility_securitization_per").alias(
                    "gf_facility_securitization_per"
                ),
                F.approx_count_distinct("gf_securitization_id").alias(
                    "gf_securitization_id"
                ),
                F.max("gf_odate_date_id").alias("gf_odate_date_id"),
            )
            .withColumn(
                "gf_securitization_id",
                F.when(F.col("gf_securitization_id") > 0, "Y").otherwise("N"),
            )
        )
        return securitization_df
