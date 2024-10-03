"""
This module loads the guarantees and guarantors information
"""
from pyspark.sql import functions as F
from rubik.next.services.contracts.contract_guarantees import ContractGuarantees


class GuaranteesAndGuarantorsLoader:
    """This class provides information related to Guarantees and Guarantors (guaranteed amounts)."""

    def __init__(self, logger, dataproc):
        """
        Constructor
        """
        self.logger = logger
        self.dataproc = dataproc

    def get_guarantees_information(self, reference_date):
        """
        This method gets the guarantees information.

        Parameters:
        ----------
        reference_date : string (YYYYMMDD)
            Reference date
        Returns:
        -------
        This method returns the guaranteed amounts (pyspark.sql.DataFrame).
        """
        self.logger.info("GuaranteesAndGuarantors.get_guarantees_information")
        guarantees = ContractGuarantees(self.dataproc)
        guar_assignments = guarantees.get_guarantee_assignments(
            ["ES", "MX"], from_date=reference_date, to_date=reference_date
        ).select(
            "g_contract_id",
            "g_guarantee_id",
            "g_countervalued_currency_id",
            "gf_ctvl_curr_guartd_amount",
            "gf_beginning_guarantee_date",
            "gf_end_guarantee_date",
            "gf_cutoff_date",
        )
        guar_struc_board = guarantees.get_guarantee_struc_board(
            ["ES", "MX"], from_date=reference_date, to_date=reference_date
        ).select("g_guarantee_id", "g_customer_id")

        if guar_assignments.count() == 0:
            self.logger.warning(
                f"No guarantee assignments found for the given date: {reference_date}"
            )
            raise ValueError(
                f"No guarantee assignments found for the given date: {reference_date}"
            )
        if guar_struc_board.count() == 0:
            self.logger.warning(
                f"No guarantee structure board data found for the given date: {reference_date}"
            )
            raise ValueError(
                f"No guarantee structure board data found for the given date: {reference_date}"
            )
        return guar_assignments, guar_struc_board


class GuaranteesAndGuarantorsBuilder:
    """This class builds the guarantees and guarantors information."""

    def __init__(
        self,
        logger,
        dataproc,
        cross_fx,
        contract_relations,
        guar_assignments,
        guar_struc_board,
        cust_data,
    ):
        """
        Constructor
        """
        self.logger = logger
        self.dataproc = dataproc
        self.cross_fx = cross_fx
        self.contract_relations = contract_relations
        self.guar_assignments = guar_assignments
        self.guar_struc_board = guar_struc_board
        self.cust_data = cust_data

    def build_guaranteed_amounts(self):
        """
        This method builds the guaranteed amount information.

        Parameters:
        ----------
        None

        Returns:
        -------
        This method returns the guarantees and guarantors information.
        """
        self.logger.info("GuaranteesAndGuarantors.build_guaranteed_amounts")
        guar_assignments = (
            self.cross_fx.generic(
                self.guar_assignments,
                {"gf_ctvl_curr_guartd_amount": "gf_ctvl_curr_guartd_amount"},
                currency_id_name="g_countervalued_currency_id",
            )
            .filter(
                (F.col("gf_beginning_guarantee_date") <= F.col("gf_cutoff_date"))
                & (F.col("gf_cutoff_date") <= F.col("gf_end_guarantee_date"))
            )
            .selectExpr("g_contract_id", "g_guarantee_id", "gf_ctvl_curr_guartd_amount")
        )

        # European Investment Banking (BEI) flag
        cust_bei = self.cust_data.filter(
            F.col("g_golden_customer_id") == "A27721311"
        ).select("g_customer_id", F.lit(True).alias("BEI_flag"))
        guar_bei = (
            self.guar_struc_board.join(cust_bei, on="g_customer_id", how="left")
            .fillna(False, subset=["BEI_flag"])
            .select("g_guarantee_id", "BEI_flag")
        )

        guar_amounts = guar_assignments.join(
            guar_bei, on="g_guarantee_id", how="left"
        ).fillna(False, subset=["BEI_flag"])

        # Sum gf_ctvl_curr_guartd_amount and pivot depending on BEI_flag variable
        guar_amounts = (
            guar_amounts.groupBy("g_contract_id")
            .pivot("BEI_flag", [True, False])
            .agg(F.sum("gf_ctvl_curr_guartd_amount"))
            .withColumnRenamed("true", "bei_guaranteed_amount")
            .withColumnRenamed("false", "non_bei_guaranteed_amount")
        )

        # Join with contract_relations to get the local identifiers
        guar_amounts = (
            guar_amounts.join(self.contract_relations, on="g_contract_id", how="right")
            .groupBy("delta_file_id", "delta_file_band_id", "branch_id")
            .agg(
                F.sum("bei_guaranteed_amount").alias("bei_guaranteed_amount"),
                F.sum("non_bei_guaranteed_amount").alias("non_bei_guaranteed_amount"),
            )
        )

        return guar_amounts
