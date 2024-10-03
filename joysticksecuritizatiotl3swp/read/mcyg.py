"""
This module is responsible for retrieving MCyG data
"""
import datetime
from itertools import chain

from pyspark.sql import functions as F, Window as W
from rubik.next.services.stakeholders.customers import CustomersService

from joysticksecuritizatiotl3swp.configurations.catalogues import (
    tier_dict,
    segment_dict,
)


class Maestro:  # pragma: no cover
    """
    Class containing the function to retrieve MCyG data
    """

    def __init__(self, logger, dataproc):
        """
        Constructor
        """
        self.dataproc = dataproc
        self.logger = logger

        # MCyG field selection
        self.c_cli = [
            "g_golden_customer_id",
            "gf_first_name as gf_customer_desc",
            "gf_holding_activity_sector_id as gf_holding_activity_sector_id_cliente",
            "gf_taxpayer_id as gf_ifo_id",
            "gf_country_ifo_id",
            "g_nace_activity_id",
        ]
        self.c_members = [
            "g_hier_level_type",
            "gf_main_relation_group_type",
            "gf_group_child_id as g_customer_id",
            "gf_group_parent_id as g_holding_group_id",
            "gf_business_group_header_type",
            "gf_risk_group_header_type",
            "gf_prtcpt_gr_header_type",
        ]
        self.c_hold_grupo = [
            "g_holding_group_id",
            "g_country_id",
            "gf_group_name",
            "gf_tier_id",
            "g_business_area_group_type",
            "g_customer_holding_activity_id as gf_holding_activity_sector_id_grupo",
            "g_management_country_id as gf_management_country_id",
            "gf_group_full_desc",
            "g_cib_segment_id",
            "gf_crm_group_id",
        ]

    def read_mcyg_data(self, cli, cli_rel, members, hold_grupo):
        """
        This method generates MCyG data for Internal Data Process
        Parameters:
        ----------
        cli, cli_rel, members, hold_grupo : pyspark.sql.DataFrame
        Returns:
        -------
        This method returns the joined data for CIB - MCyG. Type pyspark.sql.DataFrame
        """
        self.logger.info("Maestro.mcygdata")
        mapping_expr = F.create_map([F.lit(x) for x in chain(*tier_dict.items())])
        mapping_expr2 = F.create_map([F.lit(x) for x in chain(*segment_dict.items())])

        cli = cli.selectExpr(self.c_cli).dropDuplicates()

        cli_rel = cli_rel.select(
            "g_golden_customer_id", "g_customer_id"
        ).dropDuplicates()
        members = members.selectExpr(self.c_members).dropDuplicates()

        hold_grupo = hold_grupo.selectExpr(self.c_hold_grupo).dropDuplicates()

        members = (
            members.where(
                (F.col("g_hier_level_type").isin("CP", "CC", "SC"))
                & (F.col("gf_main_relation_group_type").isin("A", "B", "R"))
            )
            .withColumn("customer_id", F.substring("g_customer_id", 7, 9))
            .withColumn("cust_country", F.substring("g_customer_id", 1, 2))
            .withColumn("cust_entity", F.substring("g_customer_id", 3, 4))
        )

        window_rel_ppal = W.partitionBy("g_customer_id").orderBy(
            F.col("gf_main_relation_group_type").asc()
        )
        members = (
            members.withColumn("rank", F.rank().over(window_rel_ppal))
            .filter(F.col("rank") == 1)
            .drop("rank")
        )

        cli_final = cli.join(cli_rel, on=["g_golden_customer_id"], how="inner").join(
            members, on=["g_customer_id"], how="inner"
        )

        cli_final = cli_final.join(hold_grupo, on=["g_holding_group_id"], how="left")

        cli_final = cli_final.drop("g_hier_level_type", "gf_main_relation_group_type")

        mcygdata_df = (
            cli_final.withColumn("gf_group_name", F.trim(F.col("gf_group_name")))
            .withColumn("gf_group_full_desc", F.trim(F.col("gf_group_full_desc")))
            .withColumn("gf_tier_id_desc", mapping_expr.getItem(F.col("gf_tier_id")))
            .withColumn(
                "g_cib_segment_desc", mapping_expr2.getItem(F.col("g_cib_segment_id"))
            )
            .withColumn("customer_id", F.substring("g_customer_id", 7, 9))
        )
        return mcygdata_df

    def get_cust_id_relation(self, date_maestro):
        """
        This method translates between local and golden customer id
        Parameters:
        ----------
        dataproc : object
            Instance of class DatioPysparkSession
        date_maestro : string (YYYYMMDD)
            Maestro date
        Returns:
        ----------
        This method returns a dataframe with the relationship between local and golden customer id
        """
        date = datetime.datetime.strptime(date_maestro, "%Y%m%d").strftime("%Y-%m-%d")
        srvc = CustomersService(dataproc=self.dataproc)
        customers_rel = (
            srvc.get_customer_attrs(
                from_date=date, to_date=date, countries=["ES", "MX"]
            )
            .select("g_customer_id", "g_golden_customer_id")
            .distinct()
        )
        return customers_rel
