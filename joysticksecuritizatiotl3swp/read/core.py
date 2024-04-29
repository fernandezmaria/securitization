"""
Module to retrieve CORE information
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from rubik.load.ratings import Ratings
from rubik.load.risk_operations import RiskOperations

from joysticksecuritizatiotl3swp.read.fx import FX


class EconRegltyCapital:
    """This class builds the economic and regulatory capital information."""

    def __init__(self, logger, spark, dataproc, date_reg_econ_capital: str, fx: FX,
                 contract_relations: DataFrame):
        """
        Constructor
        """
        self.logger = logger
        self.spark = spark
        self.dataproc = dataproc
        self.fx = fx
        self.contract_relations = contract_relations
        self.rtg_score_letter_rel = Ratings('/data', self.spark, self.dataproc).get_score_letter_relations()
        risk_operations = RiskOperations(self.dataproc, '/data', date_reg_econ_capital)
        econ_capital_sel = (risk_operations.get_econ_capital_info().filter(
            F.col("g_entific_id").isin("ES", "MX")
        ))
        # Manage 999999999999999.99 values to 0, 999.999999 for _per fields
        self.econ_capital_sel = econ_capital_sel.select(
            *[F.when(F.col(c) == 999999999999999.99, F.lit(0)).otherwise(F.col(c)).alias(c)
              if c in ['gf_economic_capital_ead_amount', 'gf_ek_mitigated_el_adj_amount', 'gf_ek_adj_mit_dvrsfn_amount']
              else F.when(F.col(c) == 999.999999, F.lit(0)).otherwise(F.col(c)).alias(c)
              if c in ['gf_ek_aftr_mit_wght_pd_per'] else c for c in econ_capital_sel.columns])

        regl_capital_sel = risk_operations.get_regly_info_hold().filter(
            F.col("g_entific_id").isin("ES", "MX")
        )
        # Manage 999999999999999.99 values to 0, 999.999999 for _per fields
        self.regl_capital_sel = regl_capital_sel.select(
            *[F.when(F.col(c) == 999999999999999.99, F.lit(0)).otherwise(F.col(c)).alias(c)
              if c in ['gf_rce_amd_exposure_amount', 'gf_rce_adm_mit_captl_amount', 'gf_rce_adm_mit_el_amount']
              else F.when(F.col(c) == 999.999999, F.lit(0)).otherwise(F.col(c)).alias(c)
              if c in ['gf_aftr_mit_wght_pd_per', 'gf_rce_amd_appl_calc_lgd_per']
              else c for c in regl_capital_sel.columns])

    def build_econ_reglty_capital(self):
        """
        This method builds the economic and regulatory capital information and adds the local identifiers
        (delta_file_id, delta_file_band_id, branch_id) for later joining with CLAN.
        Parameters:
        ----------
        None
        Returns:
        -------
        This method returns the economic and regulatory capital information.
        """
        self.logger.info("EconRegltyCapital.build_econ_reglty_capital")
        econ_capital_df = self.fx.generic(
            self.econ_capital_sel,
            {'gf_economic_capital_ead_amount': 'gf_economic_capital_ead_amount',
             'gf_ek_mitigated_el_adj_amount': 'gf_ek_mitigated_el_adj_amount',
             'gf_ek_adj_mit_dvrsfn_amount': 'gf_ek_adj_mit_dvrsfn_amount'},
            currency_id_name='g_currency_id') \
            .selectExpr('g_contract_id', 'gf_economic_capital_ead_amount', 'gf_ek_aftr_mit_wght_pd_per',
                        'gf_ek_mitigated_el_adj_amount', 'gf_ek_adj_mit_dvrsfn_amount') \
            .join(self.contract_relations,
                  on='g_contract_id',
                  how='right')

        regl_capital_df = self.fx.generic(
            self.regl_capital_sel,
            {'gf_rce_amd_exposure_amount': 'gf_rce_amd_exposure_amount',
             'gf_rce_adm_mit_captl_amount': 'gf_rce_adm_mit_captl_amount',
             'gf_rce_adm_mit_el_amount': 'gf_rce_adm_mit_el_amount'},
            currency_id_name='g_local_currency_id') \
            .selectExpr('g_contract_id', 'gf_rce_amd_exposure_amount', 'gf_rce_adm_mit_captl_amount',
                        'gf_aftr_mit_wght_pd_per', 'gf_rce_amd_appl_calc_lgd_per', 'gf_rce_adm_mit_el_amount') \
            .join(self.contract_relations,
                  on='g_contract_id',
                  how='right')

        # Se calculan máximos en lugar de sumas como en el proceso mensual
        econ_capital_df = econ_capital_df \
            .groupBy('delta_file_id', 'delta_file_band_id', 'branch_id') \
            .agg(F.max('gf_economic_capital_ead_amount').alias('gf_economic_capital_ead_amount'),
                 F.max('gf_ek_mitigated_el_adj_amount').alias('gf_ek_mitigated_el_adj_amount'),
                 F.max('gf_ek_adj_mit_dvrsfn_amount').alias('gf_ek_adj_mit_dvrsfn_amount'),
                 (F.sum(F.col('gf_ek_aftr_mit_wght_pd_per') * F.col('gf_economic_capital_ead_amount')) /
                  F.sum('gf_economic_capital_ead_amount')).alias('pd_m5_mitig_per')
                 ) \
            .join(self.rtg_score_letter_rel.selectExpr('gf_from_prblty_dflt_per', 'gf_to_prblty_dflt_per',
                                                       'g_smscl_internal_ratg_type AS  smscl_econ_internal_ratg_type',
                                                       'g_lmscl_internal_ratg_type AS lmscl_econ_internal_ratg_type'),
                  on=((F.col('gf_from_prblty_dflt_per') <= 100 * F.col('pd_m5_mitig_per')) &
                      (100 * F.col('pd_m5_mitig_per') < F.col('gf_to_prblty_dflt_per'))), how='left') \
            .drop('gf_from_prblty_dflt_per', 'gf_to_prblty_dflt_per')
        regl_capital_df = regl_capital_df \
            .groupBy('delta_file_id', 'delta_file_band_id', 'branch_id') \
            .agg(F.max('gf_rce_amd_exposure_amount').alias('gf_rce_amd_exposure_amount'),
                 F.max('gf_rce_adm_mit_captl_amount').alias('gf_rce_adm_mit_captl_amount'),
                 (F.sum(F.col('gf_aftr_mit_wght_pd_per') * F.col('gf_rce_amd_exposure_amount')) /
                  F.sum('gf_rce_amd_exposure_amount')).alias('pd_ma_mitig_per'),
                 F.max('gf_rce_amd_appl_calc_lgd_per').alias('gf_rce_amd_appl_calc_lgd_per'),
                 F.max('gf_rce_adm_mit_el_amount').alias('gf_rce_adm_mit_el_amount')
                 ) \
            .join(self.rtg_score_letter_rel.selectExpr('gf_from_prblty_dflt_per', 'gf_to_prblty_dflt_per',
                                                       'g_smscl_internal_ratg_type AS  smscl_reg_internal_ratg_type',
                                                       'g_lmscl_internal_ratg_type AS lmscl_reg_internal_ratg_type'),
                  on=((F.col('gf_from_prblty_dflt_per') <= 100 * F.col('pd_ma_mitig_per')) &
                      (100 * F.col('pd_ma_mitig_per') < F.col('gf_to_prblty_dflt_per'))), how='left') \
            .drop('gf_from_prblty_dflt_per', 'gf_to_prblty_dflt_per')
        return econ_capital_df, regl_capital_df


class IFRS9:
    """This class builds the IFRS9 information."""

    def __init__(self, logger, spark, dataproc, date_ifrs9: str, fx: FX, contract_relations: DataFrame):
        """
        Constructor
        """
        self.logger = logger
        self.spark = spark
        self.dataproc = dataproc
        self.fx = fx
        self.contract_relations = contract_relations
        ifrs9 = (
            Ratings('/data', self.spark, self.dataproc, date_ifrs9)
            .get_ifrs9_rating(provision_type='N', entific_id=None).filter(
                F.col("entific_id").isin("ES", "MX")
            ))
        # Manage 999999999999999.99 values to 0
        self.ifrs9 = ifrs9.select(
            *[F.when(F.col(c) == 999999999999999.99, F.lit(0)).otherwise(F.col(c)).alias(c)
              if c in ['final_provision_amount', 'final_stage_type']
              else c for c in ifrs9.columns])

    def build_ifrs9(self):
        """
        This method builds the IFRS9 information and adds the local identifiers
        (delta_file_id, delta_file_band_id, branch_id) for later joining with CLAN.
        Parameters:
        ----------
        None
        Returns:
        -------
        This method returns the IFRS9 information.
        """
        self.logger.info("IFRS9.build_ifrs9")
        ifrs9 = (self.fx.generic(self.ifrs9, {'final_provision_amount': 'final_provision_amount'})
                 .select(F.substring('branch_id', 3, 4).alias('branch_id'),
                         'contract_id', 'entific_id', 'final_stage_type', 'final_provision_amount'))

        provisiones_df = self.contract_relations \
            .withColumn('entific_id', F.substring('g_contract_id', 0, 2)) \
            .withColumn('contract_id', F.substring('g_contract_id', 11, 26)) \
            .join(ifrs9,
                  on=['entific_id', 'contract_id', 'branch_id'],
                  how='inner') \
            .groupBy('delta_file_id', 'delta_file_band_id', 'branch_id') \
            .agg(F.sum('final_provision_amount').alias('final_provision_amount'),
                 F.max('final_stage_type').alias('final_stage_type')
                 )

        return provisiones_df

    def get_cust_watchlist(self):
        """
        This method retrieves watchlist values at g_customer_id level.

        Parameters:
        ----------
        None
        Returns:
        -------
        This method returns the watchlist information.
        """
        self.logger.info("IFRS9.get_cust_watchlist")
        watchlist = self.ifrs9.select(
            F.concat(
                F.when(F.col("entific_id") == "ES", F.lit("ES0182")).when(
                    F.col("entific_id") == "MX", F.lit("MX0074")
                ),
                F.col("customer_id"),
            ).alias("g_customer_id"),
            "watch_list_clasification_type",
        ).drop_duplicates(subset=["g_customer_id"])
        return watchlist
