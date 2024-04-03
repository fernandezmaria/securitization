"""
Module to retrieve CORE information
"""
from pyspark.sql import functions as F


class EconRegltyCapital:
    """This class builds the economic and regulatory capital information."""
    def __init__(self, logger, econ_capital, regl_capital, contract_relations):
        """
        Constructor
        """
        self.logger = logger
        self.econ_capital_sel = econ_capital
        self.regl_capital_sel = regl_capital
        self.contract_relations = contract_relations

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
        econ_capital_df = self.econ_capital_sel\
            .withColumn('gf_economic_capital_ead_amount',
                        F.when(F.col('gf_economic_capital_ead_amount') == 999999999999999.99, F.lit(0))
                        .otherwise(F.col('gf_economic_capital_ead_amount'))) \
            .withColumn('gf_ek_mitigated_el_adj_amount',
                        F.when(F.col('gf_ek_mitigated_el_adj_amount') == 999999999999999.99, F.lit(0))
                        .otherwise(F.col('gf_ek_mitigated_el_adj_amount')))\
            .withColumn('gf_ek_adj_mit_dvrsfn_amount',
                        F.when(F.col('gf_ek_adj_mit_dvrsfn_amount') == 999999999999999.99, F.lit(0))
                        .otherwise(F.col('gf_ek_adj_mit_dvrsfn_amount')))\
            .selectExpr('g_contract_id', 'gf_economic_capital_ead_amount', 'gf_ek_aftr_mit_wght_pd_per',
                        'gf_ek_mitigated_el_adj_amount', 'gf_ek_adj_mit_dvrsfn_amount')\
            .join(self.contract_relations,
                  on='g_contract_id',
                  how='right')

        regl_capital_df = self.regl_capital_sel\
            .withColumn('gf_rce_amd_exposure_amount',
                        F.when(F.col('gf_rce_amd_exposure_amount') == 999999999999999.99, F.lit(0))
                        .otherwise(F.col('gf_rce_amd_exposure_amount'))) \
            .withColumn('gf_rce_adm_mit_captl_amount',
                        F.when(F.col('gf_rce_adm_mit_captl_amount') == 999999999999999.99, F.lit(0))
                        .otherwise(F.col('gf_rce_adm_mit_captl_amount')))\
            .withColumn('gf_rce_adm_mit_el_amount',
                        F.when(F.col('gf_rce_adm_mit_el_amount') == 999999999999999.99, F.lit(0))
                        .otherwise(F.col('gf_rce_adm_mit_el_amount')))\
            .selectExpr('g_contract_id', 'gf_rce_amd_exposure_amount', 'gf_rce_adm_mit_captl_amount',
                        'gf_aftr_mit_wght_pd_per', 'gf_rce_amd_appl_calc_lgd_per', 'gf_rce_adm_mit_el_amount')\
            .join(self.contract_relations,
                  on='g_contract_id',
                  how='right')
        return econ_capital_df, regl_capital_df
