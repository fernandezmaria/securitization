"""
This module contains the paths to the master tables the process uses
"""
from rubik.config.config import config_datalake


class Paths:  # pragma: no cover
    """
    Class containing the paths to the master tables
    """

    def __init__(self):
        """
        Constructor
        """
        # PATHS de las fuentes
        config1 = config_datalake('/data')
        self.config_deals = config1['GF']['deals'].copy()
        # config_items = config1['GF']['items']
        # config_balances_clan = config1['GF']['balances']
        # config_basemoto = config1['RISK']['risk_operations']
        self.config_economic_capital = config1['RISK']['econ_capital_info']
        self.config_reg_capital = config1['RISK']['regly_info_hold']
        self.config_ifrs9 = config1['RISK']['rating_ifrs9']

        # Paths Maestro
        self.config_cli_ksag = config1['KEY']['global_customer']
        self.config_cli_rel = config1['KEY']['global_customer_rel']
        self.path_members_rel = '/data/master/ksag/data/t_ksag_members_relation/'
        self.path_hold_group = '/data/master/ksag/data/t_ksag_holding_business_group/'
        # Taxonomy
        self.path_rel_cat = '/data/master/ktny/data/t_ktny_rel_values_taxonomy/gf_frequency_type=M/'
        self.path_cat = '/data/master/ktny/data/t_ktny_catalog_values_taxonomy/gf_frequency_type=M/'
        self.campo_date = 'gf_cutoff_date'
        # Rating Externo
        self.path_ext_rating = '/data/master/kctk/data/t_kctk_cust_ext_rating/g_entific_id=HO/'
        # Atributos de ratings de clientes
        self.path_cust_rating = '/data/master/kctk/data/t_kctk_cust_rating_atrb/g_entific_id=ES/'
        # Acreditadas
        self.path_acreditadas = '/data/master/kctk/data/t_kctk_accredited/'
