"""
This module contains the paths to the master tables the process uses
"""
from rubik.config.config import config_datalake


class Paths:  # pragma: no cover
    """
    Class containing the paths to the master tables
    """

    def __init__(self, parameters):
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

        self.campo_date = 'gf_cutoff_date'

        # Paths Maestro
        self.config_cli_ksag = config1['KEY']['global_customer']
        self.config_cli_rel = config1['KEY']['global_customer_rel']
        self.path_members_rel = parameters['PATH_MEMBERS_REL']
        self.path_hold_group = parameters['PATH_HOLD_GROUP']
        # Taxonomy
        self.path_rel_cat = parameters['PATH_REL_CAT']
        self.path_cat = parameters['PATH_CAT']
        # Rating Externo
        self.path_ext_rating = parameters['PATH_EXT_RATING']
        # Atributos de ratings de clientes
        self.path_cust_rating = parameters['PATH_CUST_RATING']
        # Acreditadas
        self.path_acreditadas = parameters['PATH_ACREDITADAS']
