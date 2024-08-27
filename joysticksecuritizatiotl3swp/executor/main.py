"""
Run process through this file
"""
import datetime
import re
from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from rubik.load.branches import Branches
from rubik.load.movements import Movements
from rubik.load.operations import Operations
from rubik.load.products import Products
from rubik.utils.partitions import PartitionsUtils

from joysticksecuritizatiotl3swp.configurations.catalogues import get_countries_iso_name_table
from joysticksecuritizatiotl3swp.configurations.catalogues import rating_dict
from joysticksecuritizatiotl3swp.configurations.constants import Constants
from joysticksecuritizatiotl3swp.read.core import EconRegltyCapital, IFRS9
from joysticksecuritizatiotl3swp.read.entity_cat import EntityCatalogue
from joysticksecuritizatiotl3swp.read.fx import FX
from joysticksecuritizatiotl3swp.read.guarantees_and_guarantors import GuaranteesAndGuarantorsBuilder
from joysticksecuritizatiotl3swp.read.guarantees_and_guarantors import GuaranteesAndGuarantorsLoader
from joysticksecuritizatiotl3swp.read.mcyg import Maestro
from joysticksecuritizatiotl3swp.read.paths import Paths
from joysticksecuritizatiotl3swp.read.securitizations import Securitizations
from joysticksecuritizatiotl3swp.transform.clan import ItemsBalance
from joysticksecuritizatiotl3swp.utils.dslb_writer import DSLBWriter


class SecuritizationProcess:  # pragma: no cover
    """
    Main class to execute the process
    """

    def __init__(self, logger, spark, dataproc, parameters):
        """
        Constructor
        """
        self.logger = logger
        self.spark = spark
        self.dataproc = dataproc
        self.parameters = parameters

        # Set spark configurations
        self.spark.conf.set('spark.sql.parquet.mergeSchema', 'true')
        self.spark.conf.set('spark.sql.execution.arrow.enabled', 'false')
        self.spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
        # Disabling the Vectorized Reader is NECESSARY as the output tables contain Decimal Types
        # This may cause the BI tool to fail when reading the tables
        self.spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

        # Sandbox writer
        self.dslb_writer = DSLBWriter(self.logger, self.dataproc)

        # Load paths
        self.paths = Paths(self.parameters)

        # Load constants
        self.constants = Constants(self.parameters)

        # Load output paths
        if parameters['OUTPUT_MODE'] in ['DEVELOPMENT', 'PRODUCTION']:
            self.sandbox_path = self.constants.CONFIG[parameters['OUTPUT_MODE']]['SANDBOX_PATH']
        else:
            raise ValueError('Invalid output mode! Please use DEVELOPMENT or PRODUCTION.')

        # Load reference CLAN date
        if not re.fullmatch(r"[0-9]{8}|(\bNOT\s+AVAILABLE\b)", parameters['CLAN_DATE']):
            raise ValueError('Invalid CLAN_DATE format! Please use YYYYMMDD or NOT AVAILABLE.')
        if parameters['CLAN_DATE'] == "NOT AVAILABLE":
            self.clan_date = None
        else:
            self.clan_date = parameters['CLAN_DATE']

    def execute_process(self) -> int:
        """
        Main method to execute the process
        """
        self.logger.info("Main.execute_process")

        # FECHA DE LAS TABLAS DE REGULATORIO Y ECONÓMICO: Última partición común disponible
        econ = self.dataproc.read().parquet(self.paths.config_economic_capital['path'])
        _, date_econ = (PartitionsUtils
                        .get_newest_date_data(data=econ.filter(F.col('g_entific_id').isin(['ES', 'MX'])),
                                              date_part_col=self.paths.config_economic_capital['column_date']))
        regl = self.dataproc.read().parquet(self.paths.config_reg_capital['path'])
        _, date_regl = (PartitionsUtils
                        .get_newest_date_data(data=regl.filter(F.col('g_entific_id').isin(['ES', 'MX'])),
                                              date_part_col=self.paths.config_reg_capital['column_date']))
        date_reg_econ_capital = min(set(date_econ).union(set(date_regl)))
        fecha_valor = datetime.datetime.strptime(date_reg_econ_capital, "%Y-%m-%d").date()
        self.logger.info('Date REGULATORIO Y ECONÓMICO: ' + str(date_reg_econ_capital))
        # FECHA DE CLAN: tomamos la más reciente o si se ha pasado una fecha, bajo demanda
        if self.clan_date is not None:
            date_clan = self.clan_date
        else:
            date_clan = max(
                PartitionsUtils.get_newest_date_data(data=self.dataproc.read().parquet(
                    self.paths.config_deals['path']),
                    date_part_col=self.paths.config_deals['column_date'])[1]).replace('-', '')

        self.logger.info('Date Clan: ' + str(date_clan))
        # Fecha IFRS9
        date_ifrs9 = max(
            PartitionsUtils.get_newest_date_data(data=self.dataproc.read().parquet(self.paths.config_ifrs9['path'])
                                                 .filter(F.col('provision_type') == 'N')
                                                 .filter(F.col('entific_id').isin('ES', 'MX')),
                                                 date_part_col=self.paths.config_ifrs9['column_date'])[1])
        self.logger.info('Date IFRS9: ' + str(date_ifrs9))
        # Fecha Maestro
        date_maestro = max(
            PartitionsUtils.get_newest_date_data(data=self.dataproc.read().parquet(self.paths.path_members_rel),
                                                 date_part_col=self.paths.campo_date)[1])
        self.logger.info('Date Maestro: ' + str(date_maestro))
        # Fecha Catálogos
        date_cat = max(
            PartitionsUtils.get_newest_date_data(data=self.dataproc.read().parquet(self.paths.path_rel_cat)
                                                 .filter(F.col('gf_frequency_type') == 'M'),
                                                 date_part_col=self.paths.campo_date)[1])
        self.logger.info('Date Taxonomy: ' + str(date_cat))
        # Fecha Rating Externo
        date_ext_rating = max(
            PartitionsUtils.get_newest_date_data(data=self.dataproc.read().parquet(self.paths.path_ext_rating)
                                                 .filter(F.col('g_entific_id') == 'HO'),
                                                 date_part_col=self.paths.campo_date)[1])
        self.logger.info('Date Rating Externo: ' + str(date_ext_rating))
        # Fecha Atributos ratings de clientes
        date_client_rat_attr = max(
            PartitionsUtils.get_newest_date_data(data=self.dataproc.read().parquet(self.paths.path_cust_rating)
                                                 .filter(F.col('g_entific_id') == 'ES'),
                                                 date_part_col=self.paths.campo_date)[1])
        self.logger.info('Date Atributos ratings de clientes: ' + str(date_client_rat_attr))
        # Fecha Acreditadas
        date_accredited = max(
            PartitionsUtils.get_newest_date_data(data=self.dataproc.read().parquet(self.paths.path_acreditadas),
                                                 date_part_col=self.paths.campo_date)[1])
        self.logger.info('Date Acreditadas: ' + str(date_accredited))

        # Perímetro de Oficinas: BBVA SA + IBF
        branches = Branches(self.dataproc, '/data', types_entity=['Global Finance'])
        branches_df = branches.getBranches()
        branches_df = (branches_df.filter(F.col('entity_id').isin('0182', '9016'))
                       .withColumn('rn',
                                   F.row_number().over(
                                       W.partitionBy('branch_id', 'entity_id').orderBy(F.desc('entity_product'))))
                       .filter(F.col('rn') == 1)
                       .drop('rn'))

        # Datos de Clan a nivel oficina
        product_all = Products(product_subtype=Products().getProductsList('GF'))
        product_all.getProductsList('GF')
        operations = Operations(self.dataproc, path_data='/data', data_date=date_clan, product=product_all)

        deals_op = operations.deals_operations(level='oficina', status=None)
        deals_bal = operations.balance_operations(levels_agg=None, status=None)

        # Añadir código de cliente y entidad globales
        deals_cust = (
            operations._deals_all_columns.filter(F.col("main_owner_type") == "SI")
            .filter(F.col("branch_role_type").isin("Participating Lender"))
            .withColumn(
                "rn",
                F.row_number().over(
                    W.partitionBy(
                        "delta_file_id", "delta_file_band_id", "branch_id", "entity_id"
                    ).orderBy("total_nominal_eur_amount")
                ),
            )
            .filter("rn==1")
            .drop("rn")
            .select(
                "delta_file_id",
                "delta_file_band_id",
                "entity_id",
                "branch_id",
                "g_customer_id",
                "g_entity_id",
            )
            .distinct()
        )
        saldos_oficina = deals_op.select('delta_file_id', 'delta_file_band_id',
                                         'project_country_id',
                                         'file_product_desc',
                                         'financial_product_desc',
                                         'seniority_name',
                                         'insured_type',
                                         'currency_id',
                                         'deal_signing_date',
                                         'expiration_date',
                                         'financial_product_class_desc',
                                         'customer_id',
                                         'borrower_country_id',
                                         'entity_id', 'branch_id',
                                         'operation_reference_id', 'page_id',
                                         'project_id', 'syndicated_type',
                                         'deal_purpose_type', 'project_sector_desc'). \
            join(deals_bal, on=['delta_file_id', 'delta_file_band_id',
                                "entity_id", "branch_id", 'currency_id'],
                 how='left'). \
            join(branches_df.select('branch_id', 'entity_product'), on=['branch_id'], how='inner') \
            .join(deals_cust,
                  on=["delta_file_id", "delta_file_band_id", "entity_id", "branch_id"],
                  how="left")

        # Cómputo de plazo_medio

        saldos_cruce = saldos_oficina.select(
            "delta_file_id",
            "delta_file_band_id",
            "entity_id",
            "branch_id",
            # "file_tranche_status_type",
            "deal_signing_date",
            "expiration_date",
        )

        items = operations.getOperationsProperties("items").getItems_total()
        items_balance = ItemsBalance(self.logger, self.dataproc)
        items = items_balance.cruce(items, saldos_cruce)

        evol_saldos = items_balance.narrow_down_movs(items)
        evol_saldos = items_balance.balance_evolution(evol_saldos, fecha_valor)

        list_year = [fecha_valor.year + x for x in range(0, 11)]
        col_year = ["imp_amortizado_y" + str(x) for x in range(0, 11)]

        _, runoff2 = items_balance.runoff(
            evol_saldos, fecha_valor, col_year, list_year
        )

        contract_relations = operations.get_deals_global_contract_relations(saldos_oficina)
        self.dslb_writer.write_df_to_sb(contract_relations, f'{self.sandbox_path}auxiliar',
                                        'contract_relations_securitizations', 'parquet', 'overwrite', 10)
        contract_relations = self.dataproc.read() \
            .parquet(f'{self.sandbox_path}auxiliar/contract_relations_securitizations')
        if contract_relations.count() == 0:
            raise Exception('contract_relations empty')
        # FX
        fx = FX(self.logger, self.dataproc)

        # Información de capital regulatorio y económico
        econ_reglty_capital = EconRegltyCapital(self.logger, self.spark, self.dataproc, date_reg_econ_capital, fx,
                                                contract_relations)
        econ_capital_df, regl_capital_df = econ_reglty_capital.build_econ_reglty_capital()

        # Info de IFRS9
        ifrs9_obj = IFRS9(self.logger, self.spark, self.dataproc, date_ifrs9, fx, contract_relations)
        provisiones_df = ifrs9_obj.build_ifrs9()
        unified_risk_df = econ_capital_df \
            .join(regl_capital_df, on=['delta_file_id', 'delta_file_band_id', 'branch_id'], how='left') \
            .join(provisiones_df, on=['delta_file_id', 'delta_file_band_id', 'branch_id'], how='left')

        # Base de Operaciones de Clan que nos vamos a quedar con los campos requeridos
        # y los unimos con los de regulatorio, económico e IFRS9
        ops_clan = (
            saldos_oficina.filter(F.col('bbva_commitment_amount') > 0)
            .join(unified_risk_df, on=['delta_file_id', 'delta_file_band_id', 'branch_id'],
                  how='left')
            .fillna({'smscl_econ_internal_ratg_type': 'No Rating',
                     'lmscl_econ_internal_ratg_type': 'No Rating',
                     'smscl_reg_internal_ratg_type': 'No Rating',
                     'lmscl_reg_internal_ratg_type': 'No Rating'})
            .fillna(0))

        ops_clan = ops_clan.join(
            runoff2.select(
                ["delta_file_id", "delta_file_band_id", "branch_id", "vto_medio"]
            ),
            on=["delta_file_id", "delta_file_band_id", "branch_id"],
            how="left",
        ).fillna(0).withColumn(
            "plazo_medio",
            F.datediff(F.to_date(F.col("vto_medio")), F.col("deal_signing_date")) / 365,
        ).fillna(0, subset=["plazo_medio"])

        # Persist data
        self.logger.info("cache: ops_clan")
        self.dslb_writer.write_df_to_sb(
            ops_clan,
            self.sandbox_path + "auxiliar",
            "auxiliar_clan_mrr",
            write_mode="overwrite",
        )
        self.spark.catalog.clearCache()
        self.logger.info("cache cleaned")
        self.logger.info("reading tables")

        ops_clan = self.dataproc.read().parquet(
            self.sandbox_path + "auxiliar/auxiliar_clan_mrr"
        )

        # Convertimos la escala de ratings a numérica con el diccionario
        mapping_expr = F.create_map(*[F.lit(x) for x in chain(*rating_dict.items())])
        ops_clan = ops_clan.withColumn('ma_expanded_master_scale_number',
                                       mapping_expr.getItem(F.trim(F.col('lmscl_reg_internal_ratg_type'))))
        ops_clan = ops_clan. \
            join(ifrs9_obj.get_cust_watchlist(),
                 on='g_customer_id', how='left') \
            .fillna({'watch_list_clasification_type': 0})

        # tomamos los valores de insured type a nivel expediente y lo aplicamos a todos los tramos de cada operación
        insured_type = operations.getOperationsProperties('deals')
        insured_type = insured_type.where(F.col('financial_product_desc') == 'Multitranche'). \
            select('delta_file_id', 'insured_type'). \
            drop_duplicates(). \
            withColumnRenamed('insured_type', 'insured_type_cabecera')
        insured_type_tramo_cero = operations.getOperationsProperties('deals') \
            .where(F.col('delta_file_band_id') == '0'). \
            select('delta_file_id', 'insured_type'). \
            drop_duplicates(). \
            withColumnRenamed('insured_type', 'insured_type_tramo_cero')

        ops_clan = ops_clan. \
            join(insured_type, on=['delta_file_id'], how='left'). \
            join(insured_type_tramo_cero, on=['delta_file_id'], how='left'). \
            withColumn('insured_type',
                       F.coalesce('insured_type', 'insured_type_cabecera', 'insured_type_tramo_cero')). \
            drop('insured_type_cabecera', 'insured_type_tramo_cero')

        # MAESTRO

        cli = self.dataproc.read().parquet(self.paths.config_cli_ksag['path']).filter(
            F.col(self.paths.config_cli_ksag['column_date']) == date_maestro)
        cli_rel = self.dataproc.read().parquet(self.paths.config_cli_rel['path']).filter(
            F.col(self.paths.config_cli_rel['column_date']) == date_maestro)
        members = self.dataproc.read().parquet(self.paths.path_members_rel).filter(
            F.col('gf_cutoff_date') == date_maestro)
        hold_group = self.dataproc.read().parquet(self.paths.path_hold_group).filter(
            F.col('gf_cutoff_date') == date_maestro)

        mcyg = Maestro(self.logger, self.dataproc)
        maestro_grupos = mcyg.read_mcyg_data(cli, cli_rel, members, hold_group)

        ops_clan = ops_clan.join(maestro_grupos.select('g_customer_id', 'g_holding_group_id'),
                                 on=['g_customer_id'], how='left')
        # Tabla de grupos para pegar el país de grupo
        ops_clan = ops_clan. \
            join(hold_group.select('g_holding_group_id', 'g_country_id', 'g_origin_country_id').
                 withColumnRenamed('g_country_id', 'group_country').
                 withColumnRenamed('g_origin_country_id', 'group_country2'),
                 on=['g_holding_group_id'], how='left')
        clientes = cli. \
            select('g_golden_customer_id',
                   'gf_country_ifo_id',
                   'gf_holding_activity_sector_id') \
            .drop_duplicates() \
            .join(mcyg.get_cust_id_relation(date_maestro.replace('-', '')),
                  on='g_golden_customer_id', how='left') \
            .drop('g_golden_customer_id')
        ops_clan = ops_clan.join(clientes, on=['g_customer_id'], how='left')

        # Proceso para pasar de la actividad holding a sector+subsector
        # Relacionar la actividad holding con el subsector de asset allocation y
        # con el sector
        rel_catalogos = self.dataproc.read().parquet(self.paths.path_rel_cat) \
            .filter(F.col('gf_frequency_type') == 'M') \
            .filter(F.col(self.paths.campo_date) == date_cat)
        rel_actividad_aa = rel_catalogos.where((F.col('gf_initial_catalog_id') == 'C039') &
                                               (F.col('gf_final_catalog_id') == 'C164')). \
            select('gf_initial_catalog_val_id', 'gf_final_catalog_val_id'). \
            withColumnRenamed('gf_initial_catalog_val_id', 'gf_holding_activity_sector_id'). \
            withColumnRenamed('gf_final_catalog_val_id', 'g_asset_allocation_subsec_type'). \
            drop_duplicates()
        rel_actividad_aa2 = rel_catalogos.where((F.col('gf_initial_catalog_id') == 'C039') &
                                                (F.col('gf_final_catalog_id') == 'C162')). \
            select('gf_initial_catalog_val_id', 'gf_final_catalog_val_id'). \
            withColumnRenamed('gf_initial_catalog_val_id', 'gf_holding_activity_sector_id'). \
            withColumnRenamed('gf_final_catalog_val_id', 'g_asset_class_id'). \
            drop_duplicates()
        cat = (self.dataproc.read().parquet(self.paths.path_cat)
               .filter(F.col('gf_frequency_type') == 'M')
               .filter(F.col(self.paths.campo_date) == date_cat))
        cat_subsector = cat.where(F.col('g_catalog_id') == 'C164'). \
            select('gf_catalog_val_id', 'gf_catlg_field_value_en_desc'). \
            drop_duplicates(). \
            withColumnRenamed('gf_catalog_val_id', 'g_asset_allocation_subsec_type'). \
            withColumnRenamed('gf_catlg_field_value_en_desc', 'g_asset_allocation_subsec_desc')
        cat_sector = cat.where(F.col('g_catalog_id') == 'C162'). \
            select('gf_catalog_val_id', 'gf_catlg_field_value_en_desc'). \
            drop_duplicates(). \
            withColumnRenamed('gf_catalog_val_id', 'g_asset_class_id'). \
            withColumnRenamed('gf_catlg_field_value_en_desc', 'g_asset_allocation_sector_desc')

        # SUBSECTOR Y SECTOR: Id's
        ops_clan = ops_clan. \
            join(rel_actividad_aa,
                 on=['gf_holding_activity_sector_id'], how='left')
        ops_clan = ops_clan. \
            join(rel_actividad_aa2,
                 on=['gf_holding_activity_sector_id'], how='left')
        # SUBSECTOR Y SECTOR: descripciones
        ops_clan = ops_clan. \
            join(cat_subsector,
                 on=['g_asset_allocation_subsec_type'], how='left'). \
            join(cat_sector,
                 on=['g_asset_class_id'], how='left')

        countries_df = get_countries_iso_name_table(self.spark)
        ops_clan = ops_clan. \
            join(countries_df.withColumnRenamed('country_id', 'group_country2').
                 withColumnRenamed('country_desc', 'group_country_desc'),
                 on=['group_country2'], how='left'). \
            join(countries_df.withColumnRenamed('country_id', 'gf_country_ifo_id').
                 withColumnRenamed('country_desc', 'customer_country'),
                 on=['gf_country_ifo_id'], how='left'). \
            join(countries_df.withColumnRenamed('country_id', 'project_country_id').
                 withColumnRenamed('country_desc', 'project_country_desc'),
                 on=['project_country_id'], how='left')

        # RATING S&P DE LA MATRIZ
        # Tomamos de maestro la relación grupo - matriz
        grupo_cli_matriz = maestro_grupos.where(F.col('gf_prtcpt_gr_header_type') == 'S') \
            .select('g_holding_group_id', 'g_customer_id') \
            .distinct() \
            .join(cli_rel.select('g_golden_customer_id', 'g_customer_id'),
                  on=['g_customer_id'], how='left')

        # Tomamos el rating externo de s&p
        rating_sp = (self.dataproc.read().parquet(self.paths.path_ext_rating)
                     .filter(F.col('g_entific_id') == 'HO')
                     .filter(F.col(self.paths.campo_date) == date_ext_rating))
        rating_sp = (rating_sp.withColumn('rn', F.row_number().over(W.partitionBy('g_golden_customer_id')
                                                                    .orderBy(F.desc('g_sp_lt_rating_fc_type'))))
                     .filter('rn==1').drop('rn').select('g_golden_customer_id', 'g_sp_lt_rating_fc_type'))
        # Unimos a cada grupo por su cliente matriz y pegamos a la base de clan por el id de grupo
        rating_sp_matriz = grupo_cli_matriz.drop('g_customer_id').distinct() \
            .join(rating_sp, on=['g_golden_customer_id'], how='inner')
        ops_clan = ops_clan.join(rating_sp_matriz.drop('g_golden_customer_id'), on=['g_holding_group_id'], how='left')
        # Añadimos información de acreditadas y atributos de clientes
        # Esta lectura debería dar unicidad de cliente, pero hay alguna errata. Se elimina duplicidad
        t_kctk_accredited = self.dataproc.read().parquet(self.paths.path_acreditadas).filter(
            F.col(self.paths.campo_date) == date_accredited) \
            .withColumn('rn',
                        F.row_number().over(W.partitionBy('gf_participant_id', 'gf_pf_acct_fin_ent_id')
                                            .orderBy(F.desc('gf_pf_ratg_date'),
                                                     F.desc('gf_contract_register_date')))) \
            .filter('rn==1') \
            .select(
            F.when(F.col('gf_pf_acct_fin_ent_id') == '0182', F.concat(F.lit('ES0182'), F.col('gf_participant_id')))
            .when(F.col('gf_pf_acct_fin_ent_id') == '9016', F.concat(F.lit('MX0074'), F.col('gf_participant_id')))
            .alias('g_customer_id'),
            'gf_capital_adjustment_desc',
            'gf_pf_project_const_type',
            'gf_sbprfl_mrch_risk_ind_type',
            'gf_pf_current_ratg_id',
            'gf_pf_score_ind_desc',
            'gf_pf_final_lgd_amount',
            F.substring('gf_pf_ratg_date', 1, 10).alias('gf_pf_ratg_date'))

        t_kctk_cust_rating_atrb = self.dataproc.read().parquet(self.paths.path_cust_rating).filter(
            F.col(self.paths.campo_date) == date_client_rat_attr) \
            .select('g_customer_id',
                    'gf_current_rating_tool_date',
                    'g_smscl_internal_ratg_type',
                    'g_lmscl_internal_ratg_type')

        ops_clan = ops_clan \
            .join(t_kctk_accredited,
                  how='left', on='g_customer_id') \
            .join(t_kctk_cust_rating_atrb,
                  how='left', on='g_customer_id')

        # Payment condition STS: At Least 1 Payment Made
        mvts = Movements('/data', self.dataproc).get_movements(origin_apps=['CLAN'], from_date=None, to_date=None) \
            .filter((F.col('gf_payment_situation_type_desc').isin('Manually Settled', 'Settled')) &
                    (F.col('gf_movement_class_desc').isin('Fees', 'Interest',
                                                          'Fee Payment', 'Repayment of principal')))\
            .select(F.col('gf_facility_id').alias('delta_file_id'),
                    F.col('gf_fclty_trc_id').alias('delta_file_band_id'),
                    F.col('gf_branch_id').alias('branch_id'),
                    F.lit(True).alias('sts_payment_condition'))\
            .distinct()

        ops_clan = ops_clan.join(mvts, on=['delta_file_id', 'delta_file_band_id', 'branch_id'], how='left')\
            .fillna({'sts_payment_condition': False})

        # Match Clan-Titularizaciones

        securitizations = Securitizations(self.logger, self.spark, self.dataproc)
        ops_clan = ops_clan.join(
            securitizations.securitization(),
            on=["delta_file_band_id", "delta_file_id", "branch_id"],
            how="left",
        ).fillna({"gf_securitization_id": "N"})
        ops_clan = ops_clan.withColumn(
            "gf_facility_securitization_amount",
            (F.col("gf_facility_securitization_per") / 100)
            * F.col("gf_rce_amd_exposure_amount"),
        ).fillna({"gf_facility_securitization_amount": 0})

        # Guarantees and guarantors
        guar_assignments, guar_struc_board = GuaranteesAndGuarantorsLoader(
            self.logger, self.dataproc
        ).get_guarantees_information(date_reg_econ_capital.replace("-", ""))
        guaranteed_amounts = GuaranteesAndGuarantorsBuilder(
            self.logger,
            self.dataproc,
            fx,
            contract_relations,
            guar_assignments,
            guar_struc_board,
            maestro_grupos
        ).build_guaranteed_amounts()
        ops_clan = ops_clan.join(
            guaranteed_amounts,
            on=["delta_file_id", "delta_file_band_id", "branch_id"],
            how="left",
        )

        # Risk Weight condition STS:
        ops_clan = ops_clan.withColumn('sts_sm_rw_condition', 100*F.col('gf_rw_sm_per') <= 100)\
            .fillna({'sts_sm_rw_condition': True})

        # Add entity description
        entities = (
            EntityCatalogue(self.logger, self.dataproc)
            .get_entities_catalog()
            .selectExpr("g_holding_entity_id AS entity_id", "gf_entity_desc AS banking_entity_desc")
        )
        ops_clan = ops_clan.join(
            entities,
            on="entity_id",
            how="left",
        )
        cubo_aud = ops_clan. \
            select('delta_file_id', 'delta_file_band_id', 'branch_id', 'project_id', 'project_country_desc',
                   'financial_product_desc', 'project_sector_desc', 'deal_purpose_type', 'seniority_name',
                   'insured_type', 'currency_id', 'expiration_date',
                   F.substring('deal_signing_date', 1, 10).alias('deal_signing_date'),
                   'syndicated_type', 'sts_payment_condition', 'gf_rw_sm_per', 'sts_sm_rw_condition',
                   'financial_product_class_desc', 'customer_id', 'g_customer_id', 'customer_country',
                   'g_holding_group_id', 'group_country_desc',
                   F.col("entity_id").alias("banking_entity_id"),
                   F.col("banking_entity_desc"),
                   F.col('lmscl_econ_internal_ratg_type').alias('m5_expanded_master_scale_id'),
                   F.col('lmscl_reg_internal_ratg_type').alias('gf_ma_expanded_master_scale_id'),
                   'ma_expanded_master_scale_number', 'pd_ma_mitig_per',
                   F.col('gf_economic_capital_ead_amount').alias('gf_m5_economic_ead_amount'),
                   F.col('gf_rce_amd_exposure_amount').alias('gf_ma_ead_amount'),
                   'pd_m5_mitig_per',
                   F.col('gf_rce_amd_appl_calc_lgd_per').alias('adj_lgd_ma_mitig_per'),
                   'g_asset_allocation_sector_desc', 'g_asset_allocation_subsec_desc', 'final_stage_type',
                   F.col('file_product_desc').alias('com_product'),
                   'bbva_drawn_amount', 'bbva_available_amount', 'bbva_drawn_eur_amount', 'bbva_available_eur_amount',
                   'total_nominal_amount', 'total_nominal_eur_amount',
                   F.col('g_sp_lt_rating_fc_type').alias('group_rating_sp'),
                   'watch_list_clasification_type',
                   F.col('gf_ek_adj_mit_dvrsfn_amount').alias('gf_m5_mitigation_rc_amount'),
                   F.col('gf_rce_adm_mit_captl_amount').alias('gf_ma_mitigation_rc_amount'),
                   F.col('gf_ek_mitigated_el_adj_amount').alias('gf_m5_total_el_amount'),
                   F.col('gf_rce_adm_mit_el_amount').alias('gf_ma_tot_expected_loss_amount'),
                   'gf_capital_adjustment_desc', 'gf_pf_project_const_type', 'gf_sbprfl_mrch_risk_ind_type',
                   'gf_pf_current_ratg_id', 'gf_pf_score_ind_desc', 'gf_pf_final_lgd_amount', 'gf_pf_ratg_date',
                   'gf_current_rating_tool_date', 'g_smscl_internal_ratg_type', 'g_lmscl_internal_ratg_type',
                   'gf_facility_securitization_amount', 'bei_guaranteed_amount', 'non_bei_guaranteed_amount',
                   'plazo_medio',
                   ). \
            withColumn('exchange_rate', F.col('total_nominal_amount') / F.col('total_nominal_eur_amount')). \
            withColumn('Total_Amount_CCY', F.col('bbva_drawn_amount') + F.col('bbva_available_amount')). \
            withColumn('Total_Amount_EUR', F.col('bbva_drawn_eur_amount') + F.col('bbva_available_eur_amount')). \
            withColumn('EC_per', F.col('gf_m5_mitigation_rc_amount') / F.col('gf_m5_economic_ead_amount')). \
            withColumn('RC_per', F.col('gf_ma_mitigation_rc_amount') / F.col('gf_ma_ead_amount')). \
            withColumn('Risk_Weight', 12.5 * F.col('RC_per')). \
            withColumn('EL_per', F.col('gf_m5_total_el_amount') / F.col('gf_m5_economic_ead_amount')). \
            withColumn('Reg_EL_per', F.col('gf_ma_tot_expected_loss_amount') / F.col('gf_ma_ead_amount')). \
            withColumn('Internal_Rating_EC_Model', F.lit('NA')). \
            withColumn('Importe_Garantizado_CCY', F.lit(0)). \
            withColumn('Importe_Garantizado_EUR', F.lit(0)). \
            withColumn('clan_date', F.lit(date_clan)). \
            withColumn('basemoto_date', F.lit(date_reg_econ_capital)). \
            withColumn('ifrs9_date', F.lit(date_ifrs9)). \
            drop('total_nominal_amount', 'total_nominal_eur_amount', 'gf_m5_mitigation_rc_amount',
                 'gf_ma_mitigation_rc_amount', 'gf_m5_total_el_amount', 'gf_ma_tot_expected_loss_amount')
        self.logger.info("Main.execute_process ended. Output count = " + str(cubo_aud.count()))

        # Write using DSLBWriter
        self.spark.conf.set('spark.sql.parquet.mergeSchema', 'false')
        self.dslb_writer.write_df_to_sb(cubo_aud, f'{self.sandbox_path}mrr/mrr_csv',
                                        'securitization_model_portfolio_' + date_clan + '.csv', 'csv', 'overwrite')
        self.dslb_writer.write_df_to_sb(cubo_aud, f'{self.sandbox_path}mrr', 'joystick_mrr', 'parquet', 'overwrite',
                                        partition_cols=['clan_date'])
        return 0
