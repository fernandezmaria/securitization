import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from joysticksecuritizatiotl3swp.configurations.catalogues import facility_type, \
    key_facility
from joysticksecuritizatiotl3swp.read.limits import LimitsLoader
from joysticksecuritizatiotl3swp.read.paths import Paths
from joysticksecuritizatiotl3swp.utils.algorithm_utils import SecuritizationUtils
from joysticksecuritizatiotl3swp.utils.utilities import Utilities


class PortfolioOptimizer:
    """
    Class to optimize the securitizacion porfolio using algorithm.
    """

    def __init__(self, logger, dataproc, parameters, data_date, limits_processed_df, limits_processed_with_ctes_df):
        """
        Constructor
        """
        self.logger = logger
        self.dataproc = dataproc
        self.parameters = parameters
        self.data_date = data_date
        self.paths = Paths(parameters=self.parameters)

        self.portfolio_size = self.build_portfolio_size(limits_processed_with_ctes_df)
        self.facility_type_dict = facility_type
        self.key_facility = key_facility
        self.securization_type = SecuritizationUtils.get_securization_type(limits_processed_df)
        self.limit_field_relation = LimitsLoader(logger, dataproc, parameters).read_limit_field_relation()
        self.securitization_escenario, self.securitization_date = SecuritizationUtils.get_securitization_escenario_and_date(
            limits_processed_df)

    def get_limit_value(self, limits_df):
        """
        Get the limit value from the limits DataFrame.
        """
        limit_value_df = limits_df.withColumn(
            'limit_value',
            F.when(
                F.col('limit_type') == 'risk_retention',
                (F.lit(1) - F.col('limit_value')).cast('float')
            )
            .otherwise(F.col('limit_value'))
        )

        return limit_value_df

    def calculate_limits_total(self, limits_df):
        """
        Calculate the total limits from the limits DataFrame filtering by securitization type.
        """
        field_relation_df = (self.limit_field_relation.drop('corporate_loan_column').
                             withColumnRenamed('project_finance_column', 'campo_datio'))

        if self.securization_type == 'corporate_loan':
            field_relation_df = (
                self.limit_field_relation.drop(
                    'project_finance_column'
                )
                .withColumnRenamed('corporate_loan_column', 'campo_datio')
            )

        total_limits_df = (
            limits_df.join(
                field_relation_df, ['limit_type'], 'left'
            )
            .dropDuplicates()
            .withColumn(
                'imp_limit',
                F.when(F.col('imp_limit') == 'limit_scope', F.col('limit_scope')).otherwise(F.col('imp_limit'))
            )
        )

        return total_limits_df

    def build_facilities(self, facilities_df):
        """
        Build the facilities DataFrame filtering by securitization type.
        """
        facilities_filtered_by_type_df = (
            facilities_df.where(
                F.col('com_product') == self.facility_type_dict[self.securization_type]
            )
            .withColumn('excluded', F.lit(0))
            .withColumn('exclusion_limit', F.lit(''))
        )

        self.logger.info("Building facilities for algorithm")

        return facilities_filtered_by_type_df

    def build_portfolio_size(self, total_limits_df):
        """
        Build the portfolio size from the total limits DataFrame.
        """
        portfolio_size = [x.limit_value for x in
                          total_limits_df.where(F.col('limit_type') == 'portfolio_size').select(
                              'limit_value').collect()][0]

        return float(portfolio_size)

    def build_dict_individual_limits(self, individual_limits_df):
        """
        Build individual limits dictionary from the individual limits DataFrame.
        """
        self.logger.info("Building individual limits dictionary")

        limit_field_list = individual_limits_df.select('limit_type', 'campo_datio').collect()
        dict_indiviual_limits = {row['limit_type']: row['campo_datio'] for row in limit_field_list}

        return dict_indiviual_limits

    def get_individual_limits(self, total_limits_df):
        """
        Get individual limits from the total limits DataFrame.
        """
        self.logger.info("Getting individual limits")

        individual_limits_df = total_limits_df.where(F.col('limit_scope') == 'individual')

        return individual_limits_df

    def get_individual_limits_with_null_values(self, individual_limits_df):
        """
        Get individual limits with null values from the DataFrame.
        """
        limit_types_null_list = individual_limits_df.select('limit_type', 'null_values').collect()
        dict_lim_ind_nul = {row['limit_type']: row['null_values'] for row in limit_types_null_list}
        return dict_lim_ind_nul

    def build_individual_limits_list(self, dict_lim_ind):
        """
        Build individual limits list from launchpad definition.
        """
        individual_limits_list = ['limit_' + k for k in dict_lim_ind]
        return individual_limits_list

    def apply_limites_individuales(self, limites_total, facilities_df, dict_lim_ind):
        """
        Build df with applied individual limits defined in launchpad.
        """
        self.logger.info("Applying individual limits")

        limits_indv = []

        # Limites necesarios
        lim_imp = ['risk_retention', 'ccf']

        limits_list = [
            (x.limit_type, x.limit_value)
            for x in limites_total.select('limit_type', 'limit_value')
            .where(F.col('limit_type').isin(*lim_imp)).collect()
        ]

        facilities_t1 = facilities_df.withColumn(
            'limit_escenario',
            F.lit(self.securitization_escenario)
        ).withColumn(
            'limit_fecha',
            F.lit(self.securitization_date)
        )

        for e in limits_list:
            facilities_t1 = facilities_t1.withColumn('limit_' + e[0], F.lit(e[1]))

        # Calculamos el importe
        facilities_f0 = (
            facilities_t1.withColumn(
                'importe1',
                F.col('bbva_drawn_eur_amount') + (F.col('bbva_available_eur_amount') * F.col('limit_ccf'))
            )
            .withColumn(
                'importe2',
                F.col('gf_facility_securitization_amount') / F.col('limit_risk_retention')
            )
            .withColumn(
                'importe_susceptible',
                F.least(F.col('gf_ma_ead_amount'), F.col('importe1')) - F.col('importe2')
            )
        )

        limites_ind = self.get_individual_limits(limites_total)
        dict_lim_ind_nul = self.get_individual_limits_with_null_values(limites_ind)
        limites_ind.show(230, False)

        facilities_add = facilities_f0.withColumn('limits_applied', F.lit(''))

        varios_campos = 0

        for k, v in dict_lim_ind.items():
            if ('/' in v):  # limite con varios campos viene con este separador en el catálogo de campos
                varios_campos = 1

            # CASO BASE: casi todos lo límites aplican a un solo campo
            if (varios_campos == 0):
                individual_limits_one_field_df = (
                    limites_ind.where(F.col('limit_type') == k).withColumn(
                        'concepto_valor',
                        F.when(F.col('complex_limit') == 1, F.col('concept2_value'))
                        .otherwise(F.col('concept1_value'))
                    )
                    .select('concepto_valor', 'limit_value')
                    .withColumnRenamed('concepto_valor', v)
                    .withColumnRenamed('limit_value', 'limit_' + k)
                    .withColumn('limit_' + k, F.col('limit_' + k).cast("float"))
                    .withColumn('limit_apply', F.lit('limit_' + k))
                )

                # limite generico para todas las facilities
                if ((individual_limits_one_field_df.count() == 1) & (individual_limits_one_field_df.select(v).collect()[0][v] == 'total')):
                    facilities_add = (
                        facilities_add.withColumn(
                            'limit_' + k,
                            F.lit(individual_limits_one_field_df.select('limit_' + k).collect()[0]['limit_' + k]).cast("float")
                        )
                        .withColumn(
                            'limit_apply',
                            F.lit('limit_' + k)
                        )
                    )

                # limite según categoria - valor
                else:
                    facilities_add = (
                        facilities_add.join(
                            individual_limits_one_field_df,
                            [v],
                            'left'
                        ).fillna(1.0).fillna({'limit_apply': ''})
                    )
                    # valor del limite para columnas nulas
                    num_limits_null_columns = facilities_add.where(F.col(v).isNull()).count()
                    if (num_limits_null_columns > 0):

                        # si hay columnas nulas para ese campo
                        facilities_add = (
                            facilities_add.withColumn(
                                'limit_' + k,
                                F.when(F.col(v).isNull(), dict_lim_ind_nul[k]).otherwise(F.col('limit_' + k))
                            )
                        )

            # Limit applied to several fields
            else:
                v1, v2 = v.split('/')

                individual_limits_several_fields_df = (
                    limites_ind.where(
                        F.col('limit_type') == k
                    )
                    .select('concept1_value', 'concept2_value', 'limit_value')
                    .withColumnRenamed('concept1_value', v1)
                    .withColumnRenamed('concept2_value', v2)
                    .withColumnRenamed('limit_value', 'limit_' + k)
                    .withColumn('limit_' + k, F.col('limit_' + k).cast("float"))
                    .withColumn('limit_apply', F.lit('limit_' + k))
                )

                # limite generico para todas las facilities
                if (individual_limits_several_fields_df.count() == 1) & (individual_limits_several_fields_df.select(v1).collect()[0][v1] == 'total'):
                    if (individual_limits_several_fields_df.count() == 1) & (individual_limits_several_fields_df.select(v2).collect()[0][v2] == 'total'):
                        facilities_add = (
                            facilities_add.withColumn(
                                'limit_' + k,
                                F.lit(individual_limits_several_fields_df.select('limit_' + k).collect()[0]['limit_' + k]).cast("float"))
                            .withColumn('limit_apply', F.lit('limit_' + k))
                        )
                    else:
                        facilities_add = (
                            facilities_add.join(individual_limits_several_fields_df, [v2], 'left').fillna(1.0).fillna({'limit_apply': ''})
                        )

                # limite según categoria - valor
                else:
                    if (individual_limits_several_fields_df.count() == 1) & (individual_limits_several_fields_df.select(v2).collect()[0][v2] == 'total'):
                        facilities_add = (
                            facilities_add.join(individual_limits_several_fields_df, [v1], 'left').fillna(1.0).fillna({'limit_apply': ''})
                        )
                    else:
                        facilities_add = (
                            facilities_add.join(individual_limits_several_fields_df, [v1, v2], 'left').fillna(1.0).fillna({'limit_apply': ''})
                        )

                        num_limits_null_columns_several_fields = facilities_add.where(F.col(v2).isNull()).count()
                        if num_limits_null_columns_several_fields > 0:

                            facilities_add = (
                                facilities_add.withColumn(
                                    'limit_' + k,
                                    F.when(F.col(v2).isNull(), dict_lim_ind_nul[k]).otherwise(F.col('limit_' + k))
                                )
                            )

            # añadimos al campo de limites aplicados el procesado que vendrá con valor cuando los limites han coincidido
            facilities_add = (
                facilities_add.withColumn(
                    'limits_applied',
                    F.when(F.col('limit_apply') != '',
                           F.concat(F.col('limits_applied'), F.lit(','), F.col('limit_apply')))
                    .otherwise(F.col('limits_applied'))
                )
                .drop('limit_apply')
                .withColumn(
                    'exclusion_limit',
                    F.when(F.col('limit_' + k) == 0, F.concat(F.lit(k), F.lit(', '), F.col('exclusion_limit')))
                    .otherwise(F.col('exclusion_limit'))
                )
            )

            limits_indv.append('limit_' + k)

            varios_campos = 0

        return facilities_add, limits_indv

    def build_importe_titulizable(self, facilities_with_individual_limits_applied_df, dict_lim_ind, limites_ind, limits_indv):
        """
        Build dataframe with importe titulizable for algorithm and each facility.
        """
        self.logger.info("Building importe titulizable")

        l_pr = [limit.limit_type for limit in
                limites_ind.where(F.col('imp_limit') != 'individual').select('limit_type').distinct().collect()]

        dict_lim_ind_p = {}
        facilities_with_importe_ini_df = (
            facilities_with_individual_limits_applied_df.withColumn(
                'imp_maximo_individual',
                F.lit(self.portfolio_size)
            )
            .withColumn('limit_individual_p', F.lit(1.0))
        )

        if (len(l_pr) > 0):
            for k in l_pr:

                dict_lim_ind_p[k] = dict_lim_ind[k]

                if ('limit_' + k in limits_indv):
                    limits_indv.remove('limit_' + k)  # quitamos del cálculo del minimo limite individual

                facilities_with_importe_ini_df = (
                    facilities_with_importe_ini_df.withColumn(
                        'imp_maximo_' + k,
                        (F.col('limit_' + k) * self.portfolio_size).cast("float")
                    )
                )  # calculo ese importe máximo

            if (len(l_pr) == 1):
                self.logger.info('Solo un limite individual con limite de importe portfolio')
                facilities_with_importe_ini_df = (
                    facilities_with_importe_ini_df.withColumn(
                        'imp_maximo_individual', F.col('imp_maximo_' + k)
                    )
                    .withColumn('limit_individual_p', F.col('limit_' + k))
                )
            else:
                facilities_with_importe_ini_df = (
                    facilities_with_importe_ini_df.withColumn(
                        'imp_maximo_individual',
                        F.least(*[F.col('imp_maximo_' + x) for x in l_pr])
                    )
                    .withColumn('limit_individual_p', F.least(*[F.col('limit_' + x) for x in l_pr]))
                )

        facilities_with_individual_and_portfolio_limits_applied_df = (
            facilities_with_importe_ini_df.withColumn(
                'limit_individual', F.least(*[F.col(x).cast('float') for x in limits_indv])
            ).withColumn('importe_titulizable_ini', F.col('importe_susceptible') * F.col('limit_individual'))
            .withColumn(
                'importe_titulizable',
                F.when(
                    F.col('importe_titulizable_ini') <= F.col('imp_maximo_individual'),
                    F.col('importe_titulizable_ini')
                ).otherwise(F.col('imp_maximo_individual'))
            ).withColumn(
                'excluded',
                F.when(
                    ((F.col('limit_individual') == 0) | (F.col('imp_maximo_individual') <= 0)), 1
                )
                .otherwise(F.col('excluded'))
            ).withColumn('candidata', F.when(F.col('importe_titulizable') > 0, 1).otherwise(0))
        )

        facilities_with_individual_and_portfolio_limits_applied_and_exclusions_df = (
            facilities_with_individual_and_portfolio_limits_applied_df.withColumn(
                'motivo_exclusion',
                F.when(
                    F.col('excluded') == 1,
                    'limite individual 0'
                ).when(
                    F.col('importe_susceptible') <= 0,
                    'importe susceptible 0'
                ).otherwise('NA')
            )
            .withColumn('detalle_exclusion', F.col('exclusion_limit'))
        )

        return facilities_with_individual_and_portfolio_limits_applied_and_exclusions_df

    def ini_columns(self, lim_portfolio, df):
        """
        Initialize columns for the algorithm.
        """
        consumption_cols = []

        for k in lim_portfolio:
            df['limit_' + k] = 1.0000000
            df['max_portfolio_size_' + k] = self.portfolio_size
            df['consumido_' + k] = 0.0000000
            df['importe_consumido_' + k] = 0.0000000
            consumption_cols.append('limit_' + k)
            consumption_cols.append('max_portfolio_size_' + k)
            consumption_cols.append('consumido_' + k)
            consumption_cols.append('importe_consumido_' + k)

        df['selected'] = 0  # si se incluye en la cartera
        df['limit_portfolio'] = 1.0000000  # limite más restrictivo a nivel portfolio
        df[
            'porcentaje_portfolio_size'] = 0.0000000  # % del importe del portfolio_size consumido por la facility (peso de la facility en la cartera)
        df['importe_optimo'] = 0.0000000  # importe que se incluye de la facility
        df['ranking_candidata'] = 0  # orden de prioridad en el modelo
        df['ranking_selected'] = 0  # orden de selección en la cartera final
        df['importe_optimo_acumulado'] = 0.0000000  # suma de los importes de las facilities incluidas en la cartera
        df['porcentaje_portfolio_size_acumulado'] = 0.0000000  # % de la cartera (portfolio_size) que se ha ido consumiendo
        df['porcentaje_optimo'] = 0.0000000  # % sobre el importe suceptible de la facility que se toma en la cartera

        # lo incluyo como columna el máximo importe a titulizar
        df['limit_portfolio_size'] = self.portfolio_size

        consumption_cols = consumption_cols + ['selected', 'limit_portfolio', 'porcentaje_portfolio_size',
                                               'importe_optimo',
                                               'ranking_candidata', 'ranking_selected', 'importe_optimo_acumulado',
                                               'porcentaje_portfolio_size_acumulado', 'porcentaje_optimo',
                                               'limit_portfolio_size']

        return consumption_cols, df

    def build_consumed_limits(self, dict_lim_values, dict_lim_port, facilities_df):
        """
        Build consumed limits dict by the algorithm.
        """
        consumed_limits_dict = {}  # inicializamos diccionario de consumos
        limit_keys = list(dict_lim_values.keys())  # % marcados en launchad
        keys_fechas = ['maturity_min', 'maturity_max']

        for k, v in dict_lim_port.items():
            if (k not in keys_fechas):  # caso base: limite-categoria=valor
                for k1 in facilities_df[v].unique():  # recojo todos los posibles valores de ese campo en la facility
                    consumed_limits_dict[k + '-' + str(k1)] = 0.0000000

                    # si no hay marcado limite se pone a 1.0
                    if (k + '-' + str(k1) not in limit_keys):
                        valor = 1.0000000

                        if (k + '-' + 'total' in limit_keys):  # si es un limite a nivel global como TODOS los grupos
                            valor = round(float(dict_lim_values[k + '-' + 'total']), 7)
                            # valor = round(float(dict_lim_values[k + '-' + 'total'].replace(',', '.')), 4)
                        dict_lim_values[k + '-' + str(k1)] = valor

            else:  # no es un limite directo y hay que calcular fechas: limite-ndias=valor
                lk = [limit_key for limit_key in limit_keys if k in limit_key][0]
                dias = int(lk[len(k) + 1:])  # ndias que marcan fecha tope
                f_tope = np.datetime64(Utilities.get_fecha(self.securitization_date, dias)).astype('datetime64[D]')

                for k1 in facilities_df[v].unique().astype('datetime64[D]'):
                    consumed_limits_dict[k + '-' + str(k1)] = 0.0000000
                    if (((k == 'maturity_min') & (k1 >= f_tope)) | ((k == 'maturity_max') & (k1 <= f_tope))):
                        valor = 1.0000  # valor si el limite no aplica
                    else:
                        valor = round(float(dict_lim_values[lk]), 7)  # valor si el limite aplica

                    dict_lim_values[k + '-' + str(k1)] = valor

        return consumed_limits_dict

    def build_appliable_limits(self, dict_lim_values, l_lim_consumidos):
        """
        Build dictionary with limit-field relation to be applied in algorithm.
        """
        facilities_keys = list(l_lim_consumidos.keys())
        l_lim_marcados = {key: round(dict_lim_values[key], 4) for key in facilities_keys}

        return l_lim_marcados

    def build_max_limits(self, l_lim_marcados):
        """
        Build max limits dictionary from marked limits in launchpad.
        """
        l_max_limites = {key: (l_lim_marcados[key] * float(self.portfolio_size)) for key in l_lim_marcados}
        return l_max_limites

    def get_keys_facility(self, i_facility, dataframe, dict_lim_port, cols_type):
        """
        Function that get's facility keys from dataframe.
        """
        d_keys = {}  # formato lista de clave-valor
        res = {}  # diccionario de campos

        # para cada limite, el valor del campo de Datio indicado
        # ej. para facility fila 1 -> divisa = df.loc[1, 'currency_id'] -> 'divisa':'USD'
        for k, v in dict_lim_port.items():
            res[k] = dataframe.loc[i_facility, v]

            if (cols_type[v] == 'date'):
                res[k] = dataframe.loc[i_facility, v].strftime('%Y-%m-%d')

        for k, v in res.items():
            key = str(k) + '-' + str(v)
            d_keys[k] = key

        return d_keys

    def build_portfolio_limits(self, limites_total, facilities_disponibles):
        """
        Function that builds the portfolio limits.
        """

        self.logger.info("Building portfolio limits")

        # Building pk for algorithm ordering.
        facilities_disponibles = (
            facilities_disponibles.withColumn(
                'pk_engine',
                F.concat(
                    facilities_disponibles.delta_file_id,
                    F.lit('_'),
                    facilities_disponibles.delta_file_band_id,
                    F.lit('_'),
                    facilities_disponibles.branch_id)
            )
        )

        limit_port = (
            limites_total.where(
                F.col('limit_scope') == 'portfolio')
            .where(F.col('header_flag') == 0)
            .where(F.col('limit_type') != 'sts')
        )

        lista = limit_port.select('limit_type', 'campo_datio').collect()
        dict_lim_port = {row['limit_type']: row['campo_datio'] for row in lista}

        lim_portfolio = list(dict_lim_port.keys())

        # limites globales: no complejos
        lista1 = (
            limit_port.select(
                'limit_type', 'concept1_value', 'limit_value')
            .where(F.col('complex_limit') == 0).collect()
        )

        dict_lim_values1 = {row['limit_type'] + '-' + row['concept1_value']: row['limit_value'] for row in lista1}

        # limites globales: complejos
        lista2 = (
            limit_port
            .select('limit_type', 'concept2_value', 'limit_value')
            .where(F.col('complex_limit') == 1).collect()
        )

        dict_lim_values2 = {row['limit_type'] + '-' + row['concept2_value']: row['limit_value'] for row in lista2}

        # FUSIONO en un único diccionario
        dict_lim_values = dict_lim_values1.copy()
        dict_lim_values.update(dict_lim_values2)

        col_types = list(facilities_disponibles.dtypes)
        cols_type = dict(col_types)

        raw_data = facilities_disponibles.toPandas()
        raw_data = SecuritizationUtils.cast_facilities_df(cols_type, raw_data)

        l_lim_consumidos = self.build_consumed_limits(dict_lim_values, dict_lim_port, raw_data)
        l_lim_marcados = self.build_appliable_limits(dict_lim_values, l_lim_consumidos)
        l_max_limites = self.build_max_limits(l_lim_marcados)
        cols_test, df = self.ini_columns(lim_portfolio, raw_data)

        # generamos un diccionario tb de importe consumido por cada limite
        l_importe_consumidos = l_lim_consumidos.copy()
        importe_acumulado = 0.0000000
        porcentaje_portfolio_size_acumulado = 0.0000000
        col_imp = 'importe_titulizable'
        selected_facilities = []

        cols_ord = ['RC_per', 'pk_engine']
        df = df.sort_values(by=cols_ord, kind='mergesort', ascending=False).reset_index(drop=True)

        for i in df.index:
            # PASO_1: importe titulizable de la facility
            expediente_importe = df.loc[i, col_imp]

            df.loc[i, 'ranking_candidata'] = i + 1

            # PASO_2: %máximo a titulizar de la facility
            max_proportion = 1.0000000

            if (importe_acumulado + expediente_importe) > self.portfolio_size:
                max_proportion = (self.portfolio_size - importe_acumulado) / expediente_importe

            # PASO_3: %mínimo marcado por los limites que será lo máximo que se puede titulizar
            # campos de la facility - valor que hay que limitar
            keys_f = self.get_keys_facility(i, df, dict_lim_port, cols_type)
            min_lim = max_proportion  # inicializamos el mínimo con el máximo posible a titulizar
            min_portfolio = max_proportion

            for k, v in keys_f.items():
                df.loc[i, 'limit_' + k] = l_lim_marcados[v]
                df.loc[i, 'max_portfolio_size_' + k] = l_max_limites[v]
                min_portfolio = min(min_portfolio, l_lim_marcados[v])
                disponible_proportion = round(float(l_lim_marcados[v] - l_lim_consumidos[v]), 7)

                if (disponible_proportion < 0):
                    disponible_proportion = 0.0000000

                df.loc[i, 'disponible_' + k] = disponible_proportion
                min_lim = min(min_lim, disponible_proportion)

            df.loc[i, 'limit_portfolio'] = min_portfolio
            df.loc[i, 'porcentaje_max_portfolio'] = min_lim

            # STEP 4
            if ((df.loc[i, 'candidata'] == 1) and (min_lim > 0)):

                # PASO_5: Calculamos el importe a titulizar de la facility
                importe_max_facility = self.portfolio_size * min_lim

                df.loc[i, 'importe_max_facility'] = importe_max_facility

                if (importe_max_facility >= expediente_importe):
                    if (expediente_importe > self.portfolio_size - importe_acumulado):
                        importe_seleccionado = self.portfolio_size - importe_acumulado
                    else:
                        importe_seleccionado = expediente_importe
                else:
                    importe_seleccionado = importe_max_facility

                if (importe_seleccionado > 0):
                    # PASO_6: Actualizamos los acumulado tanto de limites como de importe a titulizar
                    importe_acumulado = importe_acumulado + importe_seleccionado

                    por_consumido = round(float(importe_seleccionado / self.portfolio_size), 7)

                    for k, v in keys_f.items():
                        l_lim_consumidos[v] = round(float(l_lim_consumidos[v] + por_consumido), 7)
                        l_importe_consumidos[v] = l_importe_consumidos[v] + importe_seleccionado
                        df.loc[i, 'consumido_' + k] = l_lim_consumidos[v]
                        df.loc[i, 'importe_consumido_' + k] = l_importe_consumidos[v]

                    # PASO_7: Rellenamos traza a nivel facility
                    selected_facilities.append((i, por_consumido))
                    df.loc[i, 'selected'] = 1
                    df.loc[i, 'importe_optimo'] = importe_seleccionado
                    df.loc[i, 'porcentaje_optimo'] = round(
                        float(importe_seleccionado / df.loc[i, 'importe_susceptible']), 7)
                    df.loc[i, 'ranking_selected'] = len(selected_facilities)
                    df.loc[i, 'importe_optimo_acumulado'] = importe_acumulado
                    porcentaje_portfolio_size_acumulado = round(float(importe_acumulado / self.portfolio_size), 7)
                    df.loc[i, 'porcentaje_portfolio_size_acumulado'] = porcentaje_portfolio_size_acumulado
                    df.loc[i, 'porcentaje_portfolio_size'] = por_consumido

            else:
                if (df.loc[i, 'candidata'] == 1):
                    min_disponible = 1.0
                    for k, v in keys_f.items():
                        disponible_proportion = round(float(l_lim_marcados[v] - l_lim_consumidos[v]), 7)
                        if (disponible_proportion < 0):
                            disponible_proportion = 0.0000000
                        if (disponible_proportion == 0):
                            min_disponible = min(min_disponible, disponible_proportion)
                            if (l_lim_marcados[v] == 0):
                                df.loc[i, 'motivo_exclusion'] = 'limite portfolio 0'
                                df.loc[i, 'detalle_exclusion'] = v
                            else:
                                df.loc[i, 'motivo_exclusion'] = 'consumido limite portfolio'
                                df.loc[i, 'detalle_exclusion'] = v

                df.loc[i, 'importe_optimo_acumulado'] = importe_acumulado
                df.loc[i, 'porcentaje_portfolio_size_acumulado'] = porcentaje_portfolio_size_acumulado
                for k, v in keys_f.items():
                    df.loc[i, 'consumido_' + k] = l_lim_consumidos[v]
                    df.loc[i, 'importe_consumido_' + k] = l_importe_consumidos[v]

            # PASO_8: si se ha alcanzado el máximo a titulizar salimos del bucle, ya tenemos las facilities a titiu
            if (importe_acumulado >= self.portfolio_size):
                break

        optimized_porfolio_df = self.build_df_with_securitization_portfolio_optimized(df)
        final_limit_dictionary_df = self.build_limits_output_df(l_lim_marcados, l_lim_consumidos, l_max_limites, l_importe_consumidos)
        return optimized_porfolio_df, final_limit_dictionary_df

    def build_limits_output_df(self, l_lim_marcados, l_lim_consumidos, l_max_limites, l_importe_consumidos):
        """
        Function that builds the limits output DataFrame. It joins all the dictionary's used in algorithm.
        """
        l_lim_marcados_df = pd.DataFrame(list(l_lim_marcados.items()), columns=['limite', 'valor_launchpad'])
        l_lim_consumidos_df = pd.DataFrame(list(l_lim_consumidos.items()), columns=['limite', 'valor_consumido'])
        l_max_limites_df = pd.DataFrame(list(l_max_limites.items()), columns=['limite', 'importe_máximo'])
        l_importe_consumidos_df = pd.DataFrame(list(l_importe_consumidos.items()), columns=['limite', 'importe_consumido'])

        l_lim_marcados_df_sp = self.dataproc.getSparkSession().createDataFrame(l_lim_marcados_df).fillna(0)
        l_lim_consumidos_df_sp = self.dataproc.getSparkSession().createDataFrame(l_lim_consumidos_df).fillna(0)
        l_max_limites_df_sp = self.dataproc.getSparkSession().createDataFrame(l_max_limites_df).fillna(0)
        l_importe_consumidos_df_sp = self.dataproc.getSparkSession().createDataFrame(l_importe_consumidos_df).fillna(0)

        limits_concat_df = l_lim_marcados_df_sp.join(l_lim_consumidos_df_sp, ['limite']).join(l_max_limites_df_sp, ['limite']).join(l_importe_consumidos_df_sp, ['limite'])
        limits_concat_df = limits_concat_df.withColumn('limit_escenario', F.lit(self.securitization_escenario))
        limits_concat_df = limits_concat_df.withColumn('Limit_type', F.split(F.col('limite'), '-').getItem(0))
        limits_concat_df = limits_concat_df.withColumn('limit', F.split(F.col('limite'), '-').getItem(1))
        limits_concat_df = limits_concat_df.drop("limite")

        return limits_concat_df

    def build_df_with_securitization_portfolio_optimized(self, cartera_pd_df):
        """
        Function that builds the optimized portfolio DataFrame in spark.
        """
        for column in cartera_pd_df.select_dtypes(include=['object', 'datetime64[ns]']).columns:
            cartera_pd_df[column] = cartera_pd_df[column].astype(str)

        # Creamos dataframe de spark
        optimized_cartera_spark_df = self.dataproc.getSparkSession().createDataFrame(cartera_pd_df).fillna(0)

        # Ajustamos el tipado de las columnas
        tipos = []

        for c in optimized_cartera_spark_df.columns:
            tipo = optimized_cartera_spark_df.schema[c].dataType
            if tipo not in tipos:
                tipos.append(tipo)

            # redondeo los tipos numéricos a 2 decimales
            if str(tipo) in ('LongType', 'DoubleType'):
                if (c in ['porcentaje_portfolio_size', 'porcentaje_portfolio_size_acumulado']):
                    optimized_cartera_spark_df = optimized_cartera_spark_df.withColumn(c, F.round(F.col(c), 7))
                else:
                    optimized_cartera_spark_df = optimized_cartera_spark_df.withColumn(c, F.round(F.col(c), 4))

        optimized_cartera_spark_df = (
            optimized_cartera_spark_df.withColumn(
                'clan_date', F.to_date(F.col('clan_date'), 'yyyyMMdd')
            ).withColumn(
                'clan_date', F.to_date(F.col('clan_date'), 'yyyy-MM-dd')
            ).withColumn(
                'deal_signing_date', F.to_date(F.col('deal_signing_date'), 'yyyy-MM-dd')
            )
        )

        optimized_cartera_spark_df = optimized_cartera_spark_df.withColumn("portfolio_type", F.lit(self.securization_type))

        # Adding concat column and dropping id for ordering.
        optimized_cartera_spark_df = optimized_cartera_spark_df.drop("pk_engine")
        optimized_cartera_spark_df = (
            optimized_cartera_spark_df.withColumn(
                'facility_id',
                F.concat(
                    optimized_cartera_spark_df.delta_file_id,
                    F.lit('_'),
                    optimized_cartera_spark_df.delta_file_band_id)
            )
        )

        return optimized_cartera_spark_df
