from pyspark.sql import functions as F

from joysticksecuritizatiotl3swp.configurations.catalogues import limits_snapshot


class SecuritizationUtils:

    @staticmethod
    def get_securization_type(limits_df):
        """
        Get securitization type
        """
        tipo_titulizacion = limits_df.where(F.col('limit_type') == 'portfolio_type').select('corporate_loan_flag',
                                                                                            'project_finance_flag')
        corporate_flag = tipo_titulizacion.select('corporate_loan_flag').collect()[
            0].corporate_loan_flag

        tipo = 'project_finance'

        if (corporate_flag == 1):
            tipo = 'corporate_loan'

        return tipo

    @staticmethod
    def get_securitization_escenario_and_date(limits_df):
        """
        Get securitization escenario and date
        """
        securitization_escenario, securitization_date = \
            [(x.name_list_desc, x.limit_date)
             for x in limits_df.select(*limits_snapshot).distinct().collect()][0]
        return securitization_escenario, securitization_date

    @staticmethod
    def cast_facilities_df(cols_type, facilities_pandas_df):
        """
        Cast pandas df to types for building spark df.
        """
        # listado de: columna - tipo de dato

        # en formato diccionario

        for r in facilities_pandas_df.columns:
            if cols_type[r] == 'string':
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('str')
            elif cols_type[r] == 'date':
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('datetime64[ns]')
            elif cols_type[r] == 'boolean':
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('bool')
            elif cols_type[r] == 'int':
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('int')
            elif cols_type[r] == 'double':
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('float')
            elif 'decimal' in cols_type[r]:
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('float')
            elif cols_type[r] == 'float':
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('float')
        return facilities_pandas_df

    @staticmethod
    def get_facilities_dtype(cols_type):
        """
        Get the data types for the facilities DataFrame based on the provided column types.
        """
        extra_types = {
            'limit_group': 'float',
            'max_portfolio_size_group': 'float',
            'consumido_group': 'float',
            'importe_consumido_group': 'float',
            'limit_sts_group': 'float',
            'max_portfolio_size_sts_group': 'float',
            'consumido_sts_group': 'float',
            'importe_consumido_sts_group': 'float',
            'limit_customer_subsector': 'float',
            'max_portfolio_size_customer_subsector': 'float',
            'consumido_customer_subsector': 'float',
            'importe_consumido_customer_subsector': 'float',
            'limit_non_ig': 'float',
            'max_portfolio_size_non_ig': 'float',
            'consumido_non_ig': 'float',
            'importe_consumido_non_ig': 'float',
            'limit_divisa': 'float',
            'max_portfolio_size_divisa': 'float',
            'consumido_divisa': 'float',
            'importe_consumido_divisa': 'float',
            'limit_no_esg_linked': 'float',
            'max_portfolio_size_no_esg_linked': 'float',
            'consumido_no_esg_linked': 'float',
            'importe_consumido_no_esg_linked': 'float',
            'limit_customer_sector': 'float',
            'max_portfolio_size_customer_sector': 'float',
            'consumido_customer_sector': 'float',
            'importe_consumido_customer_sector': 'float',
            'limit_customer_country': 'float',
            'max_portfolio_size_customer_country': 'float',
            'consumido_customer_country': 'float',
            'importe_consumido_customer_country': 'float',
            'limit_financial_product': 'float',
            'max_portfolio_size_financial_product': 'float',
            'consumido_financial_product': 'float',
            'importe_consumido_financial_product': 'float',
            'selected': 'int',
            'limit_portfolio': 'float',
            'porcentaje_portfolio_size': 'float',
            'importe_optimo': 'float',
            'ranking_candidata': 'int',
            'ranking_selected': 'int',
            'importe_optimo_acumulado': 'float',
            'porcentaje_portfolio_size_acumulado': 'float',
            'porcentaje_optimo': 'float',
            'limit_portfolio_size': 'float',
            'clan_date': 'date'
        }
        result = cols_type.copy()
        result.update(extra_types)
        return result
