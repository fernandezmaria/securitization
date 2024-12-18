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
        corporate_flag = tipo_titulizacion.select('corporate_loan_flag').collect()[0].corporate_loan_flag

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
            [(x.name_list_desc, x.limit_date) for x in limits_df.select(*limits_snapshot).distinct().collect()][0]
        return securitization_escenario, securitization_date

    @staticmethod
    def cast_facilities_df(cols_type, facilities_pandas_df):
        """
        Cast pandas df to types for building spark df.
        """
        # listado de: columna - tipo de dato

        # en formato diccionario

        for r in facilities_pandas_df.columns:
            if (cols_type[r] == 'string'):
                # print('string',r)
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('str')
            elif (cols_type[r] == 'date'):
                # raw_data[r] = raw_data[r].astype('datetime64')
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('datetime64[D]')
                # print(r)
            elif (cols_type[r] == 'boolean'):
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('bool')
            elif (cols_type[r] == 'int'):
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('int')
            elif (cols_type[r] == 'double'):
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('float')
            elif ('decimal' in cols_type[r]):
                facilities_pandas_df[r] = facilities_pandas_df[r].astype('float')

        return facilities_pandas_df
