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

        if (corporate_flag == "1"):
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
        Cast columns of a pandas DataFrame to specified data types for building a Spark DataFrame.

        Args:
        :param cols_type: Dictionary mapping column names to their target data types.
        :param facilities_pandas_df: Input pandas DataFrame to be cast.
        :return: pandas DataFrame with columns cast to specified types.
        """
        type_mapping = {
            'string': 'str',
            'date': 'datetime64[ns]',
            'boolean': 'bool',
            'int': 'int',
            'double': 'float'
        }

        for column in facilities_pandas_df.columns:
            target_type = cols_type.get(column)
            if target_type in type_mapping:
                facilities_pandas_df[column] = facilities_pandas_df[column].astype(
                    type_mapping[target_type])
            elif 'decimal' in target_type:
                facilities_pandas_df[column] = facilities_pandas_df[column].astype(
                    'float')
        return facilities_pandas_df
