from datetime import timedelta

from dataproc_sdk.dataproc_sdk_catalog.datio_catalog import DatioCatalog
from dataproc_sdk.dataproc_sdk_datiofilesystem.datiofilesystem import DatioFileSystem
from pyspark.sql import functions as F, Window as W


class Utilities:

    @staticmethod
    def last_partition(p_path: str, campo: str):
        """
        Get the last partition of an s3 path.
        """
        datio_path = DatioFileSystem().get().qualify(p_path)
        fs = datio_path.fileSystem()
        path = datio_path.path()
        path_list = fs.listStatus(path)
        paths = [path.getPath().toString() for path in path_list]  # listado de todos los paths de la ruta pasada

        l_fechas = [element.split(campo + '=')[1] for element in paths if
                    campo in element]  # listado de todas las fechas
        return max(l_fechas)  # fecha mayor

    @staticmethod
    def check_for_partition(p_path: str, campo: str, value: str):
        """
        Get the last partition of an s3 path.
        """
        datio_path = DatioFileSystem().get().qualify(p_path)
        fs = datio_path.fileSystem()
        path = datio_path.path()
        path_list = fs.listStatus(path)
        paths = [path.getPath().toString() for path in path_list]  # listado de todos los paths de la ruta pasada

        l_fechas = [element.split(campo + '=')[1] for element in paths if
                    campo in element]  # listado de todas las fechas
        return value in l_fechas

    @staticmethod
    def get_last_value_partition_table(bbdd_table_concat: str, partition_field: str):
        """
        Takes max value of the specified partition in a bbdd & table.
        """
        last_partition = max(DatioCatalog().get().listPartitionNames(bbdd_table_concat))

        key_value_pairs = last_partition.split('/')
        for pair in key_value_pairs:
            if pair.startswith(partition_field + '='):
                return pair.split('=')[1]
        return None

    @staticmethod
    def drop_duplicates(self, df):
        """
        Drop duplicates from limits
        """

        window = W.partitionBy(*self.key_cols).orderBy(F.col("limit_date").desc())
        df = df.withColumn("rn", F.row_number().over(window)).where(F.col("rn") == 1).drop('rn')

        return df

    @staticmethod
    def get_fecha(fecha_ini: str, ndays: int, op: str = 'add'):
        """
        Add or subtract ndays to a date
        """
        if (op == 'add'):
            d = fecha_ini + timedelta(days=ndays)
        else:
            d = fecha_ini - timedelta(days=ndays)
        return d

    @staticmethod
    def transform_mrr_columns(df):
        """
        Transform MRR columns to match original names.
        """
        transformations = {
            'delta_file_id': 'gf_facility_id',
            'delta_file_band_id': 'gf_fclty_trc_id',
            'branch_id': 'gf_branch_id',
            'project_id': 'gf_project_id',
            'project_sector_desc': 'gf_fclty_trc_proj_sector_desc',
            'deal_purpose_type': 'gf_fclty_trc_tran_purp_desc',
            'seniority_name': 'gf_fclty_trc_srty_type_name',
            'insured_type': 'gf_insured_contract_type_name',
            'currency_id': 'g_currency_id',
            'expiration_date': 'gf_expiration_date',
            'deal_signing_date': 'gf_deal_signing_date',
            'syndicated_type': 'gf_syndicated_fctrc_type_name',
            'financial_product_desc': 'gf_financial_product_desc',
            'financial_product_class_desc': 'gf_fclty_trc_finpro_class_desc',
            'customer_id': 'gf_customer_id',
            'bbva_drawn_amount': 'gf_bbva_funded_amount',
            'bbva_available_amount': 'gf_bbva_avail_amount',
            'bbva_drawn_eur_amount': 'gf_bbva_funded_eur_amount',
            'bbva_available_eur_amount': 'gf_bbva_avail_eur_amount'
        }

        for col_nueva, col_original in transformations.items():
            if col_nueva in df.columns:
                df = df.withColumnRenamed(col_nueva, col_original)
        return df
