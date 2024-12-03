from datetime import timedelta

from dataproc_sdk.dataproc_sdk_datiofilesystem.datiofilesystem import DatioFileSystem
from dataproc_sdk.dataproc_sdk_catalog.datio_catalog import DatioCatalog
from pyspark.sql import functions as F, Window as W

class Utilities:

    @staticmethod
    def last_partition(p_path: str, campo: str):
        datio_path = DatioFileSystem().get().qualify(p_path)
        fs = datio_path.fileSystem()
        path = datio_path.path()
        path_list = fs.listStatus(path)
        paths = [path.getPath().toString() for path in path_list]  # listado de todos los paths de la ruta pasada

        l_fechas = [element.split(campo + '=')[1] for element in paths if
                    campo in element]  # listado de todas las fechas
        return max(l_fechas)  # fecha mayor

    @staticmethod
    def get_last_value_partition_table(bbdd_table_concat: str, partition_field: str):
        """
        Takes max value of the specified partition in a bbdd & table.
        """
        last_partition = max(DatioCatalog().get().listPartitionNames(bbdd_table_concat))
        print(last_partition)
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
        if (op == 'add'):
            d = fecha_ini + timedelta(days=ndays)
        else:
            d = fecha_ini - timedelta(days=ndays)
        return d
