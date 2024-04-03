"""
Module to write in DSLB
"""
from alfred.alerts.savers import save_data_to_csv


class to_dslb:  # pragma: no cover
    """Class containing the function to write in DSLB Sandbox"""

    def __init__(self, logger, dataproc):
        """
        Constructor
        """
        self.dataproc = dataproc
        self.logger = logger

    def df_to_sb(self, df, sb_path, table_name, table_format='parquet', write_mode='overwrite', repartition_num=None,
                 partition_cols=None):
        """
            This method writes in DSLB Sandbox.

            Parameters:
            ----------
            logger : logger
                Logger
            df : pyspark.sql.DataFrame
                Data to upload
            sb_path : string
                Sandbox path
            table_name : string
                Table name
            format: string
                Table format. Could be csv or parquet. Default: parquet
            write_mode : string
                Write mode. Default: append
            repartition_num : int
                Number of partitions. Default: None
            partition_cols : list
                Partition columns. Default: None

            Returns:
            -------
            True
        """

        self.logger.info("to_dslb.df_to_sb")

        table_path = '{0}/{1}'.format(sb_path, table_name)
        self.logger.info('Writing data in path:' + table_path)
        if table_format == 'parquet':
            if repartition_num:
                df = df.repartition(repartition_num)
            if partition_cols:
                self.dataproc.write().mode(write_mode).partition_by(partition_cols)\
                    .compact_enabled(True).parquet(df, table_path)
            else:
                self.dataproc.write().mode(write_mode)\
                    .compact_enabled(True).parquet(df, table_path)
        elif table_format == 'csv':
            save_data_to_csv(self.dataproc, df, table_path)
        else:
            raise Exception('Table format is not supported! Available formats: parquet, csv')
        return True
