from alfred.alerts.savers import save_data_to_csv
from pyspark.sql import functions as F
from pyspark.errors.exceptions.captured import AnalysisException


class DSLBWriter:  # pragma: no cover
    """Class containing the function to write in DSLB Sandbox"""

    def __init__(self, logger, dataproc):
        """
        Constructor
        """
        self.dataproc = dataproc
        self.logger = logger

    def write_df_to_sb(
        self,
        df,
        sb_path,
        table_name,
        table_format="parquet",
        write_mode="overwrite",
        repartition_num=None,
        partition_cols=None,
    ):
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

        self.logger.info("DSLBWriter.write_df_to_sb")

        table_path = "{0}/{1}".format(sb_path, table_name)
        self.logger.info("Writing data in path:" + table_path)
        if table_format == "parquet":
            if repartition_num:
                df = df.repartition(repartition_num)
            if partition_cols:
                try:
                    # Align the schema of the dataframe with the existing parquet files
                    df = self.align_schemas(df, table_path)
                except AnalysisException as e:
                    if "PATH_NOT_FOUND" in str(e):
                        self.logger.warning(f"Path not found: {table_path}. Proceeding with writing.")
                    else:
                        raise
                self.dataproc.write().mode(write_mode).partition_by(
                    partition_cols
                ).parquet(df.repartition(1, *partition_cols), table_path)
            else:
                self.dataproc.write().mode(write_mode).compact_enabled(True).parquet(
                    df, table_path
                )
        elif table_format == "csv":
            save_data_to_csv(self.dataproc, df, table_path)
        else:
            raise Exception(
                "Table format is not supported! Available formats: parquet, csv"
            )
        return True

    def align_schemas(self, df_new, parquet_path):
        """
        This method aligns the schema of a new dataframe with an existing parquet file.

        Parameters:
        ----------
        spark : SparkSession
            Spark session
        df_new : pyspark.sql.DataFrame
            New dataframe
        parquet_path : string
            Path to the existing parquet file

        Returns:
        -------
        pyspark.sql.DataFrame
            Dataframe with the updated schema
        """
        # Read the existing parquet file and get its schema
        df_existing = self.dataproc.read().parquet(parquet_path)
        existing_schema = df_existing.schema

        # Get the schema of the new dataframe
        new_schema = df_new.schema

        # Compare the two schemas
        if str(existing_schema) != str(new_schema):
            # For each field in the existing schema
            for field in existing_schema:
                # Check if there's a corresponding field in the new dataframe's schema
                if any(f.name == field.name for f in new_schema):
                    # Cast the new dataframe's field to the type of the existing schema's field
                    df_new = df_new.withColumn(
                        field.name, F.col(field.name).cast(field.dataType)
                    )

        # Return the new dataframe with the updated schema
        return df_new
