"""
This module contains the FX class.
"""
from pyspark.sql import functions as F
from rubik.load.markets import Markets


class FX:
    """Class containing FX crosses."""

    def __init__(self, logger, dataproc):
        """
        Constructor
        """

        self.logger = logger
        self.dataproc = dataproc
        markets = Markets("/data", self.dataproc)
        self.fx = markets.read_fx_eod()

    def generic(
        self,
        df,
        dict_cols_to_convert,
        preserve_cols=False,
        currency_id_name="currency_id",
    ):
        """
        This method transforms currency columns into countervalued euros as according to the dict_cols_to_convert
        dictionary.

        Parameters:
        ----------
        df : pyspark.sql.DataFrame
        dict_cols_to_convert : dict


        Returns:
        -------
        This method returns the joined data.
        """

        self.logger.info("FX.generic")

        df = df.join(
            self.fx.withColumnRenamed("currency", currency_id_name),
            on=currency_id_name,
            how="left",
        ).fillna(1, subset=["fx"])
        df = df.select(
            *[
                (F.col("fx") * F.col(c)).alias(dict_cols_to_convert[c])
                if c in dict_cols_to_convert
                else c
                for c in df.columns
            ]
            + (list(dict_cols_to_convert.keys()) if preserve_cols else [])
        )
        return df
