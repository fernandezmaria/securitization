"""
This module contains some classes related to the transformation process of CLAN data.
"""
import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
from rubik.load.markets import Markets

from joysticksecuritizatiotl3swp.configurations.catalogues import mov_evol_saldos, listaMov, listaMov1, listaMov2, \
    listaMov4


class ItemsBalance:
    """Class containing the Items and Balance joining method."""

    def __init__(self, logger, dataproc):
        """
        Constructor
        """

        self.logger = logger
        self.dataproc = dataproc

    def cruce(self, items, balance):
        """
        This method gets Items and Balance joinned data.

        Parameters:
        ----------
        items, balance : pyspark.sql.DataFrame

        Returns:
        -------
        This method returns the joinned data.
        """

        self.logger.info("ItemsBalance.cruce")

        return items.join(
            balance,
            on=["delta_file_id", "delta_file_band_id", "entity_id", "branch_id"],
            how="inner",
        )

    def narrow_down_movs(self, items):
        """
        This method filters balance evolution data. This function corresponds to filter_data.balance_evolution
        in the monthly pipeline.

        Parameters:
        ----------
        items : pyspark.sql.DataFrame
            Data to filter

        Returns:
        -------
        This method returns the balance evolution data filtered (pyspark.sql.DataFrame).
        """

        self.logger.info("data.balance_evolution")

        # FILTRO PARA EVOLUCION DE SALDOS
        evol_saldos = items.where(
            F.col("movement_class_name").isin(mov_evol_saldos)
        )

        return evol_saldos.select(
            "delta_file_id",
            "delta_file_band_id",
            "delta_file_band_desc",
            "branch_id",
            "entity_id",
            "currency_id",
            "deal_signing_date",
            "expiration_date",
            "movement_class_name",
            "item_schedule_desc",
            "item_start_date",
            "item_end_date",
            "settlement_date",
            "movement_id",
            "base_item_id",
            "item_accounting_currency_amount",
            "item_base_currency_amount",
            "item_amount",
        )

    def balance_evolution(self, evol_saldos, fecha_valor):
        """
        This method filters balance evolution data.

        Parameters:
        ----------
        evol_saldos: pyspark.sql.DataFrame
            Data to filter
        fecha_valor : string (YYYY-MM-DD)
            Date

        Returns:
        -------
        This method returns balance evolution data filtered (pyspark.sql.DataFrame).
        """

        self.logger.info("Clan.balance_evolution")

        markets = Markets("/data", self.dataproc)
        fx = markets.read_fx_eod()
        # Calcular el importe del movimiento en € que no tenemos disponible en Clan
        evol_saldos = (
            evol_saldos.join(
                fx.withColumnRenamed("currency", "currency_id"),
                on="currency_id",
                how="left",
            )
            .fillna(1, subset=["fx"])
            .withColumn("item_amount_eur", F.col("item_amount") * F.col("fx"))
            .withColumn(
                "item_base_amount_eur", F.col("item_base_currency_amount") * F.col("fx")
            )
        )

        evolucion_saldos_aux = (
            evol_saldos.select(
                "delta_file_id",
                "delta_file_band_id",
                "branch_id",
                "deal_signing_date",
                "expiration_date",
            )
            .drop_duplicates()
            .withColumn("var_total", F.lit(0))
            .withColumn("var_dispuesto", F.lit(0))
            .withColumn("runoff", F.lit(0))
            .withColumn("var_total_acum", F.lit(0))
            .withColumn("var_dispuesto_acum", F.lit(0))
            .withColumn("runoff_acum", F.lit(0))
            .withColumn("item_end_date", F.lit(fecha_valor))
            .withColumn("item_start_date", F.lit(fecha_valor))
        )

        # Columnas en funcion del tipo de movimiento
        evol_saldos1 = (
            evol_saldos.withColumn(
                "var_total",
                F.when(
                    F.col("item_schedule_desc").isin(listaMov),
                    -F.col("item_base_amount_eur"),
                ).otherwise(
                    F.when(
                        F.col("item_schedule_desc") == "Authorized Amount Increase",
                        F.col("item_base_amount_eur"),
                    ).otherwise(0)
                ),
            )
            .withColumn(
                "var_dispuesto",
                F.when(
                    F.col("item_schedule_desc").isin(listaMov1),
                    F.col("item_base_amount_eur"),
                ).otherwise(
                    F.when(
                        F.col("item_schedule_desc").isin(listaMov2),
                        -F.col("item_base_amount_eur"),
                    ).otherwise(0)
                ),
            )
            .withColumn(
                "runoff",
                F.when(
                    F.col("item_schedule_desc").isin(listaMov),
                    F.col("item_amount_eur"),
                ).otherwise(0),
            )
            .withColumn(
                "item_start_date",
                F.when(
                    F.col("item_schedule_desc").isin(listaMov4),
                    F.col("item_end_date"),
                ).otherwise(F.col("item_start_date")),
            )
            .withColumn("var_dispuesto_euros", F.lit(0))
        )

        # movimientos anteriores a la fecha valor
        w_cumsum = (
            W.partitionBy("delta_file_id", "delta_file_band_id", "branch_id")
            .orderBy(
                F.col("delta_file_id").asc(),
                F.col("delta_file_band_id").asc(),
                F.col("branch_id").asc(),
                F.col("item_start_date").desc(),
            )
            .rangeBetween(W.unboundedPreceding, 0)
        )
        w_lag = W.partitionBy(
            "delta_file_id", "delta_file_band_id", "branch_id"
        ).orderBy("delta_file_id", "delta_file_band_id", "branch_id", "item_end_date")

        evolucion_saldos_hasta = (
            evol_saldos1.filter(F.col("item_start_date") <= fecha_valor)
            .withColumn("var_total", -F.col("var_total"))
            .withColumn("var_dispuesto", -F.col("var_dispuesto"))
            .withColumn("var_dispuesto_euros", -F.col("var_dispuesto_euros"))
            .groupby(
                "delta_file_id",
                "delta_file_band_id",
                "branch_id",
                "deal_signing_date",
                "expiration_date",
                "item_start_date",
            )
            .agg(
                F.sum("var_total").alias("var_total"),
                F.sum("var_dispuesto").alias("var_dispuesto"),
                F.sum("runoff").alias("runoff"),
            )
            .withColumn("var_total_acum", F.sum("var_total").over(w_cumsum))
            .withColumn("var_dispuesto_acum", F.sum("var_dispuesto").over(w_cumsum))
            .withColumn("item_end_date", F.col("item_start_date"))
            .withColumn("item_start_date", F.lag("item_end_date").over(w_lag))
            .withColumn(
                "item_start_date",
                F.when(
                    F.col("item_start_date").isNull(), F.col("deal_signing_date")
                ).otherwise(F.col("item_start_date")),
            )
        )

        evolucion_saldos_hasta_aux = evolucion_saldos_hasta.groupBy(
            "delta_file_id", "delta_file_band_id", "branch_id"
        ).agg(F.max("item_end_date").alias("desde"))

        # movimientos posteriores a la fecha valor
        w_lag2 = W.partitionBy(
            "delta_file_id", "delta_file_band_id", "branch_id"
        ).orderBy("delta_file_id", "delta_file_band_id", "branch_id", "item_start_date")
        w_cumsum2 = (
            W.partitionBy("delta_file_id", "delta_file_band_id", "branch_id")
            .orderBy(
                "delta_file_id", "delta_file_band_id", "branch_id", "item_start_date"
            )
            .rangeBetween(W.unboundedPreceding, 0)
        )

        evol_saldos1 = (
            evol_saldos1.filter(F.col("item_start_date") > fecha_valor)
            .groupby(
                "delta_file_id",
                "delta_file_band_id",
                "branch_id",
                "deal_signing_date",
                "expiration_date",
                "item_start_date",
            )
            .agg(
                F.sum("var_total").alias("var_total"),
                F.sum("var_dispuesto").alias("var_dispuesto"),
                F.sum("runoff").alias("runoff"),
            )
            .withColumn("item_end_date", F.lead("item_start_date").over(w_lag2))
            .withColumn(
                "item_end_date",
                F.when(
                    F.col("item_end_date").isNull(), F.col("expiration_date")
                ).otherwise(F.col("item_end_date")),
            )
            .withColumn("var_total_acum", F.sum("var_total").over(w_cumsum2))
            .withColumn("var_dispuesto_acum", F.sum("var_dispuesto").over(w_cumsum2))
            .withColumn("runoff_acum", F.sum("runoff").over(w_cumsum2))
        )

        evolucion_saldos1_aux = evol_saldos1.groupBy(
            "delta_file_id", "delta_file_band_id", "branch_id"
        ).agg(F.min("item_start_date").alias("hasta"))

        # Unir tablas "aux"
        evolucion_saldos_aux = (
            evolucion_saldos_aux.join(
                evolucion_saldos_hasta_aux,
                on=["delta_file_id", "delta_file_band_id", "branch_id"],
                how="left",
            )
            .join(
                evolucion_saldos1_aux,
                on=["delta_file_id", "delta_file_band_id", "branch_id"],
                how="left",
            )
            .withColumn(
                "item_start_date",
                F.when(F.col("desde").isNotNull(), F.col("desde")).otherwise(
                    F.col("deal_signing_date")
                ),
            )
            .withColumn(
                "item_end_date",
                F.when(F.col("hasta").isNotNull(), F.col("hasta")).otherwise(
                    F.col("expiration_date")
                ),
            )
            .select(
                "delta_file_id",
                "delta_file_band_id",
                "branch_id",
                "deal_signing_date",
                "expiration_date",
                "var_total",
                "var_dispuesto",
                "runoff",
                "var_total_acum",
                "var_dispuesto_acum",
                "item_end_date",
                "item_start_date",
                "runoff_acum",
            )
        )

        # Tabla de evol saldos final
        evolucion_saldos_hasta = evolucion_saldos_hasta.withColumn(
            "runoff_acum", F.lit(0)
        )
        evolucion_saldos1 = evolucion_saldos_hasta.unionByName(
            evolucion_saldos_aux
        ).unionByName(evol_saldos1)

        evolucion_saldos1.cache()
        evolucion_saldos1.count()
        self.logger.info("cache: evolucion_saldos1")

        return evolucion_saldos1

    def runoff(self, evolucion_saldos1, fecha_valor, col_year, list_year):
        """
        This method filters runoff data.

        Parameters:
        ----------
        evolucion_saldos1 : pyspark.sql.DataFrame
            Data to filter
        fecha_valor : string (YYYY-MM-DD)
            Date
        col_year, list_year : list[string]

        Returns:
        -------
        This method returns the runoff data filtered (pyspark.sql.DataFrame).
        """

        self.logger.info("data.runoff")

        @F.udf
        def date_to_num(date):
            return date.toordinal()

        @F.udf
        def num_to_date(num):
            return str(datetime.date.fromordinal(int(num)))

        runoff1 = evolucion_saldos1.filter(
            F.col("item_start_date") > fecha_valor
        ).withColumn(
            "sumproduct", F.col("runoff") * date_to_num(F.col("item_start_date"))
        )

        for i in range(0, 11):
            runoff1 = runoff1.withColumn(
                col_year[i],
                F.when(
                    F.year(F.col("item_start_date")) == list_year[i], F.col("runoff")
                ).otherwise(0),
            )

        runoff1 = runoff1.groupBy(
            "delta_file_id", "delta_file_band_id", "branch_id"
        ).agg(
            F.sum("runoff").alias("importe_amortizado"),
            F.sum("sumproduct").alias("sumproduct"),
            F.sum("imp_amortizado_y0").alias("imp_amortizado_y0"),
            F.sum("imp_amortizado_y1").alias("imp_amortizado_y1"),
            F.sum("imp_amortizado_y2").alias("imp_amortizado_y2"),
            F.sum("imp_amortizado_y3").alias("imp_amortizado_y3"),
            F.sum("imp_amortizado_y4").alias("imp_amortizado_y4"),
            F.sum("imp_amortizado_y5").alias("imp_amortizado_y5"),
            F.sum("imp_amortizado_y6").alias("imp_amortizado_y6"),
            F.sum("imp_amortizado_y7").alias("imp_amortizado_y7"),
            F.sum("imp_amortizado_y8").alias("imp_amortizado_y8"),
            F.sum("imp_amortizado_y9").alias("imp_amortizado_y9"),
            F.sum("imp_amortizado_y10").alias("imp_amortizado_y10"),
        )

        runoff2 = runoff1.where(F.col("importe_amortizado") > 0).withColumn(
            "vto_medio", num_to_date(F.col("sumproduct") / F.col("importe_amortizado"))
        )

        return runoff1, runoff2
