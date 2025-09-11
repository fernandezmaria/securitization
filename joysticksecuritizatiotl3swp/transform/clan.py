"""
This module contains some classes related to the transformation process of CLAN data.
"""
import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
from rubik.load.markets import Markets

from joysticksecuritizatiotl3swp.configurations.catalogues import (
    mov_evol_saldos,
    listaMov,
    listaMov1,
    listaMov2,
    listaMov4,
)


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
            on=["gf_facility_id", "gf_fclty_trc_id", "gf_entity_id", "gf_branch_id"],
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
        evol_saldos = items.where(F.col("gf_movement_class_desc").isin(mov_evol_saldos))

        return evol_saldos.select(
            "gf_facility_id",
            "gf_fclty_trc_id",
            "gf_fclty_trc_desc",
            "gf_branch_id",
            "gf_entity_id",
            "g_currency_id",
            "gf_deal_signing_date",
            "gf_expiration_date",
            "gf_movement_class_desc",
            "gf_movement_type_desc",
            "gf_mov_start_date",
            "gf_mov_end_date",
            "gf_settlement_date",
            "gf_movement_sequential_id",
            "gf_drwdn_id",
            "gf_ac_movement_amount",
            "gf_mov_settl_br_bc_amount",
            "gf_movement_amount",
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
                fx.withColumnRenamed("currency", "g_currency_id"),
                on="g_currency_id",
                how="left",
            )
            .fillna(1, subset=["fx"])
            .withColumn("item_amount_eur", F.col("gf_movement_amount") * F.col("fx"))
            .withColumn(
                "item_base_amount_eur", F.col("gf_mov_settl_br_bc_amount") * F.col("fx")
            )
        )

        evolucion_saldos_aux = (
            evol_saldos.select(
                "gf_facility_id",
                "gf_fclty_trc_id",
                "gf_branch_id",
                "gf_deal_signing_date",
                "gf_expiration_date",
            )
            .drop_duplicates()
            .withColumn("var_total", F.lit(0))
            .withColumn("var_dispuesto", F.lit(0))
            .withColumn("runoff", F.lit(0))
            .withColumn("var_total_acum", F.lit(0))
            .withColumn("var_dispuesto_acum", F.lit(0))
            .withColumn("runoff_acum", F.lit(0))
            .withColumn("gf_mov_end_date", F.lit(fecha_valor))
            .withColumn("gf_mov_start_date", F.lit(fecha_valor))
        )

        # Columnas en funcion del tipo de movimiento
        evol_saldos1 = (
            evol_saldos.withColumn(
                "var_total",
                F.when(
                    F.col("gf_movement_type_desc").isin(listaMov),
                    -F.col("item_base_amount_eur"),
                ).otherwise(
                    F.when(
                        F.col("gf_movement_type_desc") == "Authorized Amount Increase",
                        F.col("item_base_amount_eur"),
                    ).otherwise(0)
                ),
            )
            .withColumn(
                "var_dispuesto",
                F.when(
                    F.col("gf_movement_type_desc").isin(listaMov1),
                    F.col("item_base_amount_eur"),
                ).otherwise(
                    F.when(
                        F.col("gf_movement_type_desc").isin(listaMov2),
                        -F.col("item_base_amount_eur"),
                    ).otherwise(0)
                ),
            )
            .withColumn(
                "runoff",
                F.when(
                    F.col("gf_movement_type_desc").isin(listaMov),
                    F.col("item_amount_eur"),
                ).otherwise(0),
            )
            .withColumn(
                "gf_mov_start_date",
                F.when(
                    F.col("gf_movement_type_desc").isin(listaMov4),
                    F.col("gf_mov_end_date"),
                ).otherwise(F.col("gf_mov_start_date")),
            )
            .withColumn("var_dispuesto_euros", F.lit(0))
        )

        # movimientos anteriores a la fecha valor
        w_cumsum = (
            W.partitionBy("gf_facility_id", "gf_fclty_trc_id", "gf_branch_id")
            .orderBy(
                F.col("gf_facility_id").asc(),
                F.col("gf_fclty_trc_id").asc(),
                F.col("gf_branch_id").asc(),
                F.col("gf_mov_start_date").desc(),
            )
            .rangeBetween(W.unboundedPreceding, 0)
        )
        w_lag = W.partitionBy(
            "gf_facility_id", "gf_fclty_trc_id", "gf_branch_id"
        ).orderBy("gf_facility_id", "gf_fclty_trc_id", "gf_branch_id", "gf_mov_end_date")

        evolucion_saldos_hasta = (
            evol_saldos1.filter(F.col("gf_mov_start_date") <= fecha_valor)
            .withColumn("var_total", -F.col("var_total"))
            .withColumn("var_dispuesto", -F.col("var_dispuesto"))
            .withColumn("var_dispuesto_euros", -F.col("var_dispuesto_euros"))
            .groupby(
                "gf_facility_id",
                "gf_fclty_trc_id",
                "gf_branch_id",
                "gf_deal_signing_date",
                "gf_expiration_date",
                "gf_mov_start_date",
            )
            .agg(
                F.sum("var_total").alias("var_total"),
                F.sum("var_dispuesto").alias("var_dispuesto"),
                F.sum("runoff").alias("runoff"),
            )
            .withColumn("var_total_acum", F.sum("var_total").over(w_cumsum))
            .withColumn("var_dispuesto_acum", F.sum("var_dispuesto").over(w_cumsum))
            .withColumn("gf_mov_end_date", F.col("gf_mov_start_date"))
            .withColumn("gf_mov_start_date", F.lag("gf_mov_end_date").over(w_lag))
            .withColumn(
                "gf_mov_start_date",
                F.when(
                    F.col("gf_mov_start_date").isNull(), F.col("gf_deal_signing_date")
                ).otherwise(F.col("gf_mov_start_date")),
            )
        )

        evolucion_saldos_hasta_aux = evolucion_saldos_hasta.groupBy(
            "gf_facility_id", "gf_fclty_trc_id", "gf_branch_id"
        ).agg(F.max("gf_mov_end_date").alias("desde"))

        # movimientos posteriores a la fecha valor
        w_lag2 = W.partitionBy(
            "gf_facility_id", "gf_fclty_trc_id", "gf_branch_id"
        ).orderBy("gf_facility_id", "gf_fclty_trc_id", "gf_branch_id", "gf_mov_start_date")
        w_cumsum2 = (
            W.partitionBy("gf_facility_id", "gf_fclty_trc_id", "gf_branch_id")
            .orderBy(
                "gf_facility_id", "gf_fclty_trc_id", "gf_branch_id", "gf_mov_start_date"
            )
            .rangeBetween(W.unboundedPreceding, 0)
        )

        evol_saldos1 = (
            evol_saldos1.filter(F.col("gf_mov_start_date") > fecha_valor)
            .groupby(
                "gf_facility_id",
                "gf_fclty_trc_id",
                "gf_branch_id",
                "gf_deal_signing_date",
                "gf_expiration_date",
                "gf_mov_start_date",
            )
            .agg(
                F.sum("var_total").alias("var_total"),
                F.sum("var_dispuesto").alias("var_dispuesto"),
                F.sum("runoff").alias("runoff"),
            )
            .withColumn("gf_mov_end_date", F.lead("gf_mov_start_date").over(w_lag2))
            .withColumn(
                "gf_mov_end_date",
                F.when(
                    F.col("gf_mov_end_date").isNull(), F.col("gf_expiration_date")
                ).otherwise(F.col("gf_mov_end_date")),
            )
            .withColumn("var_total_acum", F.sum("var_total").over(w_cumsum2))
            .withColumn("var_dispuesto_acum", F.sum("var_dispuesto").over(w_cumsum2))
            .withColumn("runoff_acum", F.sum("runoff").over(w_cumsum2))
        )

        evolucion_saldos1_aux = evol_saldos1.groupBy(
            "gf_facility_id", "gf_fclty_trc_id", "gf_branch_id"
        ).agg(F.min("gf_mov_start_date").alias("hasta"))

        # Unir tablas "aux"
        evolucion_saldos_aux = (
            evolucion_saldos_aux.join(
                evolucion_saldos_hasta_aux,
                on=["gf_facility_id", "gf_fclty_trc_id", "gf_branch_id"],
                how="left",
            )
            .join(
                evolucion_saldos1_aux,
                on=["gf_facility_id", "gf_fclty_trc_id", "gf_branch_id"],
                how="left",
            )
            .withColumn(
                "gf_mov_start_date",
                F.when(F.col("desde").isNotNull(), F.col("desde")).otherwise(
                    F.col("gf_deal_signing_date")
                ),
            )
            .withColumn(
                "gf_mov_end_date",
                F.when(F.col("hasta").isNotNull(), F.col("hasta")).otherwise(
                    F.col("gf_expiration_date")
                ),
            )
            .select(
                "gf_facility_id",
                "gf_fclty_trc_id",
                "gf_branch_id",
                "gf_deal_signing_date",
                "gf_expiration_date",
                "var_total",
                "var_dispuesto",
                "runoff",
                "var_total_acum",
                "var_dispuesto_acum",
                "gf_mov_end_date",
                "gf_mov_start_date",
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
            F.col("gf_mov_start_date") > fecha_valor
        ).withColumn(
            "sumproduct", F.col("runoff") * date_to_num(F.col("gf_mov_start_date"))
        )

        for i in range(0, 11):
            runoff1 = runoff1.withColumn(
                col_year[i],
                F.when(
                    F.year(F.col("gf_mov_start_date")) == list_year[i], F.col("runoff")
                ).otherwise(0),
            )

        runoff1 = runoff1.groupBy(
            "gf_facility_id", "gf_fclty_trc_id", "gf_branch_id"
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
