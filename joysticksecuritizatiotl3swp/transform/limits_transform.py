from pyspark.sql import functions as F, Window as W
import re

from joysticksecuritizatiotl3swp.configurations.catalogues import limits_key_cols
from joysticksecuritizatiotl3swp.read.limits import LimitsLoader
from joysticksecuritizatiotl3swp.utils.utilities import Utilities


class LimitsTransform:
    def __init__(self, logger, dataproc, parameters, data_date):
        self.logger = logger
        self.dataproc = dataproc
        self.data_date = data_date
        self.parameters = parameters
        self.limits = LimitsLoader(dataproc, parameters)
        self.key_cols = limits_key_cols

    def get_date(date: str):
        if date is not None:
            dates = re.findall(r'\d+', date)
            if (len(dates) == 3):
                if (len(dates[0]) == 4):
                    p_year = dates[0]
                    p_month = dates[1]
                    p_day = dates[2]
                else:
                    p_day = dates[0]
                    p_month = dates[1]
                    p_year = dates[2]
                return p_day + '-' + p_month + '-' + p_year
            else:
                return None
        else:
            return None

    def transform(self):
        """
        Transform limits
        :param data_date: Date of the limits
        :type data_date: str
        :return: DataFrame with the transformed limits
        :rtype: DataFrame
        """
        self.logger.info("Transforming limits")

        limits_df = self.limits.read_limits(self.data_date)

        # Filter active limits uploaded via launchpad.
        limits_active = limits_df.where(F.col('active_flag') == 1)

        # Drop duplicates.
        if limits_active.groupBy(*self.key_cols).agg(F.count('limit_type').alias('n')).where(
                F.col('n') > 1).count() > 0:
            self.logger.info("Droping limit duplicates")
            limits_active = Utilities.drop_duplicates(limits_active)

        # Analizamos marca STS
        sts_limits = (
            limits_active.where(
            F.col('limit_type') == 'sts'
            )
            .select('concept1_desc','concept1_value').collect()
        )

        sts_flag = sts_limits[0].concept1_value
        col_sts = sts_limits[0].concept1_desc

        if (sts_flag == '1'):
            self.logger.info('Titulización con cumplimiento STS')
            limits_current = (
                limits_active.where(
                    (F.col('concept1_value') != col_sts) |
                     (F.col('concept1_value') == col_sts) &
                      (F.col('concept1_value') == 1)
                )
            )
        else:
            self.logger.info('Titulización sin cumplimiento STS')
            sts_limites = limits_active.where((F.col('concept1_value') == col_sts) & (F.col('concept1_value') == 1))
            if (sts_limites.count() > 0):
                limits_current = limits_active.join(sts_limites, ['id_limit'], 'lefanti')
            else:
                sts_limites = (
                    limits_active.where(
                        (F.col('concept1_value') == col_sts) &
                        (F.col('concept1_value') == 1)
                    )
                )

                if (sts_limites.count() > 0):
                    limits_current = limits_active.join(sts_limites, ['id_limit'], 'lefanti')
                else:
                    limits_current = limits_active

        return limits_current  # GUARDAR A POSTGRES
