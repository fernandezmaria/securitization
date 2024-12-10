from pyspark.sql import functions as F
from rubik.load.rorc import Values as RORCvalues

from joysticksecuritizatiotl3swp.joysticksecuritizatiotl3swp.configurations.catalogues import non_ig_limit
from joysticksecuritizatiotl3swp.joysticksecuritizatiotl3swp.read.catalogue_sector_project import CatalogueSectorProjectLoader
from joysticksecuritizatiotl3swp.joysticksecuritizatiotl3swp.read.paths import Paths
from joysticksecuritizatiotl3swp.joysticksecuritizatiotl3swp.utils.utilities import Utilities
from joysticksecuritizatiotl3swp.joysticksecuritizatiotl3swp.utils.algorithm_utils import SecuritizationUtils


class SecurizationsTransform:
    """
    Class to transform securizations data for algorithm usage.
    """

    def __init__(self, logger, dataproc, parameters, data_date, limits_df):
        """
        Constructor
        """
        self.logger = logger
        self.dataproc = dataproc
        self.parameters = parameters
        self.data_date = data_date
        self.paths = Paths(parameters=self.parameters)
        self.limits_df = limits_df

        self.securization_type = SecuritizationUtils.get_securization_type(limits_df)
        self.catalogue_sector_project_df = CatalogueSectorProjectLoader(logger ,dataproc,
                                                                        parameters).read_catalogue_sector_project_relation()
        self.path_ci = self.paths.path_ci
        self.raw_ci_df = self.dataproc.read().parquet(self.path_ci)
        self.ci_date_field = "gf_cutoff_date"
        self.area = "GF"

        # Rubik constants
        self.rorcValues = RORCvalues(path="/data", dataproc=dataproc)
        self.tax_rate = self.rorcValues.TaxRate()
        self.ratio_cet1 = self.rorcValues.CET1()

        self.non_ig_limit = non_ig_limit

    def build_ci_df(self):
        data_date = Utilities.last_partition(self.path_ci, self.ci_date_field)

        ci_df = self.raw_ci_df.where(F.col(self.ci_date_field) == data_date
                                     ).where(F.col('gf_business_area_id') == self.area
                                             ).select('gf_customer_contract_control_per', 'gf_head_office_desc')
        return ci_df

    def build_securization_for_algorithm(self, securizations_df):
        """
        Build securizations for algorithm.
        """
        securizations_for_algorithm_df = (
            securizations_df.withColumn(
                'project_sector_desc',
                F.trim('project_sector_desc')
            )
            .join(self.catalogue_sector_project_df, ['project_sector_desc'], 'left')
            .fillna('No Informado')
        )

        securizations_for_algorithm_df = (
            securizations_for_algorithm_df.withColumn(
                'ico_flag',
                F.when(F.trim(F.col('deal_purpose_type')) == "ICO España", 1)
                .otherwise(0)
            )
        )

        tipo_titulizacion = self.securization_type
        col_rating_categ = self.non_ig_limit[tipo_titulizacion]['categoria']
        col_rating_pos = self.non_ig_limit[tipo_titulizacion]['valor']

        n_rating = [
            x[col_rating_pos] for x in
            securizations_for_algorithm_df.select(col_rating_pos).where(F.col(col_rating_categ) == 'BB+1').distinct()
            .collect()
        ][0]

        securizations_for_algorithm_df = (
            securizations_for_algorithm_df.withColumn(
                'non_ig_flag',
                F.when(
                    ((F.col(col_rating_pos) >= n_rating) & (~(F.col(col_rating_categ).like('BBB%')))),
                    1
                ).otherwise(0)
            )
        )

        securizations_for_algorithm_df = (
            securizations_for_algorithm_df.withColumn(
                'building_project_flag',
                F.when(F.trim(F.col('gf_pf_project_const_type')) == 'S', 1).otherwise(0)
            )
        )

        securizations_for_algorithm_df = (
            securizations_for_algorithm_df.withColumn(
                'workout_flag',
                F.when(F.trim(F.col('watch_list_clasification_type')) != 0, 1).otherwise(0)
            )
        )

        securizations_for_algorithm_df = (
            securizations_for_algorithm_df.withColumn(
                'sts_payment_flag',
                F.when(F.col('sts_payment_condition') == 'true', 1).otherwise(0)
            )
        )

        securizations_for_algorithm_df = (
            securizations_for_algorithm_df.withColumn(
                'sts_sm_rw_flag',
                F.when(F.col('sts_sm_rw_condition') == 'true', 1).otherwise(0)
            )
        )

        securizations_for_algorithm_df = (
            securizations_for_algorithm_df.withColumn(
                'esg_linked_flag',
                F.when(F.col('esg_linked') == 1, 1).otherwise(0)
            )
        )

        securizations_for_algorithm_df = (
            securizations_for_algorithm_df.withColumn(
                'bei_flag',
                F.when((F.col('bei_guaranteed_amount') != 0) & F.col('bei_guaranteed_amount').isNotNull(), 1).otherwise(0)
            )
        )

        securizations_for_algorithm_df = (
            securizations_for_algorithm_df.withColumn(
                'data_date',
                F.lit(self.data_date)
            )
        )
        return securizations_for_algorithm_df  # ESCRIBIR EN POSTGRES

    def build_constants_df(self, limits_df, securizations_df):
        constants_df = (
            limits_df.where(
                F.col('limit_type') == 'constant_type'
            )
            .select(
                F.col('concept1_desc').alias('constant_type'), F.col('limit_value').alias('constant_value')
            )
        )

        lgd = securizations_df.agg(F.avg(F.col("adj_lgd_ma_mitig_per")).cast('float').alias('lgd')).collect()[0].lgd

        ci_ratio = (
            self.build_ci_df().where(
                F.trim(F.col('gf_head_office_desc')) == 'ESPAÑA')
            .select(F.col('gf_customer_contract_control_per').cast('float'))
            .collect()[0]
            .gf_customer_contract_control_per
        )

        hardcoded_constants_list = [
            {"constant_type": 'tax_rate', "constant_value": self.tax_rate},
            {"constant_type": 'ratio_cet1', "constant_value": self.ratio_cet1},
            {"constant_type": 'lgd', "constant_value": lgd},
            {"constant_type": 'ci_ratio', "constant_value": ci_ratio}
        ]

        hardcoded_constants_df = self.dataproc.getSparkSession().createDataFrame(hardcoded_constants_list)
        constants_final_df = (
            hardcoded_constants_df.union(constants_df)
            .withColumn('closing_date', F.lit(self.data_date))
        )

        return constants_final_df  # ESCRIBIRLO EN POSTGRES
