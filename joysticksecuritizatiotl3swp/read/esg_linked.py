"""
This module is responsible for building the ESG linked operations identifier
"""

from grubik.services.contracts.contract_sust_linked import ContractSustLinked
from pyspark.sql import functions as F


class ESGLinkedBuilder:
    """This class provides the ESG linked operations identifier."""
    def __init__(self, logger, dataproc, data_date, contract_relations):
        """
        Constructor
        """
        self.logger = logger
        self.dataproc = dataproc
        self.data_date = data_date
        self.contract_relations = contract_relations
        self.contract_sust_linked = ContractSustLinked(dataproc)

    def build_esg_linked_flag(self):
        """
        This method builds the ESG linked operations identifier.

        Returns:
        -------
        This method returns the ESG linked operations identifier (pyspark.sql.DataFrame).
        """
        self.logger.info("ESGLinkedBuilder.build_esg_linked")
        # RBES sustainability mark assigned to ESG Linked according to SUST catalog in Taxonomy
        sust = self.contract_sust_linked.get_contract_sust_linked(date=self.data_date) \
            .filter(F.col('gf_sustainability_mark_id') == 'RBES')\
            .select(F.lit(1).alias('esg_linked'), 'g_contract_id').distinct()
        sust_operations = self.contract_relations.filter(F.col('g_glob_contract_hier_lvl_type') == 'FACILITY') \
            .join(sust, on='g_contract_id', how='left') \
            .fillna(0, subset=['esg_linked'])
        return sust_operations




