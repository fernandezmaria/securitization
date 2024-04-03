from typing import Dict

from dataproc_sdk.dataproc_sdk_datiopysparksession import datiopysparksession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger

from joysticksecuritizatiotl3swp.executor.main import run as prun


class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.logger = get_user_logger(DataprocExperiment.__qualname__)

    def run(self, **parameters: Dict) -> int:
        """
        Execute the code written by the user.

        Args:
        ----
        parameters: The config file parameters

        Returns:
        -------
        This method returns:
          0 : Meaning the process went right.
         -1 : Meaning the process went wrong.
        """
        dataproc = datiopysparksession.DatioPysparkSession().get_or_create()
        spark = dataproc.getSparkSession()

        self.logger.info("Main.main")

        ret_code = 0

        try:
            prun(self.logger, spark, dataproc, parameters).execute_process()

        except Exception as exception:
            ret_code = -1
            self.logger.error("Exception: %s" % exception)

        return ret_code
