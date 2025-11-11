from typing import Dict

from dataproc_sdk.dataproc_sdk_datiopysparksession import datiopysparksession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import pyspark.sql.functions as F
from alfred.logging.tracing import ExecutionLogger

from joysticksecuritizatiotl3swp.executor.main import SecuritizationProcess
from joysticksecuritizatiotl3swp.utils.utilities import Utilities


class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.logger = get_user_logger(DataprocExperiment.__qualname__)

    @ExecutionLogger.log_func_details
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
            if parameters["STAGE"] == "ALGORITHM":
                sandbox_path = parameters["SANDBOX_PATH_DEV"] if parameters[
                    "OUTPUT_MODE"] == "DEVELOPMENT" else parameters["SANDBOX_PATH_PROD"]

                clan_date = parameters["CLAN_DATE"]

                if (Utilities.check_for_partition(
                        f"{sandbox_path}mrr/joystick_mrr", "clan_date", clan_date)):
                    cubo_df = (
                        dataproc.read()
                        .option("basePath", f"{sandbox_path}mrr/joystick_mrr")
                        .parquet(f"{sandbox_path}mrr/joystick_mrr/clan_date={clan_date}")
                    )
                    if "delta_file_id" in cubo_df.columns:
                        cubo_df = Utilities.transform_mrr_columns(cubo_df)
                    SecuritizationProcess(
                        self.logger, spark, dataproc, parameters
                    ).execute_algorithm(cubo_df)

                else:
                    self.logger.info(
                        "Clan date %s not found in MRR partition. Executing full engine." % clan_date)
                    SecuritizationProcess(self.logger, spark,
                                          dataproc, parameters).execute_process()

            else:
                SecuritizationProcess(
                    self.logger, spark, dataproc, parameters
                ).execute_process()

        except Exception as exception:
            self.logger.error("Exception: %s" % exception)
            raise exception

        return ret_code
