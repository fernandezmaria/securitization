import os

os.environ["ENMA_CONFIG_PATH"] = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), ".sandbox"
)
from py4j.java_gateway import JavaObject
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from joysticksecuritizatiotl3swp.config import get_params_from_runtime

if os.path.isfile(os.path.join(os.path.dirname(__file__), "dataflow.py")):
    from joysticksecuritizatiotl3swp.dataflow import dataproc_dataflow  # noqa: E402
else:
    from joysticksecuritizatiotl3swp.app import DataprocExperiment  # noqa: E402


class Main:
    """
    Main method for executing PySpark example process
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(Main.__qualname__)

    def main(self, runtimeContext: JavaObject) -> int:
        """
        THIS METHOD CANNOT BE REMOVED
        Application entry point
        """
        ret_code = 0
        parameters = {}

        # PART 1 - READ FROM CONFIGURATION
        # Reading config file for input and output paths
        try:
            config = runtimeContext.getConfig()
            if not config.isEmpty():
                root_key = "EnvironmentVarsPM"
                parameters = get_params_from_runtime(runtimeContext, root_key)
        except Exception as e:
            self.__logger.error(e)
            return -1

        self.__logger.info(f"parameters: {parameters}")

        # PART 2 - BUSSINESS LOGIC
        try:
            self.__logger.info("Started 'run' method")
            dataflow_path = os.path.join(os.path.dirname(__file__), "dataflow.py")
            if os.path.isfile(dataflow_path):
                self.__logger.info("Executing dataflow code")
                dataproc_dataflow.run_dataproc(**parameters)
                self.__logger.info("Dataflow code executed")
            else:
                self.__logger.info("Executing experiment code")
                entrypoint = DataprocExperiment()
                entrypoint.run(**parameters)
                self.__logger.info("Experiment code executed")
            self.__logger.info("Ended 'run' method")
        except Exception as e:
            ret_code = -1
            self.__logger.error(e)

        return ret_code
