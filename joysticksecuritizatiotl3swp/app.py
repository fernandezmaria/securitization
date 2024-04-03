
from typing import Dict
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger


class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)

    def run(self, **parameters: Dict) -> None:
        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """
        # -------------------------
        # - Your code starts here -
        # -------------------------
        pass
