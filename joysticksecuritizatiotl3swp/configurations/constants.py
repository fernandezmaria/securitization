"""
This module contains the hardcoded constants the process uses
"""


class Constants:  # pragma: no cover
    """
    Class containing the constants
    """

    def __init__(self, parameters):
        """
        Constructor
        """
        self.CONFIG = {
            'PRODUCTION': {
                'SANDBOX_PATH': parameters['SANDBOX_PATH_PROD']
            },
            'DEVELOPMENT': {
                'SANDBOX_PATH': parameters['SANDBOX_PATH_DEV']
            }
        }
