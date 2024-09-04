import logging
from unittest import TestCase
from unittest.mock import PropertyMock, MagicMock, patch
import os

from joysticksecuritizatiotl3swp.run_pyspark import Main

from py4j.java_gateway import GatewayProperty, GatewayClient, JavaObject
from py4j.protocol import Py4JJavaError


class TestApp(TestCase):
    """
    Test class for Dataproc Pyspark job entrypoint execution
    """

    @patch('joysticksecuritizatiotl3swp.run_pyspark.get_params_from_env')
    def test_app_unknown_error(self, mock_get_params):
        mock_get_params.side_effect = Exception("Test exception")

        app_main = Main()
        runtimeContext = MagicMock()  # This may not be necessary if get_params_from_env is mocked

        ret_code = app_main.main(runtimeContext)

        self.assertEqual(ret_code, -1)

    def test_app_config(self):
        """
        Test app entrypoint execution with config
        """

        runtimeContext = MagicMock()
        app_main = Main()

        if os.path.exists("./joysticksecuritizatiotl3swp/dataflow.py"):
            with patch("joysticksecuritizatiotl3swp.run_pyspark.dataproc_dataflow"):
                ret_code = app_main.main(runtimeContext)
        else:
            with patch("joysticksecuritizatiotl3swp.run_pyspark.DataprocExperiment"):
                ret_code = app_main.main(runtimeContext)

        self.assertEqual(ret_code, 0)