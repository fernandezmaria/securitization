from unittest import TestCase
from unittest.mock import patch, mock_open, MagicMock
import os
import sys

from joysticksecuritizatiotl3swp.config import get_params_from_args
from joysticksecuritizatiotl3swp.config import get_params_from_env
from joysticksecuritizatiotl3swp.config import get_params_from_hps


class ConfigTestCase(TestCase):

    """
    Test case for a Python entrypoint execution.
    """

    def test__get_params_from_args_empty(self) -> None:
        testargs = ["prog"]

        with patch.object(sys, 'argv', testargs):
            parameters = get_params_from_args()

        self.assertEqual(parameters, {})

    def test__get_params_from_args(self) -> None:
        testargs = ["prog", "PARAM_FILE", "foo.txt"]

        with patch.object(sys, 'argv', testargs):
            parameters = get_params_from_args()

        self.assertEqual(parameters, {"FILE": "foo.txt"})

    def test__get_params_from_env_empty(self) -> None:
        testargs = {}

        with patch.object(os, 'environ', testargs):
            parameters = get_params_from_env()

        self.assertEqual(parameters, {})

    def test__get_params_from_env(self) -> None:
        testargs = {"PARAM_FILE": "foo.txt"}

        with patch.object(os, 'environ', testargs):
            parameters = get_params_from_env()

        self.assertEqual(parameters, {"FILE": "foo.txt"})

    def test__get_params_from_hps_empty(self) -> None:
        testargs = {}

        with patch.object(os, 'environ', testargs):
            parameters = get_params_from_hps()

        self.assertEqual(parameters, {})

    def test__get_params_from_hps(self) -> None:
        testargs = {
            'USER': 'user123',
            'SM_HPS': '{"FILE": "foo.txt", "PACKAGE_NAME": "my_package"}'
        }

        with patch.object(os, 'environ', testargs):
            parameters = get_params_from_hps()

        self.assertEqual(parameters, {"FILE": "foo.txt"})
