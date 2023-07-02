# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional


from canals.testing import BaseTestComponent
from canals.component import component


@component
class Threshold:
    """
    Redirects the value, unchanged, along a different connection whether the value is above
    or below the given threshold.

    Single input, double output decision component.

    :param threshold: the number to compare the input value against. This is also a parameter.
    """

    @component.input
    class Input:
        value: int
        threshold: int = 10

    @component.output
    class Output:
        above: int
        below: int

    def __init__(self, threshold: Optional[int] = None):
        """
        :param threshold: the number to compare the input value against.
        """
        if threshold:
            self.Input.threshold = threshold

    def run(self, data):
        if data.value < data.threshold:
            return self.Output(above=None, below=data.value)
        return self.Output(above=data.value, below=None)


class TestThreshold(BaseTestComponent):
    def test_saveload_default(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(Threshold(), tmp_path)

    def test_saveload_threshold(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(Threshold(threshold=3), tmp_path)

    def test_threshold(self):
        component = Threshold()

        results = component.run(component.Input(value=5, threshold=10))
        assert results == component.Output(above=None, below=5)

        results = component.run(component.Input(value=15, threshold=10))
        assert results == component.Output(above=15, below=None)
