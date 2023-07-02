# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from canals.testing import BaseTestComponent
from canals.component import component


@component
class Subtract:
    """
    Compute the difference between two values.
    """

    @component.input
    class Input:
        first_value: int
        second_value: int

    @component.output
    class Output:
        difference: int

    def run(self, data):
        """
        :param first_value: name of the connection carrying the value to subtract from.
        :param second_value: name of the connection carrying the value to subtract.
        """
        return self.Output(difference=data.first_value - data.second_value)


class TestSubtract(BaseTestComponent):
    def test_saveload_default(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(Subtract(), tmp_path)

    def test_subtract(self):
        component = Subtract()
        results = component.run(component.Input(first_value=10, second_value=7))
        assert results == component.Output(difference=3)
        assert component.init_parameters == {}
