# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0

from canals.component import component
from canals.testing import BaseTestComponent


@component
class Double:
    """
    Doubles the input value.
    """

    @component.input  # type: ignore
    class Input:
        value: int

    @component.output  # type: ignore
    class Output:
        value: int

    def run(self, data):
        """
        Doubles the input value
        """
        return self.Output(value=data.value * 2)


class TestDouble(BaseTestComponent):
    def test_saveload_default(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(Double(), tmp_path)

    def test_double_default(self):
        component = Double()
        results = component.run(component.Input(value=10))
        assert results == component.Output(value=20)
        assert component.init_parameters == {}
