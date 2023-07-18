# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import List

from dataclasses import make_dataclass


from canals import component
from canals.testing import BaseTestComponent


@component
class Repeat:
    """
    Repeats the input value on all outputs.
    """

    @component.input  # type: ignore
    def input(self):
        class Input:
            value: int

        return Input

    def __init__(self, outputs: List[str] = ["output_1", "output_2", "output_3"]):
        self.outputs = outputs
        self._output_type = make_dataclass("Output", fields=[(val, int, None) for val in outputs])

    @component.output  # type: ignore
    def output(self):
        return self._output_type

    def run(self, data):
        output_dataclass = self.output()
        for output in self.outputs:
            setattr(output_dataclass, output, data.value)
        return output_dataclass


class TestRepeat(BaseTestComponent):
    def test_saveload_default(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(Repeat(), tmp_path)

    def test_saveload_outputs(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(Repeat(outputs=["one", "two"]), tmp_path)

    def test_repeat_default(self):
        component = Repeat()
        results = component.run(component.input(value=10))
        assert results == component.output(output_1=10, output_2=10, output_3=10)
        assert component.init_parameters == {}

    def test_repeat_init(self):
        component = Repeat(outputs=["one", "two"])
        results = component.run(component.input(value=10))
        assert results == component.output(one=10, two=10)
        assert component.init_parameters == {"outputs": ["one", "two"]}
