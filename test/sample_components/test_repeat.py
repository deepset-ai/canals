# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import List

from dataclasses import make_dataclass, fields


from canals.testing import BaseTestComponent
from canals.component import component


@component
class Repeat:
    """
    Repeats the input value on all outputs.
    """

    @component.input
    class Input:
        value: int

    @component.output
    class Output:
        pass

    def __init__(self, outputs: List[str] = ["output_1", "output_2", "output_3"]):
        self.Output = make_dataclass("Output", fields=[(val, int, None) for val in outputs])

    def run(self, data):
        output_dataclass = self.Output()
        for f in fields(self.Output):
            setattr(output_dataclass, f.name, data.value)
        return output_dataclass


class TestRepeat(BaseTestComponent):
    def test_saveload_default(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(Repeat(), tmp_path)

    def test_saveload_outputs(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(Repeat(outputs=["one", "two"]), tmp_path)

    def test_repeat_default(self):
        component = Repeat()
        results = component.run(component.Input(value=10))
        assert results == component.Output(output_1=10, output_2=10, output_3=10)
        assert component.init_parameters == {}

    def test_repeat_init(self):
        component = Repeat(outputs=["one", "two"])
        results = component.run(component.Input(value=10))
        assert results == component.Output(one=10, two=10)
        assert component.init_parameters == {"outputs": ["one", "two"]}
