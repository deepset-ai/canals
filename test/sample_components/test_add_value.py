# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple, Dict, Union, Any, get_origin, get_args


from canals.component import component, Input, Output
from canals.testing.test_component import BaseTestComponent


@component
class AddFixedValue:
    def __init__(self, add: int = 1):
        self.input = Input(value=int, add=int)
        self.output = Output(value=int)

        self.input.set_defaults(add=add)

    def run(self, data):
        return self.output(value=data.value + data.add)


class TestAddFixedValue(BaseTestComponent):
    def test_saveload_default(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(AddFixedValue(), tmp_path)

    def test_saveload_add(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(AddFixedValue(add=2), tmp_path)

    def test_addvalue(self):
        component = AddFixedValue()
        results = component.run(component.input(value=50, add=10))
        assert results == component.output(value=60)
        assert component.init_parameters == {}
