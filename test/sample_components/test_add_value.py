# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from canals.component import component
from canals.testing.test_component import BaseTestComponent


@component
class AddFixedValue:
    """
    Adds the value of `add` to `value`. If not given, `add` defaults to 1.
    """

    @component.input
    class In:
        value: int
        add: int

    @component.output
    class Out:
        value: int

    def __init__(self, add: Optional[int] = 1):
        if add:
            self.In.add = add

    def run(self, data):
        return self.Out(value=data.value + data.add)


class TestAddFixedValue(BaseTestComponent):
    def test_saveload_default(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(AddFixedValue(), tmp_path)

    def test_saveload_add(self, tmp_path):
        self.assert_can_be_saved_and_loaded_in_pipeline(AddFixedValue(add=2), tmp_path)

    def test_addvalue(self):
        component = AddFixedValue()
        results = component.run(component.In(value=50, add=10))
        assert results == component.Out(value=60)
        assert component.init_parameters == {}
