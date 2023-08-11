# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from canals import component


@component
class Sum:  # pylint: disable=too-few-public-methods
    def __init__(self, inputs, base_value: int = 0):
        self.base_value = base_value
        self.init_parameters = {"inputs": inputs, "base_value": base_value}
        component.set_input_types(self, **{input_name: Optional[int] for input_name in inputs})

    @component.output_types(total=int)
    def run(self, base_value: Optional[int] = None, **kwargs):
        """
        :param value: the value to check the remainder of.
        """
        if base_value is None:
            base_value = self.base_value

        return {"total": base_value + sum(v for v in kwargs.values() if v is not None)}
