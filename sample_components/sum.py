# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Dict, Any

from canals import component
from canals.serialization import default_to_dict, default_from_dict


@component
class Sum:  # pylint: disable=too-few-public-methods
    def __init__(self, inputs, base_value: int = 0):
        self.inputs = inputs
        self.base_value = base_value
        component.set_input_types(self, **{input_name: Optional[int] for input_name in inputs})

    def to_dict(self) -> Dict[str, Any]:  # pylint: disable=missing-function-docstring
        return default_to_dict(self, inputs=self.inputs, base_value=self.base_value)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Sum":  # pylint: disable=missing-function-docstring
        return default_from_dict(cls, data)

    @component.output_types(total=int)
    def run(self, base_value: Optional[int] = None, **kwargs):
        """
        :param value: the value to check the remainder of.
        """
        if base_value is None:
            base_value = self.base_value

        return {"total": base_value + sum(v for v in kwargs.values() if v is not None)}
