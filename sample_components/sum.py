# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from canals import component
from canals.component.types import Variadic


@component
class Sum:  # pylint: disable=too-few-public-methods
    def __init__(self, base_value: int = 0):
        self.base_value = base_value

    @component.output_types(total=int)
    def run(self, values: Variadic[int]):
        """
        :param values: the values to sum
        """
        return {"total": self.base_value + sum(v for v in values if v is not None)}  # type: ignore
