# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from canals import component
from canals.component.types import IsOptional


@component
class AddFixedValue:
    """
    Adds two values together.
    """

    def __init__(self, add: int = 1):
        self.add = add

    @component.output_types(result=int)
    def run(self, value: int, add: IsOptional[int] = None):
        """
        Adds two values together.
        """
        if add is None:
            add = self.add
        return {"result": value + add}
