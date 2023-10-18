# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from canals import component
from canals.component.types import Variadic


@component
class StringJoiner:
    def __init__(self):
        component.set_input_types(self, input_str=Variadic[str])

    @component.output_types(output=str)
    def run(self, input_str):
        """
        Take strings from multiple input nodes and join them
        into a single one returned in output. Since `input_str`
        is Variadic, we know we'll receive a List[str].
        """
        return {"output": " ".join(input_str)}
