# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Type, List

from canals import component
from canals.component.types import Variadic


@component
class Joiner:
    def __init__(self, inputs_type: Type):
        component.set_input_types(self, input=Variadic[inputs_type])
        component.set_output_types(self, output=List[inputs_type])

    def run(self, input):
        return {"output": input}
