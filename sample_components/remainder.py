# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0

from canals import component


@component
class Remainder:  # pylint: disable=too-few-public-methods
    def __init__(self, divisor=3):
        if divisor == 0:
            raise ValueError("Can't divide by zero")
        self.divisor = divisor
        component.set_output_types(self, **{f"remainder_is_{val}": int for val in range(divisor)})

    def run(self, value: int):
        """
        :param value: the value to check the remainder of.
        """
        remainder = value % self.divisor
        output = {f"remainder_is_{val}": None if val != remainder else value for val in range(self.divisor)}
        return output