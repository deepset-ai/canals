# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from canals import component


@component
class Parity:
    """
    Redirects the value, unchanged, along the 'even' connection if even, or along the 'odd' one if odd.
    """

    @component.return_types(even=int, odd=int)
    def run(self, value: int):
        """
        :param value: The value to check for parity
        """
        remainder = value % 2
        if remainder:
            return {"even": None, "odd": value}
        return {"even": value, "odd": None}