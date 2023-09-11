# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, Any

from canals.serialization import default_to_dict, default_from_dict
from canals import component


@component
class Subtract:
    """
    Compute the difference between two values.
    """

    @component.output_types(difference=int)
    def run(self, first_value: int, second_value: int):
        """
        :param first_value: name of the connection carrying the value to subtract from.
        :param second_value: name of the connection carrying the value to subtract.
        """
        return {"difference": first_value - second_value}
