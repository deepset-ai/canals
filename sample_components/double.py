# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, Any

from canals.serialization import default_to_dict, default_from_dict
from canals import component


@component
class Double:
    """
    Doubles the input value.
    """

    @component.output_types(value=int)
    def run(self, value: int):
        """
        Doubles the input value.
        """
        return {"value": value * 2}
