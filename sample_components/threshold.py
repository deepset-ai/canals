# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from canals import component
from canals.component.types import IsOptional


@component
class Threshold:  # pylint: disable=too-few-public-methods
    """
    Redirects the value, unchanged, along a different connection whether the value is above
    or below the given threshold.

    :param threshold: the number to compare the input value against. This is also a parameter.
    """

    def __init__(self, threshold: int = 10):
        """
        :param threshold: the number to compare the input value against.
        """
        self.threshold = threshold

    @component.output_types(above=int, below=int)
    def run(self, value: int, threshold: IsOptional[int] = None):
        """
        Redirects the value, unchanged, along a different connection whether the value is above
        or below the given threshold.

        :param threshold: the number to compare the input value against. This is also a parameter.
        """
        if threshold is None:
            threshold = self.threshold

        if value < threshold:
            return {"above": None, "below": value}
        return {"above": value, "below": None}
