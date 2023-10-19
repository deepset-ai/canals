# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import List

from canals import component


@component
class TextSplitter:
    @component.output_types(output=List[str])
    def run(self, sentence: str):
        """
        Takes a string in input and returns "Hello, <string>!"
        in output.
        """
        return {"output": sentence.split()}
