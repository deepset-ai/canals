# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import get_origin, get_args, List, Type, Union
import logging
from dataclasses import dataclass, field

from canals.component.types import Variadic


logger = logging.getLogger(__name__)


@dataclass
class InputSocket:
    name: str
    type: Type
    is_optional: bool = field(init=False)
    is_variadic: bool = field(init=False)
    sender: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.is_optional = get_origin(self.type) is Union and type(None) in get_args(self.type)
        self.is_variadic = get_origin(self.type) is Variadic
        if self.is_variadic:
            # We need to "unpack" the type inside the Variadic container,
            # otherwise the pipeline connection api will try to match "Variadic"
            self.type = get_args(self.type)


@dataclass
class OutputSocket:
    name: str
    type: type
