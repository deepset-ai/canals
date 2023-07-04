# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Union, Optional, Dict, Set, Any, get_origin, get_args

import logging
from dataclasses import dataclass, fields, Field, _MISSING_TYPE


logger = logging.getLogger(__name__)


@dataclass
class OutputSocket:
    name: str
    types: Set[type]
    default: Any


@dataclass
class InputSocket:
    name: str
    types: Set[type]
    default: Any
    sender: Optional[str] = None


def find_input_sockets(component) -> Dict[str, InputSocket]:
    """
    Find a component's input sockets.
    """
    return {
        f.name: InputSocket(name=f.name, types=_get_types(f), default=_get_default(f)) for f in fields(component.input)
    }


def find_output_sockets(component) -> Dict[str, OutputSocket]:
    """
    Find a component's output sockets.
    """
    return {
        f.name: OutputSocket(name=f.name, types=_get_types(f), default=_get_default(f))
        for f in fields(component.output)
    }


def _get_types(field: Field) -> Set[Any]:
    if get_origin(field.type) is Union:
        return {t for t in get_args(field.type) if t is not type(None)}
    return {field.type}


def _get_default(field: Field) -> Any:
    if not isinstance(field.default, _MISSING_TYPE):
        return field.default
    if not isinstance(field.default_factory, _MISSING_TYPE):
        return field.default_factory
    return _MISSING_TYPE
