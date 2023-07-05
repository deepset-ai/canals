# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Union, List, get_origin, get_args, Tuple, ClassVar, Any, TYPE_CHECKING, Dict
from dataclasses import make_dataclass, fields, _MISSING_TYPE, Field


class Input:
    """
    The input data of a component
    """

    __canals_optionals__: List[str]
    __canals_mandatory__: List[str]

    if TYPE_CHECKING:
        __dataclass_fields__: ClassVar[Dict[str, Field[Any]]]  # To please mypy's DataclassInstance protocol

    def __new__(cls, **dataclass_fields):
        final_fields = []
        for name, data in dataclass_fields.items():
            if isinstance(data, Tuple):
                final_fields.append((name, *data))
            else:
                final_fields.append((name, data, None))
        dataclass = make_dataclass("Input", final_fields)

        dataclass.__canals_mandatory__, dataclass.__canals_optionals__ = Input._split_mandatory_and_optionals(
            [(name, data[0]) if isinstance(data, Tuple) else (name, data) for name, data in dataclass_fields.items()]
        )
        return dataclass

    @staticmethod
    def from_dataclass(dataclass):
        """
        Transforms the given dataclass into a Canals-compatible Input dataclass
        """
        dataclass.__canals_mandatory__, dataclass.__canals_optionals__ = Input._split_mandatory_and_optionals(
            [(field.name, field.type) for field in fields(dataclass)]
        )
        for field in fields(dataclass):
            if field.default is _MISSING_TYPE and field.default_factory is _MISSING_TYPE:
                field.default = None
        return dataclass

    @staticmethod
    def _split_mandatory_and_optionals(dataclass_fields: List[Tuple[str, type]]):
        """
        Splits the input list of (field-name, field-type) into mandatory and optional field lists.
        """
        optional_fields = []
        mandatory_fields = []
        for name, type_ in dataclass_fields:
            if get_origin(type_) is Union and type(None) in get_args(type_):
                optional_fields.append(name)
            else:
                mandatory_fields.append(name)
        return mandatory_fields, optional_fields


class Output:
    """
    The output data of a component
    """

    def __new__(cls, **dataclass_fields):
        final_fields = [
            (name, *data) if isinstance(data, Tuple) else (name, data, None) for name, data in dataclass_fields.items()
        ]
        return make_dataclass("Output", final_fields)

    @staticmethod
    def from_dataclass(dataclass):
        """
        Transforms the given dataclass into a Canals-compatible Output dataclass
        """
        return dataclass
