# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Optional, Dict, Any, Type
import sys
import builtins
from importlib import import_module

from canals.component import component, Component
from canals.errors import ComponentDeserializationError


def _default_function(first: int, second: int) -> int:
    return first + second


@component
class Accumulate:  # pylint: disable=too-few-public-methods
    """
    Accumulates the value flowing through the connection into an internal attribute.
    The sum function can be customized.

    Example of how to deal with serialization when some of the parameters
    are not directly serializable.
    """

    def __init__(self, function: Optional[Callable] = None):
        """
        :param function: the function to use to accumulate the values.
            The function must take exactly two values.
            If it's a callable, it's used as it is.
            If it's a string, the component will look for it in sys.modules and
            import it at need. This is also a parameter.
        """
        self.state = 0
        self.function: Callable = _default_function if function is None else function  # type: ignore

    @component.output_types(value=int)
    def run(self, value: int):
        """
        Accumulates the value flowing through the connection into an internal attribute.
        The sum function can be customized.
        """
        self.state = self.function(self.state, value)
        return {"value": self.state}

    @classmethod
    def from_dict(cls: Type[Component], data: Dict[str, Any]) -> Component:
        """
        Loads the function by trying to import it.
        """
        if "type" not in data:
            raise ComponentDeserializationError("Missing 'type' in component serialization data")
        if data["type"] != cls.__name__:
            raise ComponentDeserializationError(f"Component '{data['type']}' can't be deserialized as '{cls.__name__}'")

        init_params = data.get("init_parameters", {})

        parts = init_params["function"].split(".")
        module_name = ".".join(parts[:-1])
        function_name = parts[-1]
        module = import_module(module_name)
        accumulator_function = getattr(module, function_name)

        return cls(function=accumulator_function)  # type: ignore

    def to_dict(self) -> Dict[str, Any]:
        """
        Saves the function by returning its import path to be used with `from_dict`
        (which uses `import_module` internally).
        """
        module = sys.modules.get(self.function.__module__)
        if not module:
            raise ValueError("Could not locate the import module.")
        if module == builtins:
            function_name = self.function.__name__
        else:
            function_name = f"{module.__name__}.{self.function.__name__}"

        return {
            "hash": id(self),
            "type": self.__class__.__name__,
            "init_parameters": {"function": function_name},
        }
