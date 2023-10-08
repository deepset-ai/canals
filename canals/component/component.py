# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
"""
    Attributes:

        component: Marks a class as a component. Any class decorated with `@component` can be used by a Pipeline.

    All components must follow the contract below. This docstring is the source of truth for components contract.

    <hr>

    `@component` decorator

    All component classes must be decorated with the `@component` decorator. This allows Canals to discover them.

    <hr>

    `__init__(self, **kwargs)`

    Optional method.

    Components may have an `__init__` method where they define:

    - `self.init_parameters = {same parameters that the __init__ method received}`:
        In this dictionary you can store any state the components wish to be persisted when they are saved.
        These values will be given to the `__init__` method of a new instance when the pipeline is loaded.
        Note that by default the `@component` decorator saves the arguments automatically.
        However, if a component sets their own `init_parameters` manually in `__init__()`, that will be used instead.
        Note: all of the values contained here **must be JSON serializable**. Serialize them manually if needed.

    Components should take only "basic" Python types as parameters of their `__init__` function, or iterables and
    dictionaries containing only such values. Anything else (objects, functions, etc) will raise an exception at init
    time. If there's the need for such values, consider serializing them to a string.

    _(TODO explain how to use classes and functions in init. In the meantime see `test/components/test_accumulate.py`)_

    The `__init__` must be extrememly lightweight, because it's a frequent operation during the construction and
    validation of the pipeline. If a component has some heavy state to initialize (models, backends, etc...) refer to
    the `warm_up()` method.

    <hr>

    `warm_up(self)`

    Optional method.

    This method is called by Pipeline before the graph execution. Make sure to avoid double-initializations,
    because Pipeline will not keep track of which components it called `warm_up()` on.

    <hr>

    `run(self, data)`

    Mandatory method.

    This is the method where the main functionality of the component should be carried out. It's called by
    `Pipeline.run()`.

    When the component should run, Pipeline will call this method with an instance of the dataclass returned by the
    method decorated with `@component.input`. This dataclass contains:

    - all the input values coming from other components connected to it,
    - if any is missing, the corresponding value defined in `self.defaults`, if it exists.

    `run()` must return a single instance of the dataclass declared through the method decorated with
    `@component.output`.

"""

import logging
import inspect
from typing import Protocol, Union, Dict, Any, get_origin, get_args
from functools import wraps

from canals.errors import ComponentError


logger = logging.getLogger(__name__)


class Component(Protocol):
    """
    Abstract interface of a Component.
    This is only used by type checking tools.
    If you want to create a new Component use the @component decorator.
    """

    def run(self, **kwargs) -> Dict[str, Any]:
        """
        Takes the Component input and returns its output.
        Inputs are defined explicitly by the run method's signature or with `component.set_input_types()` if dynamic.
        Outputs are defined by decorating the run method with `@component.output_types()`
        or with `component.set_output_types()` if dynamic.
        """


class ComponentMeta(type):
    def __call__(cls, *args, **kwargs):
        # This runs before __new__ is called
        run_signature = inspect.signature(cls.run)
        instance = super().__call__(*args, **kwargs)
        # If the __init__ called component.set_output_types(), __canals_output__ is already populated
        if not hasattr(instance, "__canals_output__"):
            # if the run method was decorated, it has a _output_types_cache field assigned
            instance.__canals_output__ = getattr(instance.run, "_output_types_cache", {})
        # If the __init__ called component.set_input_types(), __canals_input__ is already populated
        if not hasattr(instance, "__canals_input__"):
            instance.__canals_input__ = {
                # Create the input sockets
                param: {
                    "name": param,
                    "type": run_signature.parameters[param].annotation,
                    "is_optional": _is_optional(run_signature.parameters[param].annotation),
                }
                for param in list(run_signature.parameters)[1:]  # First is 'self' and it doesn't matter.
            }

        return instance


class _Component:
    """
    See module's docstring.

    Args:
        class_: the class that Canals should use as a component.
        serializable: whether to check, at init time, if the component can be saved with
        `save_pipelines()`.

    Returns:
        A class that can be recognized as a component.

    Raises:
        ComponentError: if the class provided has no `run()` method or otherwise doesn't respect the component contract.
    """

    def __init__(self):
        self.registry = {}

    def set_input_types(self, instance, **types):
        """
        Method that specifies the input types when 'kwargs' is passed to the run method.

        Use as:

        ```python
        @component
        class MyComponent:

            def __init__(self, value: int):
                component.set_input_types(value_1=str, value_2=str)
                ...

            @component.output_types(output_1=int, output_2=str)
            def run(self, **kwargs):
                return {"output_1": kwargs["value_1"], "output_2": ""}
        ```
        """
        instance.__canals_input__ = {
            name: {"name": name, "type": type_, "is_optional": _is_optional(type_)} for name, type_ in types.items()
        }

    def set_output_types(self, instance, **types):
        """
        Method that specifies the output types when the 'run' method is not decorated
        with 'component.output_types'.

        Use as:

        ```python
        @component
        class MyComponent:

            def __init__(self, value: int):
                component.set_output_types(output_1=int, output_2=str)
                ...

            # no decorators here
            def run(self, value: int):
                return {"output_1": 1, "output_2": "2"}
        ```
        """
        if not types:
            return

        instance.__canals_output__ = {name: {"name": name, "type": type_} for name, type_ in types.items()}

    def output_types(self, **types):
        """
        Decorator factory that specifies the output types of a component.

        Use as:

        ```python
        @component
        class MyComponent:
            @component.output_types(output_1=int, output_2=str)
            def run(self, value: int):
                return {"output_1": 1, "output_2": "2"}
        ```
        """

        def output_types_decorator(run_method):
            """
            This happens at class creation time, and since we don't have the instance
            available here, we temporarily store the output types as an attribute of
            the run method. The ComponentMeta metaclass will use this data to create
            sockets at instance creation time.
            """
            setattr(
                run_method,
                "_output_types_cache",
                {name: {"name": name, "type": type_} for name, type_ in types.items()},
            )
            return run_method

        return output_types_decorator

    def _component(self, class_):
        """
        Decorator validating the structure of the component and registering it in the components registry.
        """
        logger.debug("Registering %s as a component", class_)

        # Check for required methods and fail as soon as possible
        if not hasattr(class_, "run"):
            raise ComponentError(f"{class_.__name__} must have a 'run()' method. See the docs for more information.")

        # Recreate the component class so it uses our metaclass
        class_ = ComponentMeta(class_.__name__, class_.__bases__, dict(class_.__dict__))

        # Save the component in the class registry (for deserialization)
        if class_.__name__ in self.registry:
            # Corner case, but it may occur easily in notebooks when re-running cells.
            logger.debug(
                "Component %s is already registered. Previous imported from '%s', new imported from '%s'",
                class_.__name__,
                self.registry[class_.__name__],
                class_,
            )
        self.registry[class_.__name__] = class_
        setattr(class_, "__canals_component__", True)

        logger.debug("Registered Component %s", class_)

        return class_

    def __call__(self, class_=None):
        """Allows us to use this decorator with parenthesis and without."""
        if class_:
            return self._component(class_)

        return self._component


component = _Component()


def _is_optional(type_: type) -> bool:
    """
    Utility method that returns whether a type is Optional.
    """
    return get_origin(type_) is Union and type(None) in get_args(type_)
