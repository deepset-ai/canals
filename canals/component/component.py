# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
import logging
import inspect
from typing import Protocol, Any
from functools import wraps

from canals.errors import ComponentError
from canals.component.input_output import Input, Output


logger = logging.getLogger(__name__)


# We ignore too-few-public-methods Pylint error as this is only meant to be
# the definition of the Component interface.
# A concrete Component will have more than method in any case.
class Component(Protocol):  # pylint: disable=too-few-public-methods
    """
    Abstract interface of a Component.
    This is only used by type checking tools.

    If you want to create a new Component use the @component decorator.
    """

    input: Input
    output: Output

    def run(self, data: Any) -> Any:
        """
        Takes the Component input and returns its output.
        Input and output dataclasses types must be defined in separate methods
        decorated with @component.input and @component.output respectively.

        We use Any both as data and return types since dataclasses don't have a specific type.
        """


class _Component:  # pylint: disable=too-few-public-methods
    """
    Marks a class as a component. Any class decorated with `@component` can be used by a Pipeline.

    All components must follow the contract below. This docstring is the source of truth for components contract.

    ### `@component` decorator

    All component classes must be decorated with the `@component` decorator. This allows Canals to discover them.

    ### `__init__()`

    ```python
    def __init__(self, [... components init parameters ...]):
    ```
    Mandatory method.

    Components may have an `__init__` method where they define:

    - `self.input`: TODO
    - `self.output`: TODO

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

    ### `warm_up()`

    ```python
    def warm_up(self):
    ```
    Optional method.

    This method is called by Pipeline before the graph execution. Make sure to avoid double-initializations,
    because Pipeline will not keep track of which components it called `warm_up()` on.

    ### `run()`

    ```python
    def run(self, data: <Input if defined, otherwise untyped>) -> <Output if defined, otherwise untyped>:
    ```
    Mandatory method.

    This is the method where the main functionality of the component should be carried out. It's called by
    `Pipeline.run()`.

    When the component should run, Pipeline will call this method with:

    - all the input values coming from other components connected to it,
    - if any is missing, the corresponding default value from the dataclass, if it exists.

    `run()` must return a single instance of the dataclass declared through `self.output`.

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

    def _decorate(self, class_):
        # '__canals_component__' is used to distinguish components from regular classes.
        # Its value is set to the desired component name: normally it is the class name, but it can technically be customized.
        class_.__canals_component__ = class_.__name__

        # Check that the run method respects all constraints
        _check_run_signature(class_)

        # Makes sure the self.init_parameters is always present
        class_.init_parameters = {}

        # Automatically registers all the init parameters in an instance attribute called `init_parameters`.
        class_.__init__ = _save_init_params(class_.__init__)

        if class_.__name__ in self.registry:
            logger.error(
                "Component %s is already registered. Previous imported from '%s', new imported from '%s'",
                class_.__name__,
                self.registry[class_.__name__],
                class_,
            )

        self.registry[class_.__name__] = class_
        logger.debug("Registered Component %s", class_)

        return class_

    def __call__(self, class_=None):
        if class_:
            return self._decorate(class_)

        return self._decorate


component = _Component()


def _check_run_signature(class_):
    """
    Check that the component's run() method exists and respects all constraints
    """
    # Check for run()
    if not hasattr(class_, "run"):
        raise ComponentError(f"{class_.__name__} must have a 'run()' method. See the docs for more information.")
    run_signature = inspect.signature(class_.run)

    # run() must take a single input param
    if len(run_signature.parameters) != 2:
        raise ComponentError("run() must accept only a single parameter called 'data'.")

    # The input param must be called data
    if not "data" in run_signature.parameters:
        raise ComponentError("run() must accept a parameter called 'data'.")


def _save_init_params(init_func):
    """
    Decorator that saves the init parameters of a component in `self.init_parameters`
    """

    @wraps(init_func)
    def wrapper(self, *args, **kwargs):
        # Call the actual __init__ function with the arguments
        init_func(self, *args, **kwargs)

        if not hasattr(self, "input"):
            raise ComponentError("Component must declare their input as self.input = Input(...fields...)")
        if not hasattr(self, "output"):
            raise ComponentError("Component must declare their output as self.output = Output(...fields...)")

        # Collect and store all the init parameters, preserving whatever the components might have already added there
        self.init_parameters = {**kwargs, **self.init_parameters}

    return wrapper
