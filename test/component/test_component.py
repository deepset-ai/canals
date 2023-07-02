import pytest

from canals.component import component
from canals.errors import ComponentError


def test_correct_declaration():
    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    # Verifies also instantiation works with no issues
    assert MockComponent()
    assert component.registry["MockComponent"] == MockComponent


def test_input_inherits():
    from dataclasses import dataclass

    @dataclass
    class SuperInput:
        value: int

    @component
    class MockComponent:
        @component.input
        class Input(SuperInput):
            pass

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    # Verifies also instantiation works with no issues
    comp = MockComponent()
    assert comp
    comp.Input.value = 2
    assert comp.Input().value == 2
    assert comp.Input(1).value == 1


def test_input_required():
    with pytest.raises(
        ComponentError,
        match="No input definition found in Component MockComponent. "
        "Create a method that returns a dataclass defining the input and "
        "decorate it with @component.input\(\) to fix the error.",
    ):

        @component
        class MockComponent:
            @component.output
            class Output:
                output_value: int

            def run(self, data):
                return MockComponent.Output(output_value=1)


def test_output_required():
    with pytest.raises(
        ComponentError,
        match="No output definition found in Component MockComponent. "
        "Create a method that returns a dataclass defining the output and "
        "decorate it with @component.output\(\) to fix the error.",
    ):

        @component
        class MockComponent:
            @component.input
            class Input:
                input_value: int

            def run(self, data):
                return 1


# TODO: How should we behave in this case?
@pytest.mark.skip
def test_only_single_input_defined():
    with pytest.raises(
        ComponentError,
        match="Multiple input definitions found for Component MockComponent",
    ):

        @component
        class MockComponent:
            @component.input
            class Input:
                input_value: int

            @component.input
            class Input:
                input_value: int

            @component.output
            class Output:
                output_value: int

            def run(self, data):
                return self.Output(output_value=1)


# TODO: How should we behave in this case?
@pytest.mark.skip
def test_only_single_output_defined():
    with pytest.raises(
        ComponentError,
        match="Multiple output definitions found for Component MockComponent",
    ):

        @component
        class MockComponent:
            @component.input
            class Input:
                input_value: int

            @component.output
            class Output:
                output_value: int

            @component.output
            class Output:
                output_value: int

            def run(self, data):
                return self.Output(output_value=1)


def test_check_for_run():
    with pytest.raises(ComponentError, match="must have a 'run\(\)' method"):

        @component
        class MockComponent:
            @component.input
            class Input:
                input_value: int

            @component.output
            class Output:
                output_value: int


def test_run_takes_only_one_param():
    with pytest.raises(ComponentError, match="must accept only a single parameter called 'data'."):

        @component
        class MockComponent:
            @component.input
            class Input:
                input_value: int

            @component.output
            class Output:
                output_value: int

            def run(self, data, something_else: int):
                return self.Output(output_value=1)


def test_run_takes_only_kwarg_data():
    with pytest.raises(ComponentError, match="must accept a parameter called 'data'."):

        @component
        class MockComponent:
            @component.input
            class Input:
                input_value: int

            @component.output
            class Output:
                output_value: int

            def run(self, wrong_name):
                return self.Output(output_value=1)
