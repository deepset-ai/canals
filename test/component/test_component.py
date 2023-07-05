import pytest

from canals.component import component, Input, Output
from canals.errors import ComponentError


def test_correct_declaration():
    @component
    class MockComponent:
        def __init__(self):
            self.input = Input()
            self.output = Output()

        def run(self, data):
            return self.output()

    # Verifies also instantiation works with no issues
    assert MockComponent()
    assert component.registry["MockComponent"] == MockComponent


def test_input_required():
    @component
    class MockComponent:
        def __init__(self):
            self.output = Output()

        def run(self, data):
            return self.output()

    with pytest.raises(
        ComponentError,
        match="Component must declare their input",
    ):
        MockComponent()


def test_output_required():
    @component
    class MockComponent:
        def __init__(self):
            self.input = Input()

        def run(self, data):
            return 1

    with pytest.raises(
        ComponentError,
        match="Component must declare their output",
    ):
        MockComponent()


def test_check_for_run():
    with pytest.raises(ComponentError, match="must have a 'run\(\)' method"):

        @component
        class MockComponent:
            def __init__(self):
                self.input = Input()
                self.output = Output()


def test_run_takes_only_one_param():
    with pytest.raises(ComponentError, match="must accept only a single parameter called 'data'."):

        @component
        class MockComponent:
            def __init__(self):
                self.input = Input()
                self.output = Output()

            def run(self, data, something_else: int):
                return self.output(output_value=1)


def test_run_takes_only_data():
    with pytest.raises(ComponentError, match="must accept a parameter called 'data'."):

        @component
        class MockComponent:
            def __init__(self):
                self.input = Input()
                self.output = Output()

            def run(self, wrong_name):
                return self.output()
