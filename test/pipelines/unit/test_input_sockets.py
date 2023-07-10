import typing
from typing import List, Optional, Union, Set, Sequence, Iterable, Dict, Mapping, Tuple

from dataclasses import dataclass, field, Field

import pytest

from canals.component import component, Input, Output
from canals.pipeline.sockets import (
    find_input_sockets,
    InputSocket,
)


def test_find_input_sockets_one_regular_builtin_type_input():
    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(input_value=int)
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=data.input_value)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={int}, default=None)}
    assert sockets == expected


def test_find_input_sockets_many_regular_builtin_type_inputs():
    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(int_value=int, str_value=str, bool_value=bool)
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=data.int_value)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "int_value": InputSocket(name="int_value", types={int}, default=None),
        "str_value": InputSocket(name="str_value", types={str}, default=None),
        "bool_value": InputSocket(name="bool_value", types={bool}, default=None),
    }
    assert sockets == expected


def test_find_input_sockets_one_regular_object_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(input_value=MyObject)
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={MyObject}, default=None)}
    assert sockets == expected


def test_find_input_sockets_one_union_type_input():
    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(input_value=Union[str, int])
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={str, int}, default=None)}
    assert sockets == expected


def test_find_input_sockets_one_optional_builtin_type_input():
    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(input_value=Optional[int])
            self.input.set_defaults(input_value=None)
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={int}, default=None)}
    assert sockets == expected


def test_find_input_sockets_one_optional_object_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(input_value=Optional[MyObject])
            self.input.set_defaults(input_value=None)
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={MyObject}, default=None)}
    assert sockets == expected


def test_find_input_sockets_sequences_of_builtin_type_input():
    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(
                list_value=List[int], set_value=Set[int], sequence_value=Sequence[int], iterable_value=Iterable[int]
            )
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "list_value": InputSocket(name="list_value", types={typing.List[int]}, default=None),
        "set_value": InputSocket(name="set_value", types={typing.Set[int]}, default=None),
        "sequence_value": InputSocket(name="sequence_value", types={typing.Sequence[int]}, default=None),
        "iterable_value": InputSocket(name="iterable_value", types={typing.Iterable[int]}, default=None),
    }
    assert sockets == expected


def test_find_input_sockets_sequences_of_object_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(
                list_value=List[MyObject],
                set_value=Set[MyObject],
                sequence_value=Sequence[MyObject],
                iterable_value=Iterable[MyObject],
            )
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "list_value": InputSocket(name="list_value", types={typing.List[MyObject]}, default=None),
        "set_value": InputSocket(name="set_value", types={typing.Set[MyObject]}, default=None),
        "sequence_value": InputSocket(name="sequence_value", types={typing.Sequence[MyObject]}, default=None),
        "iterable_value": InputSocket(name="iterable_value", types={typing.Iterable[MyObject]}, default=None),
    }
    assert sockets == expected


def test_find_input_sockets_mappings_of_builtin_type_input():
    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(dict_value=Dict[str, int], mapping_value=Mapping[str, int])
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "dict_value": InputSocket(name="dict_value", types={typing.Dict[str, int]}, default=None),
        "mapping_value": InputSocket(name="mapping_value", types={typing.Mapping[str, int]}, default=None),
    }
    assert sockets == expected


def test_find_input_sockets_mappings_of_object_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(dict_value=Dict[str, MyObject], mapping_value=Mapping[str, MyObject])
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "dict_value": InputSocket(name="dict_value", types={typing.Dict[str, MyObject]}, default=None),
        "mapping_value": InputSocket(name="mapping_value", types={typing.Mapping[str, MyObject]}, default=None),
    }
    assert sockets == expected


def test_find_input_sockets_tuple_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(tuple_value=Tuple[str, MyObject])
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "tuple_value": InputSocket(name="tuple_value", types={typing.Tuple[str, MyObject]}, default=None),
    }
    assert sockets == expected


def test_find_input_sockets_with_default_factory():
    class MyObject:
        ...

    @component
    class MockComponent:
        def __init__(self):
            self.input = Input(input_value=Dict[str, int])
            self.input.set_defaults(input_value=field(default_factory=dict))
            self.output = Output(output_value=int)

        def run(self, data):
            return self.output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "input_value": InputSocket(name="input_value", types={typing.Dict[str, int]}, default={}),
    }
    assert sockets == expected
