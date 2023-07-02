import typing
from typing import List, Optional, Union, Set, Sequence, Iterable, Dict, Mapping, Tuple

from dataclasses import dataclass

import pytest

from canals.component import component
from canals.pipeline.sockets import (
    find_input_sockets,
    InputSocket,
)


def test_find_input_sockets_one_regular_builtin_type_input():
    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=data.input_value)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={int})}
    assert sockets == expected


def test_find_input_sockets_many_regular_builtin_type_inputs():
    @component
    class MockComponent:
        @component.input
        class Input:
            int_value: int
            str_value: str
            bool_value: bool

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=data.int_value)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "int_value": InputSocket(name="int_value", types={int}),
        "str_value": InputSocket(name="str_value", types={str}),
        "bool_value": InputSocket(name="bool_value", types={bool}),
    }
    assert sockets == expected


def test_find_input_sockets_one_regular_object_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: MyObject

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={MyObject})}
    assert sockets == expected


def test_find_input_sockets_one_union_type_input():
    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: Union[str, int]

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={str, int})}
    assert sockets == expected


def test_find_input_sockets_one_optional_builtin_type_input():
    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: Optional[int] = None

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={int})}
    assert sockets == expected


def test_find_input_sockets_one_optional_object_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: Optional[MyObject] = None

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {"input_value": InputSocket(name="input_value", types={MyObject})}
    assert sockets == expected


def test_find_input_sockets_sequences_of_builtin_type_input():
    @component
    class MockComponent:
        @component.input
        class Input:
            list_value: List[int]
            set_value: Set[int]
            sequence_value: Sequence[int]
            iterable_value: Iterable[int]

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "list_value": InputSocket(name="list_value", types={typing.List[int]}),
        "set_value": InputSocket(name="set_value", types={typing.Set[int]}),
        "sequence_value": InputSocket(name="sequence_value", types={typing.Sequence[int]}),
        "iterable_value": InputSocket(name="iterable_value", types={typing.Iterable[int]}),
    }
    assert sockets == expected


def test_find_input_sockets_sequences_of_object_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            list_value: List[MyObject]
            set_value: Set[MyObject]
            sequence_value: Sequence[MyObject]
            iterable_value: Iterable[MyObject]

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "list_value": InputSocket(name="list_value", types={typing.List[MyObject]}),
        "set_value": InputSocket(name="set_value", types={typing.Set[MyObject]}),
        "sequence_value": InputSocket(name="sequence_value", types={typing.Sequence[MyObject]}),
        "iterable_value": InputSocket(name="iterable_value", types={typing.Iterable[MyObject]}),
    }
    assert sockets == expected


def test_find_input_sockets_mappings_of_builtin_type_input():
    @component
    class MockComponent:
        @component.input
        class Input:
            dict_value: Dict[str, int]
            mapping_value: Mapping[str, int]

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "dict_value": InputSocket(name="dict_value", types={typing.Dict[str, int]}),
        "mapping_value": InputSocket(name="mapping_value", types={typing.Mapping[str, int]}),
    }
    assert sockets == expected


def test_find_input_sockets_mappings_of_object_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            dict_value: Dict[str, MyObject]
            mapping_value: Mapping[str, MyObject]

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "dict_value": InputSocket(name="dict_value", types={typing.Dict[str, MyObject]}),
        "mapping_value": InputSocket(name="mapping_value", types={typing.Mapping[str, MyObject]}),
    }
    assert sockets == expected


def test_find_input_sockets_tuple_type_input():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            tuple_value: Tuple[str, MyObject]

        @component.output
        class Output:
            output_value: int

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_input_sockets(comp)
    expected = {
        "tuple_value": InputSocket(name="tuple_value", types={typing.Tuple[str, MyObject]}),
    }
    assert sockets == expected
