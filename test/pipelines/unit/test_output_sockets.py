from typing import List, Optional, Union, Set, Sequence, Iterable, Dict, Mapping, Tuple

import pytest

from canals.component import component
from canals.pipeline.sockets import (
    find_output_sockets,
    OutputSocket,
)


def test_find_output_sockets_one_regular_builtin_type_output():
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

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {"output_value": OutputSocket(name="output_value", types={int})}
    assert sockets == expected


def test_find_output_sockets_many_regular_builtin_type_outputs():
    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            int_value: int
            str_value: str
            bool_value: bool

        def run(self, data):
            return self.Output(int_value=1, str_value="1", bool_value=True)

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {
        "int_value": OutputSocket(name="int_value", types={int}),
        "str_value": OutputSocket(name="str_value", types={str}),
        "bool_value": OutputSocket(name="bool_value", types={bool}),
    }
    assert sockets == expected


def test_find_output_sockets_one_regular_object_type_output():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            output_value: MyObject

        def run(self, data):
            return self.Output(output_value=MyObject())

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {"output_value": OutputSocket(name="output_value", types={MyObject})}
    assert sockets == expected


def test_find_output_sockets_one_union_type_output():
    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            output_value: Union[str, int]

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {"output_value": OutputSocket(name="output_value", types={str, int})}
    assert sockets == expected


def test_find_output_sockets_one_optional_builtin_type_output():
    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            output_value: Optional[int] = None

        def run(self, data):
            return self.Output(output_value=1)

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {"output_value": OutputSocket(name="output_value", types={int})}
    assert sockets == expected


def test_find_output_sockets_one_optional_object_type_output():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            output_value: Optional[MyObject] = None

        def run(self, data):
            return self.Output(output_value=MyObject())

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {"output_value": OutputSocket(name="output_value", types={MyObject})}
    assert sockets == expected


def test_find_output_sockets_sequences_of_builtin_type_output():
    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            list_value: List[int]
            set_value: Set[int]
            sequence_value: Sequence[int]
            iterable_value: Iterable[int]

        def run(self, data):
            return self.Output(list_value=[], set_value=set(), sequence_value=[], iterable_value=[])

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {
        "list_value": OutputSocket(name="list_value", types={List[int]}),
        "set_value": OutputSocket(name="set_value", types={Set[int]}),
        "sequence_value": OutputSocket(name="sequence_value", types={Sequence[int]}),
        "iterable_value": OutputSocket(name="iterable_value", types={Iterable[int]}),
    }
    assert sockets == expected


def test_find_output_sockets_sequences_of_object_type_output():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            list_value: List[MyObject]
            set_value: Set[MyObject]
            sequence_value: Sequence[MyObject]
            iterable_value: Iterable[MyObject]

        def run(self, data):
            return self.Output(list_value=[], set_value=set(), sequence_value=[], iterable_value=[])

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {
        "list_value": OutputSocket(name="list_value", types={List[MyObject]}),
        "set_value": OutputSocket(name="set_value", types={Set[MyObject]}),
        "sequence_value": OutputSocket(name="sequence_value", types={Sequence[MyObject]}),
        "iterable_value": OutputSocket(name="iterable_value", types={Iterable[MyObject]}),
    }
    assert sockets == expected


def test_find_output_sockets_mappings_of_builtin_type_output():
    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            dict_value: Dict[str, int]
            mapping_value: Mapping[str, int]

        def run(self, data):
            return self.Output(dict_value={}, mapping_value={})

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {
        "dict_value": OutputSocket(name="dict_value", types={Dict[str, int]}),
        "mapping_value": OutputSocket(name="mapping_value", types={Mapping[str, int]}),
    }
    assert sockets == expected


def test_find_output_sockets_mappings_of_object_type_output():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            dict_value: Dict[str, MyObject]
            mapping_value: Mapping[str, MyObject]

        def run(self, data):
            return self.Output(dict_value={}, mapping_value={})

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {
        "dict_value": OutputSocket(name="dict_value", types={Dict[str, MyObject]}),
        "mapping_value": OutputSocket(name="mapping_value", types={Mapping[str, MyObject]}),
    }
    assert sockets == expected


def test_find_output_sockets_tuple_type_output():
    class MyObject:
        ...

    @component
    class MockComponent:
        @component.input
        class Input:
            input_value: int

        @component.output
        class Output:
            tuple_value: Tuple[str, MyObject]

        def run(self, data):
            return self.Output(tuple_value=("a", MyObject()))

    comp = MockComponent()
    sockets = find_output_sockets(comp)
    expected = {
        "tuple_value": OutputSocket(name="tuple_value", types={Tuple[str, MyObject]}),
    }
    assert sockets == expected
