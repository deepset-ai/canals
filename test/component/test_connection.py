from canals.component.connection import Connection
from canals.component.sockets import InputSocket, OutputSocket

import pytest


@pytest.mark.parametrize(
    "c,expected",
    [
        (
            Connection("source_component", OutputSocket("out", int), "destination_component", InputSocket("in", int)),
            "source_component.out (int) --> (int) destination_component.in",
        ),
        (
            Connection(None, None, "destination_component", InputSocket("in", int)),
            "input needed --> (int) destination_component.in",
        ),
        (Connection("source_component", OutputSocket("out", int), None, None), "source_component.out (int) --> output"),
        (Connection(None, None, None, None), "input needed --> output"),
    ],
)
def test_repr(c, expected):
    assert str(c) == expected


def test_is_mandatory():
    c = Connection(None, None, "destination_component", InputSocket("in", int))
    assert c.is_mandatory

    c = Connection(None, None, "destination_component", InputSocket("in", int, is_mandatory=False))
    assert not c.is_mandatory

    c = Connection("source_component", OutputSocket("out", int), None, None)
    assert not c.is_mandatory
