from typing import Optional

from canals.component import InputSocket
from canals.component.types import IsOptional


def test_is_not_optional():
    s = InputSocket("test_name", int)
    assert s.is_optional is False


def test_is_optional_python_type():
    s = InputSocket("test_name", Optional[int])
    assert not s.is_optional


def test_is_optional_canals_type():
    s = InputSocket("test_name", IsOptional[int])
    assert s.is_optional
