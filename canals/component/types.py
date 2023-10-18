from typing import TypeVar, Generic


# Generic type variable used in the Variadic container
T = TypeVar("T")


class Variadic(Generic[T]):
    """
    Variadic is a generic container type we use to mark input types.
    This type doesn't do anything else than "marking" the contained
    type so it can be used in the `InputSocket` creation.
    """

    def __init__(self, type_: T) -> None:
        self.type = type_
