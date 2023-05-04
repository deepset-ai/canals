from dataclasses import dataclass

from canals import name


@name
class Threshold:
    """
    Redirects the value, unchanged, along a different connection whether the value is above
    or below the given threshold.

    Single input, double output decision component.

    :param threshold: the number to compare the input value against. This is also a parameter.
    """

    @dataclass
    class Output:
        above: int
        below: int

    def __init__(self, threshold: int = 10):
        """
        :param threshold: the number to compare the input value against.
        """
        self.defaults = {"threshold": threshold}

    def run(self, value: int, threshold: int) -> Output:
        if value < threshold:
            return Threshold.Output(above=None, below=value)  # type: ignore
        return Threshold.Output(above=value, below=None)  # type: ignore


def test_threshold():
    component = Threshold()

    results = component.run(value=5, threshold=10)
    assert results == Threshold.Output(above=None, below=5)

    results = component.run(value=15, threshold=10)
    assert results == Threshold.Output(above=15, below=None)