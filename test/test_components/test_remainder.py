from dataclasses import make_dataclass

from canals import component


@component
class Remainder:
    """
    Redirects the value, unchanged, along the connection corresponding to the remainder
    of a division. For example, if `divisor=3`, the value `5` would be sent along
    the second output connection.
    """

    def __init__(self, divisor: int = 2):
        self.divisor = divisor
        self._output_type = make_dataclass("Output", [(f"remainder_is_{val}", int, None) for val in range(divisor)])

    @property
    def output_type(self):
        return self._output_type

    def run(self, value: int):
        """
        :param value: the value to check the remainder of.
        """
        remainder = value % self.divisor
        output = self.output_type()
        setattr(output, f"remainder_is_{remainder}", value)
        return output


def test_remainder():
    component = Remainder()
    results = component.run(value=3)
    assert results == component.output_type(remainder_is_1=3)

    component = Remainder(divisor=4)
    results = component.run(value=3)
    assert results == component.output_type(remainder_is_3=3)