from dataclasses import dataclass
from canals import component


@component
class Sum:
    """
    Sums the values of all the input connections together.

    Multi input, single output component. Order of input connections is irrelevant.
    """

    @dataclass
    class Output:
        total: int

    def run(self, *value: int) -> Output:
        total = sum(value)
        return Sum.Output(total=total)


def test_sum_no_values():
    component = Sum()
    results = component.run()
    assert results == Sum.Output(total=0)
    assert component._init_parameters == {}


def test_sum_one_value():
    component = Sum()
    results = component.run(10)
    assert results == Sum.Output(total=10)
    assert component._init_parameters == {}


def test_sum_few_values():
    component = Sum()
    results = component.run(10, 11, 12)
    assert results == Sum.Output(total=33)
    assert component._init_parameters == {}