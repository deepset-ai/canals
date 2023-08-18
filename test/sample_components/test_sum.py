# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0

from sample_components import Sum


def test_to_dict():
    component = Sum(inputs=["first", "second"])
    res = component.to_dict()
    assert res == {"hash": id(component), "type": "Sum", "init_parameters": {"inputs": ["first", "second"]}}


def test_from_dict():
    data = {"hash": 1234, "type": "Sum", "init_parameters": {"inputs": ["first", "second"]}}
    component = Sum.from_dict(data)
    assert component.inputs == ["first", "second"]


def test_sum_expects_no_values_receives_no_values():
    component = Sum(inputs=[])
    results = component.run()
    assert results == {"total": 0}


def test_sum_expects_no_values_receives_one_value():
    component = Sum(inputs=[])
    assert component.run(value_1=10) == {"total": 10}


def test_sum_expects_one_value_receives_one_value():
    component = Sum(inputs=["value_1"])
    results = component.run(value_1=10)
    assert results == {"total": 10}


def test_sum_expects_one_value_receives_wrong_value():
    component = Sum(inputs=["value_1"])
    assert component.run(something_else=10) == {"total": 10}


def test_sum_expects_one_value_receives_few_values():
    component = Sum(inputs=["value_1"])
    assert component.run(value_1=10, value_2=2) == {"total": 12}


def test_sum_expects_few_values_receives_right_values():
    component = Sum(inputs=["value_1", "value_2", "value_3"])
    results = component.run(value_1=10, value_2=11, value_3=12)
    assert results == {"total": 33}


def test_sum_expects_few_values_receives_some_wrong_values():
    component = Sum(inputs=["value_1", "value_2", "value_3"])
    assert component.run(value_1=10, value_4=11, value_3=12) == {"total": 33}
