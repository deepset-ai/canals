# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
# pylint: disable=missing-function-docstring

from pathlib import Path
from pprint import pprint
import logging

from canals.pipeline import Pipeline
from sample_components import AddFixedValue, Subtract


logging.basicConfig(level=logging.DEBUG)


def test_pipeline(tmp_path):
    add_two = AddFixedValue(add=2)

    pipeline = Pipeline()
    pipeline.add_component("first_addition", add_two)
    pipeline.add_component("second_addition", add_two)
    pipeline.add_component("third_addition", add_two)
    pipeline.add_component("diff", Subtract())
    pipeline.add_component("fourth_addition", AddFixedValue(add=1))

    pipeline.connect("first_addition.result", "second_addition.value")
    pipeline.connect("second_addition.result", "diff.first_value")
    pipeline.connect("third_addition.result", "diff.second_value")
    pipeline.connect("diff", "fourth_addition.value")

    pipeline.draw(tmp_path / "fixed_merging_pipeline.png")

    results = pipeline.run(
        {
            "first_addition": {"value": 1},
            "third_addition": {"value": 1},
        }
    )
    pprint(results)

    assert results == {"fourth_addition": {"result": 3}}


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
