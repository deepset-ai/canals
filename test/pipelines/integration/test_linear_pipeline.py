# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from pprint import pprint

from canals.pipeline import Pipeline
from test.sample_components import AddFixedValue, Double

import logging

logging.basicConfig(level=logging.DEBUG)


def test_pipeline(tmp_path):
    pipeline = Pipeline()
    afv2 = AddFixedValue(add=2)
    afv1 = AddFixedValue()

    pipeline.add_component("first_addition", afv2)
    pipeline.add_component("second_addition", afv1)
    pipeline.add_component("double", Double())
    pipeline.connect("first_addition", "double")
    pipeline.connect("double", "second_addition")

    pipeline.draw(tmp_path / "linear_pipeline.png")

    results = pipeline.run({"first_addition": afv2.input(value=1)})
    pprint(results)

    assert results == {"second_addition": afv1.output(value=7)}


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
