# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from pprint import pprint

from canals.pipeline import Pipeline
from sample_components import AddFixedValue, Double

import logging

logging.basicConfig(level=logging.DEBUG)


def test_pipeline(tmp_path):
    pipeline = Pipeline()
    pipeline.add_component("inc", AddFixedValue(add=1))
    pipeline.add_component("dec", AddFixedValue(add=-1))
    pipeline.add_component("double", Double())

    pipeline.connect("inc", "double")
    pipeline.connect("dec", "double")

    pipeline.draw(tmp_path / "linear_pipeline.png")

    results = pipeline.run({"inc": {"value": 4}, "dec": {"value": 11}})
    pprint(results)

    assert results == {"double": {"value": 30}}


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
