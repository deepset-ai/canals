# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from pprint import pprint

from canals.pipeline import Pipeline
from sample_components import AddFixedValue, Joiner

import logging

logging.basicConfig(level=logging.DEBUG)


def test_joiner(tmp_path):
    pipeline = Pipeline()
    pipeline.add_component("inc", AddFixedValue(add=1))
    pipeline.add_component("dec", AddFixedValue(add=-1))
    pipeline.add_component("joiner", Joiner(int))
    pipeline.connect("inc", "joiner")
    pipeline.connect("dec", "joiner")
    pipeline.draw(tmp_path / "joiner_pipeline.png")

    results = pipeline.run({"inc": {"value": 4}, "dec": {"value": 11}})
    assert results == {"joiner": {"output": [5, 10]}}


if __name__ == "__main__":
    test_joiner(Path(__file__).parent)
