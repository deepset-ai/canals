# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from pprint import pprint

from canals.pipeline import Pipeline
from sample_components import StringJoiner, Hello

import logging

logging.basicConfig(level=logging.DEBUG)


def test_joiner(tmp_path):
    pipeline = Pipeline()
    pipeline.add_component("hello_one", Hello())
    pipeline.add_component("hello_two", Hello())
    pipeline.add_component("joiner", StringJoiner())
    pipeline.connect("hello_one", "joiner")
    pipeline.connect("hello_two", "joiner")
    pipeline.draw(tmp_path / "joiner_pipeline.png")

    results = pipeline.run({"hello_one": {"word": "world"}, "hello_two": {"word": "my friend"}})
    assert results == {"joiner": {"output": "Hello, world! Hello, my friend!"}}


if __name__ == "__main__":
    test_joiner(Path(__file__).parent)
