# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from pathlib import Path
from pprint import pprint

from canals import component
from canals.pipeline import Pipeline

import logging

logging.basicConfig(level=logging.DEBUG)


@component
class A:
    @component.output_types(goal=int, current=int, final_result=int)
    def run(self, initial_goal: Optional[int] = None, goal: Optional[int] = None, current: Optional[int] = None):
        if initial_goal:
            goal = initial_goal - 1  # For some reason that's a better goal
        if goal == current:
            return {"goal": None, "current": None, "final_result": current}
        return {"goal": goal, "current": current, "final_result": None}


@component
class B:
    @component.output_types(y=int)
    def run(self, x: int):
        return {"y": x + 1}


def test_pipeline(tmp_path):
    pipeline = Pipeline()
    pipeline.add_component("a", A())
    pipeline.add_component("b", B())
    pipeline.connect("a.current", "b.x")
    pipeline.connect("b.y", "a.current")
    pipeline.connect("a.goal", "a.goal")

    pipeline.draw(tmp_path / "self_looping_pipeline.png")

    results = pipeline.run({"a": {"initial_goal": 5, "current": 0}})
    pprint(results)


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
