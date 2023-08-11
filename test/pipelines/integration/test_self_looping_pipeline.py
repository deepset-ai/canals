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
class SelfLoopingComponent:
    @component.output_types(goal=int, current=int, final_result=int)
    def run(self, initial_goal: Optional[int] = None, goal: Optional[int] = None, current: Optional[int] = None):
        if initial_goal:
            goal = initial_goal - 1  # For some reason that's a better goal
        if goal == current:
            return {"goal": None, "current": None, "final_result": current}
        return {"goal": goal, "current": current, "final_result": None}


@component
class Worker:
    @component.output_types(output=int)
    def run(self, input: int):
        return {"output": input + 1}


def test_pipeline(tmp_path):
    pipeline = Pipeline()
    pipeline.add_component("self_loop", SelfLoopingComponent())
    pipeline.add_component("worker", Worker())
    pipeline.connect("self_loop.current", "worker.input")
    pipeline.connect("worker.output", "self_loop.current")
    pipeline.connect("self_loop.goal", "self_loop.goal")

    pipeline.draw(tmp_path / "self_looping_pipeline.png")

    results = pipeline.run({"self_loop": {"initial_goal": 5, "current": 0}})
    pprint(results)

    assert results["self_loop"]["final_result"] == 5


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
