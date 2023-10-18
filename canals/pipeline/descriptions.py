# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import List, Dict
import logging

import networkx

from canals.utils import _type_name
from canals.component.sockets import InputSocket, OutputSocket


logger = logging.getLogger(__name__)


def _find_pipeline_inputs(graph: networkx.MultiDiGraph) -> Dict[str, List[InputSocket]]:
    """
    Collect components that have disconnected input sockets. Note that this method returns *ALL* disconnected
    input sockets, including all such sockets with default values.
    """
    return {
        name: [socket for socket in data.get("input_sockets", {}).values() if not socket.sender]
        for name, data in graph.nodes(data=True)
    }


def _find_pipeline_outputs(graph) -> Dict[str, List[OutputSocket]]:
    """
    Collect components that have disconnected output sockets. They define the pipeline output.
    """
    return {
        node: list(data.get("output_sockets", {}).values())
        for node, data in graph.nodes(data=True)
        if not graph.out_edges(node)
    }


def _describe_pipeline_inputs(graph: networkx.MultiDiGraph):
    """
    Returns a dictionary with the input names and types that this pipeline accepts.
    """
    inputs = {
        comp: {socket.name: {"type": socket.type, "is_optional": socket.is_optional} for socket in data}
        for comp, data in _find_pipeline_inputs(graph).items()
        if data
    }
    return inputs


def _describe_pipeline_inputs_as_string(graph: networkx.MultiDiGraph):
    """
    Returns a string representation of the input names and types that this pipeline accepts.
    """
    inputs = _describe_pipeline_inputs(graph)
    message = "This pipeline expects the following inputs:\n"
    for comp, sockets in inputs.items():
        if sockets:
            message += f"- {comp}:\n"
            for name, socket in sockets.items():
                message += f"    - {name}: {_type_name(socket['type'])}\n"
    return message