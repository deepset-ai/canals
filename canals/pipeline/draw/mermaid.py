# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
import logging
import base64

import requests
import networkx

from canals.errors import PipelineDrawingError


logger = logging.getLogger(__name__)


MERMAID_STYLED_TEMPLATE = """
%%{{ init: {{'theme': 'neutral' }} }}%%

{graph_as_text}

style IN  fill:#fff,stroke:#fff,stroke-width:1px
style OUT fill:#fff,stroke:#fff,stroke-width:1px
linkStyle default stroke-width:2px,stroke-dasharray: 5 5;
{solid_arrows}
"""


def _to_mermaid_image(graph: networkx.MultiDiGraph):
    """
    Renders a pipeline using Mermaid (hosted version at 'https://mermaid.ink'). Requires Internet access.
    """
    graph_as_text = _to_mermaid_text(graph=graph)
    num_solid_arrows = len(graph.edges) - len(graph.in_edges("output")) - len(graph.out_edges("input"))
    solid_arrows = "\n".join([f"linkStyle {i} stroke-width:2px;" for i in range(num_solid_arrows)])

    graph_styled = MERMAID_STYLED_TEMPLATE.format(graph_as_text=graph_as_text, solid_arrows=solid_arrows)
    logger.debug("Mermaid diagram:\n%s", graph_styled)

    graphbytes = graph_styled.encode("ascii")
    base64_bytes = base64.b64encode(graphbytes)
    base64_string = base64_bytes.decode("ascii")
    url = "https://mermaid.ink/img/" + base64_string

    logging.debug("Rendeding graph at %s", url)
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code >= 400:
            logger.warning("Failed to draw the pipeline: https://mermaid.ink/img/ returned status %s", resp.status_code)
            logger.info("Exact URL requested: %s", url)
            logger.warning("No pipeline diagram will be saved.")
            resp.raise_for_status()

    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Failed to draw the pipeline: could not connect to https://mermaid.ink/img/ (%s)", exc)
        logger.info("Exact URL requested: %s", url)
        logger.warning("No pipeline diagram will be saved.")
        raise PipelineDrawingError(
            "There was an issue with https://mermaid.ink/, see the stacktrace for details."
        ) from exc

    return resp.content


def _to_mermaid_text(graph: networkx.MultiDiGraph) -> str:
    """
    Converts a Networkx graph into Mermaid syntax. The output of this function can be used in the documentation
    with `mermaid` codeblocks and it will be automatically rendered.
    """
    components = {
        comp: f"{comp}:::{graph.nodes[comp]['style']}" if "style" in graph.nodes[comp] else comp for comp in graph.nodes
    }

    connections_list = [
        f"{components[from_comp]} -- {conn_data['label']} --> {components[to_comp]}"
        for from_comp, to_comp, conn_data in graph.edges(data=True)
        if from_comp != "input" and to_comp != "output"
    ]
    input_connections = [
        f"IN([input]) -- {conn_data['label']} --> {components[to_comp]}"
        for _, to_comp, conn_data in graph.out_edges("input", data=True)
    ]
    output_connections = [
        f"{components[from_comp]} -- {conn_data['label']} --> OUT([output])"
        for from_comp, _, conn_data in graph.in_edges("output", data=True)
    ]
    connections = "\n".join(connections_list + input_connections + output_connections)

    mermaid_graph = f"graph TD;\n{connections}"
    return mermaid_graph
