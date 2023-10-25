# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Any, Dict, List, Union, TypeVar, Type

import os
import json
import datetime
import logging
from pathlib import Path
from copy import deepcopy
from collections import OrderedDict

import networkx

from canals.component import component, Component, InputSocket, OutputSocket
from canals.errors import (
    PipelineError,
    PipelineConnectError,
    PipelineMaxLoops,
    PipelineRuntimeError,
    PipelineValidationError,
)
from canals.pipeline.draw import _draw, _convert_for_debug, RenderingEngines
from canals.pipeline.validation import validate_pipeline_input
from canals.pipeline.connections import parse_connection, _find_unambiguous_connection
from canals.type_utils import _type_name
from canals.serialization import component_to_dict, component_from_dict

logger = logging.getLogger(__name__)

# We use a generic type to annotate the return value of classmethods,
# so that static analyzers won't be confused when derived classes
# use those methods.
T = TypeVar("T", bound="Pipeline")


class Pipeline:
    """
    Components orchestration engine.

    Builds a graph of components and orchestrates their execution according to the execution graph.
    """

    def __init__(
        self,
        metadata: Optional[Dict[str, Any]] = None,
        max_loops_allowed: int = 100,
        debug_path: Union[Path, str] = Path(".canals_debug/"),
    ):
        """
        Creates the Pipeline.

        Args:
            metadata: arbitrary dictionary to store metadata about this pipeline. Make sure all the values contained in
                this dictionary can be serialized and deserialized if you wish to save this pipeline to file with
                `save_pipelines()/load_pipelines()`.
            max_loops_allowed: how many times the pipeline can run the same node before throwing an exception.
            debug_path: when debug is enabled in `run()`, where to save the debug data.
        """
        self.metadata = metadata or {}
        self.max_loops_allowed = max_loops_allowed
        self.graph = networkx.MultiDiGraph()
        self.debug: Dict[int, Dict[str, Any]] = {}
        self.debug_path = Path(debug_path)

    def __eq__(self, other) -> bool:
        """
        Equal pipelines share every metadata, node and edge, but they're not required to use
        the same node instances: this allows pipeline saved and then loaded back to be equal to themselves.
        """
        if (
            not isinstance(other, type(self))
            or not getattr(self, "metadata") == getattr(other, "metadata")
            or not getattr(self, "max_loops_allowed") == getattr(other, "max_loops_allowed")
            or not hasattr(self, "graph")
            or not hasattr(other, "graph")
        ):
            return False

        return (
            self.graph.adj == other.graph.adj
            and self._comparable_nodes_list(self.graph) == self._comparable_nodes_list(other.graph)
            and self.graph.graph == other.graph.graph
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Returns this Pipeline instance as a dictionary.
        This is meant to be an intermediate representation but it can be also used to save a pipeline to file.
        """
        components = {}
        for name, instance in self.graph.nodes(data="instance"):
            components[name] = component_to_dict(instance)

        connections = []
        for sender, receiver, edge_data in self.graph.edges.data():
            sender_socket = edge_data["from_socket"].name
            receiver_socket = edge_data["to_socket"].name
            connections.append({"sender": f"{sender}.{sender_socket}", "receiver": f"{receiver}.{receiver_socket}"})
        return {
            "metadata": self.metadata,
            "max_loops_allowed": self.max_loops_allowed,
            "components": components,
            "connections": connections,
        }

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any], **kwargs) -> T:
        """
        Creates a Pipeline instance from a dictionary.
        A sample `data` dictionary could be formatted like so:
        ```
        {
            "metadata": {"test": "test"},
            "max_loops_allowed": 100,
            "components": {
                "add_two": {
                    "type": "AddFixedValue",
                    "init_parameters": {"add": 2},
                },
                "add_default": {
                    "type": "AddFixedValue",
                    "init_parameters": {"add": 1},
                },
                "double": {
                    "type": "Double",
                },
            },
            "connections": [
                {"sender": "add_two.result", "receiver": "double.value"},
                {"sender": "double.value", "receiver": "add_default.value"},
            ],
        }
        ```

        Supported kwargs:
        `components`: a dictionary of {name: instance} to reuse instances of components instead of creating new ones.
        """
        metadata = data.get("metadata", {})
        max_loops_allowed = data.get("max_loops_allowed", 100)
        debug_path = Path(data.get("debug_path", ".canals_debug/"))
        pipe = cls(metadata=metadata, max_loops_allowed=max_loops_allowed, debug_path=debug_path)
        components_to_reuse = kwargs.get("components", {})
        for name, component_data in data.get("components", {}).items():
            if name in components_to_reuse:
                # Reuse an instance
                instance = components_to_reuse[name]
            else:
                if "type" not in component_data:
                    raise PipelineError(f"Missing 'type' in component '{name}'")
                if component_data["type"] not in component.registry:
                    raise PipelineError(f"Component '{component_data['type']}' not imported.")
                # Create a new one
                component_class = component.registry[component_data["type"]]
                instance = component_from_dict(component_class, component_data)
            pipe.add_component(name=name, instance=instance)

        for connection in data.get("connections", []):
            if "sender" not in connection:
                raise PipelineError(f"Missing sender in connection: {connection}")
            if "receiver" not in connection:
                raise PipelineError(f"Missing receiver in connection: {connection}")
            pipe.connect(connect_from=connection["sender"], connect_to=connection["receiver"])

        return pipe

    def _comparable_nodes_list(self, graph: networkx.MultiDiGraph) -> List[Dict[str, Any]]:
        """
        Replaces instances of nodes with their class name in order to make sure they're comparable.
        """
        nodes = []
        for node in graph.nodes:
            comparable_node = graph.nodes[node]
            comparable_node["instance"] = comparable_node["instance"].__class__
            nodes.append(comparable_node)
        nodes.sort()
        return nodes

    def add_component(self, name: str, instance: Component) -> None:
        """
        Create a component for the given component. Components are not connected to anything by default:
        use `Pipeline.connect()` to connect components together.

        Component names must be unique, but component instances can be reused if needed.

        Args:
            name: the name of the component.
            instance: the component instance.

        Returns:
            None

        Raises:
            ValueError: if a component with the same name already exists
            PipelineValidationError: if the given instance is not a Canals component
        """
        # Component names are unique
        if name in self.graph.nodes:
            raise ValueError(f"A component named '{name}' already exists in this pipeline: choose another name.")

        # Components can't be named `_debug`
        if name == "_debug":
            raise ValueError("'_debug' is a reserved name for debug output. Choose another name.")

        # Component instances must be components
        if not isinstance(instance, Component):
            raise PipelineValidationError(
                f"'{type(instance)}' doesn't seem to be a component. Is this class decorated with @component?"
            )

        # Create the component's input and output sockets
        input_sockets = getattr(instance, "__canals_input__", {})
        output_sockets = getattr(instance, "__canals_output__", {})

        # Add component to the graph, disconnected
        logger.debug("Adding component '%s' (%s)", name, instance)
        self.graph.add_node(
            name, instance=instance, input_sockets=input_sockets, output_sockets=output_sockets, visits=0
        )

    def connect(self, connect_from: str, connect_to: str) -> None:
        """
        Connects two components together. All components to connect must exist in the pipeline.
        If connecting to an component that has several output connections, specify the inputs and output names as
        'component_name.connections_name'.

        Args:
            connect_from: the component that delivers the value. This can be either just a component name or can be
                in the format `component_name.connection_name` if the component has multiple outputs.
            connect_to: the component that receives the value. This can be either just a component name or can be
                in the format `component_name.connection_name` if the component has multiple inputs.

        Returns:
            None

        Raises:
            PipelineConnectError: if the two components cannot be connected (for example if one of the components is
                not present in the pipeline, or the connections don't match by type, and so on).
        """
        # Edges may be named explicitly by passing 'node_name.edge_name' to connect().
        from_node, from_socket_name = parse_connection(connect_from)
        to_node, to_socket_name = parse_connection(connect_to)

        # Get the nodes data.
        try:
            from_sockets = self.graph.nodes[from_node]["output_sockets"]
        except KeyError as exc:
            raise ValueError(f"Component named {from_node} not found in the pipeline.") from exc

        try:
            to_sockets = self.graph.nodes[to_node]["input_sockets"]
        except KeyError as exc:
            raise ValueError(f"Component named {to_node} not found in the pipeline.") from exc

        # If the name of either socket is given, get the socket
        if from_socket_name:
            from_socket = from_sockets.get(from_socket_name, None)
            if not from_socket:
                raise PipelineConnectError(
                    f"'{from_node}.{from_socket_name} does not exist. "
                    f"Output connections of {from_node} are: "
                    + ", ".join([f"{name} (type {_type_name(socket.type)})" for name, socket in from_sockets.items()])
                )
        if to_socket_name:
            to_socket = to_sockets.get(to_socket_name, None)
            if not to_socket:
                raise PipelineConnectError(
                    f"'{to_node}.{to_socket_name} does not exist. "
                    f"Input connections of {to_node} are: "
                    + ", ".join([f"{name} (type {_type_name(socket.type)})" for name, socket in to_sockets.items()])
                )

        # Look for an unambiguous connection among the possible ones.
        # Note that if there is more than one possible connection but two sockets match by name, they're paired.
        from_sockets = [from_socket] if from_socket_name else list(from_sockets.values())
        to_sockets = [to_socket] if to_socket_name else list(to_sockets.values())
        from_socket, to_socket = _find_unambiguous_connection(
            sender_node=from_node, sender_sockets=from_sockets, receiver_node=to_node, receiver_sockets=to_sockets
        )

        # Connect the components on these sockets
        self._direct_connect(from_node=from_node, from_socket=from_socket, to_node=to_node, to_socket=to_socket)

    def _direct_connect(self, from_node: str, from_socket: OutputSocket, to_node: str, to_socket: InputSocket) -> None:
        """
        Directly connect socket to socket. This method does not type-check the connections: use 'Pipeline.connect()'
        instead (which uses 'find_unambiguous_connection()' to validate types).
        """
        # Make sure the receiving socket isn't already connected, unless it's variadic. Sending sockets can be
        # connected as many times as needed, so they don't need this check
        if to_socket.sender and not to_socket.is_variadic:
            raise PipelineConnectError(
                f"Cannot connect '{from_node}.{from_socket.name}' with '{to_node}.{to_socket.name}': "
                f"{to_node}.{to_socket.name} is already connected to {to_socket.sender}.\n"
            )

        # Create the connection
        logger.debug("Connecting '%s.%s' to '%s.%s'", from_node, from_socket.name, to_node, to_socket.name)
        edge_key = f"{from_socket.name}/{to_socket.name}"
        self.graph.add_edge(
            from_node,
            to_node,
            key=edge_key,
            conn_type=_type_name(from_socket.type),
            from_socket=from_socket,
            to_socket=to_socket,
        )

        # Stores the name of the nodes that will send its output to this socket
        to_socket.sender.append(from_node)

    def get_component(self, name: str) -> Component:
        """
        Returns an instance of a component.

        Args:
            name: the name of the component

        Returns:
            The instance of that component.

        Raises:
            ValueError: if a component with that name is not present in the pipeline.
        """
        try:
            return self.graph.nodes[name]["instance"]
        except KeyError as exc:
            raise ValueError(f"Component named {name} not found in the pipeline.") from exc

    def draw(self, path: Path, engine: RenderingEngines = "mermaid-image") -> None:
        """
        Draws the pipeline. Requires either `graphviz` as a system dependency, or an internet connection for Mermaid.
        Run `pip install canals[graphviz]` or `pip install canals[mermaid]` to install missing dependencies.

        Args:
            path: where to save the diagram.
            engine: which format to save the graph as. Accepts 'graphviz', 'mermaid-text', 'mermaid-image'.
                Default is 'mermaid-image'.

        Returns:
            None

        Raises:
            ImportError: if `engine='graphviz'` and `pygraphviz` is not installed.
            HTTPConnectionError: (and similar) if the internet connection is down or other connection issues.
        """
        _draw(graph=networkx.MultiDiGraph(self.graph), path=path, engine=engine)

    def warm_up(self):
        """
        Make sure all nodes are warm.

        It's the node's responsibility to make sure this method can be called at every `Pipeline.run()`
        without re-initializing everything.
        """
        for node in self.graph.nodes:
            if hasattr(self.graph.nodes[node]["instance"], "warm_up"):
                logger.info("Warming up component %s...", node)
                self.graph.nodes[node]["instance"].warm_up()

    def run(self, data: Dict[str, Any], debug: bool = False) -> Dict[str, Any]:
        """
        Runs the pipeline.

        Args:
            data: the inputs to give to the input components of the Pipeline.
            parameters: a dictionary with all the parameters of all the components, namespaced by component.
            debug: whether to collect and return debug information.

        Returns:
            A dictionary with the outputs of the output components of the Pipeline.

        Raises:
            PipelineRuntimeError: if the any of the components fail or return unexpected output.
        """
        # **** The Pipeline.run() algorithm ****
        #
        # Nodes are run as soon as an input for them appears in the inputs buffer.
        # When there's more than a node at once in the buffer (which means some
        # branches are running in parallel or that there are loops) they are selected to
        # run in FIFO order by the `inputs_buffer` OrderedDict.
        #
        # Inputs are labeled with the name of the node they're aimed for:
        #
        #   ````
        #   inputs_buffer[target_node] = {"input_name": input_value, ...}
        #   ```
        #
        # Nodes should wait until all the necessary input data has arrived before running.
        # If they're popped from the input_buffer before they're ready, they're put back in.
        # If the pipeline has branches of different lengths, it's possible that a node has to
        # wait a bit and let other nodes pass before receiving all the input data it needs.
        #
        # Chetsheet for networkx data access:
        # - Name of the node       # self.graph.nodes  (List[str])
        # - Node instance          # self.graph.nodes[node]["instance"]
        # - Input nodes            # [e[0] for e in self.graph.in_edges(node)]
        # - Output nodes           # [e[1] for e in self.graph.out_edges(node)]
        # - Output edges           # [e[2]["label"] for e in self.graph.out_edges(node, data=True)]
        #
        # if debug:
        #     os.makedirs("debug", exist_ok=True)

        data = validate_pipeline_input(self.graph, input_values=data)
        logger.info("Pipeline execution started.")

        # Prepare the inputs buffer
        inputs_buffer: Dict[str, Dict[str, Any]] = {}
        for node_name, input_data in data.items():
            for socket_name, value in input_data.items():
                if value is None:
                    continue

                socket = self.graph.nodes[node_name]["input_sockets"][socket_name]
                if not node_name in inputs_buffer:
                    inputs_buffer[node_name] = {"mandatory": {}, "optional": {}}

                if socket.is_variadic:
                    value = [value]
                if socket.is_mandatory:
                    inputs_buffer[node_name]["mandatory"][socket_name] = value
                else:
                    inputs_buffer[node_name]["optional"][socket_name] = value

        # All components that received non-optional inputs are placed in the queue
        components_queue = [name for name, inputs in inputs_buffer.items() if inputs["mandatory"]]

        pipeline_output: Dict[str, Dict[str, Any]] = {}
        self._clear_visits_count()
        self.warm_up()

        if debug:
            logger.info("Debug mode ON.")
        self.debug = {}

        # *** PIPELINE EXECUTION LOOP ***
        # We select the nodes to run by popping them in FIFO order from the inputs buffer.
        step = 0
        while components_queue:
            step += 1
            if step > 15:
                break

            if debug:
                self._record_pipeline_step(step, inputs_buffer, pipeline_output)
            logger.debug(
                "> Queue at step %s: %s %s",
                step,
                components_queue,
                {k: [list(v["mandatory"].keys()), list(v["optional"].keys())] for k, v in inputs_buffer.items()},
            )

            component_name = components_queue.pop(0)
            inputs = inputs_buffer.pop(component_name, {})

            # Make sure it didn't run too many times already
            self._check_max_loops(component_name)

            # **** IS IT MY TURN YET? ****
            # Check if the component should be run or not
            received_input_sockets = set(inputs["mandatory"].keys())
            mandatory_input_sockets = {
                socket_name
                for socket_name, socket in self.graph.nodes[component_name]["input_sockets"].items()
                if socket.is_mandatory
            }
            if mandatory_input_sockets.issubset(received_input_sockets):
                logger.debug("Component '%s' is ready to run. All mandatory inputs received.", component_name)

            else:
                logger.debug("Component '%s' is not ready to run, some inputs are still missing.", component_name)
                if not inputs_buffer:
                    # What if there are no components to wait for?
                    raise PipelineRuntimeError(
                        f"'{component_name}' is stuck waiting for input, but there are no other components to run. "
                        "This is likely a Canals bug. Open an issue at https://github.com/deepset-ai/canals."
                    )
                components_queue.append(component_name)
                inputs_buffer[component_name] = inputs
                continue

            # **** RUN THE NODE ****
            # It is our turn! The node is ready to run and all necessary inputs are present
            inputs = {**inputs["mandatory"], **inputs["optional"]}
            output = self._run_component(name=component_name, inputs=inputs)

            # **** PROCESS THE OUTPUT ****
            # The node run successfully. Let's store or distribute the output it produced, if it's valid.
            if not self.graph.out_edges(component_name):
                # Note: if a node outputs many times (like in loops), the output will be overwritten
                pipeline_output[component_name] = output
            else:
                # inputs_buffer = self._route_output(
                #     node_results=output, node_name=component_name, inputs_buffer=inputs_buffer
                # )
                for edge_data in self.graph.out_edges(component_name, data=True):
                    to_socket = edge_data[2]["to_socket"]
                    from_socket = edge_data[2]["from_socket"]
                    target_node = edge_data[1]

                    # Skip Edges that did not receive any input.
                    value_to_route = output.get(from_socket.name)
                    if value_to_route is None:
                        continue

                    # Create the buffer for the downstream node if it's not yet there.
                    if target_node not in inputs_buffer:
                        inputs_buffer[target_node] = {"mandatory": {}, "optional": {}}

                    subset = "mandatory" if to_socket.is_mandatory else "optional"
                    # If the socket was marked as variadic, pile up inputs in a list
                    if to_socket.is_variadic:
                        inputs_buffer[target_node][subset].setdefault(to_socket.name, []).append(value_to_route)
                    # Non-variadic input: just store the value
                    else:
                        inputs_buffer[target_node][subset][to_socket.name] = value_to_route

                    if to_socket.is_mandatory and target_node not in components_queue:
                        components_queue.append(target_node)

        if debug:
            self._record_pipeline_step(step + 1, inputs_buffer, pipeline_output)

            # Save to json
            os.makedirs(self.debug_path, exist_ok=True)
            with open(self.debug_path / "data.json", "w", encoding="utf-8") as datafile:
                json.dump(self.debug, datafile, indent=4, default=str)

            # Store in the output
            pipeline_output["_debug"] = self.debug  # type: ignore

        logger.info("Pipeline executed successfully.")
        return pipeline_output

    def _record_pipeline_step(self, step, inputs_buffer, pipeline_output):
        """
        Stores a snapshot of this step into the self.debug dictionary of the pipeline.
        """
        mermaid_graph = _convert_for_debug(deepcopy(self.graph))
        self.debug[step] = {
            "time": datetime.datetime.now(),
            "inputs_buffer": list(inputs_buffer.items()),
            "pipeline_output": pipeline_output,
            "diagram": mermaid_graph,
        }

    def _clear_visits_count(self):
        """
        Make sure all nodes's visits count is zero.
        """
        for node in self.graph.nodes:
            self.graph.nodes[node]["visits"] = 0

    def _check_max_loops(self, component_name: str):
        """
        Verify whether this component run too many times.
        """
        if self.graph.nodes[component_name]["visits"] > self.max_loops_allowed:
            raise PipelineMaxLoops(
                f"Maximum loops count ({self.max_loops_allowed}) exceeded for component '{component_name}'."
            )

    def _skip_downstream_unvisited_nodes(self, component_name: str, inputs_buffer: OrderedDict) -> OrderedDict:
        """
        When a component is skipped, put all downstream nodes in the inputs buffer too: the might be skipped too,
        unless they are merge nodes. They will be evaluated later by the pipeline execution loop.
        """
        downstream_nodes = [e[1] for e in self.graph.out_edges(component_name)]
        for downstream_node in downstream_nodes:
            if downstream_node in inputs_buffer:
                continue
            if self.graph.nodes[downstream_node]["visits"] == 0:
                # Skip downstream nodes only if they never been visited
                inputs_buffer[downstream_node] = {}
        return inputs_buffer

    def _run_component(self, name: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Once we're confident this component is ready to run, run it and collect the output.
        """
        self.graph.nodes[name]["visits"] += 1
        instance = self.graph.nodes[name]["instance"]
        try:
            logger.info("* Running %s (visits: %s)", name, self.graph.nodes[name]["visits"])
            logger.debug("   '%s' inputs: %s", name, inputs)

            outputs = instance.run(**inputs)

            # Unwrap the output
            logger.debug("   '%s' outputs: %s\n", name, outputs)

            # Make sure the component returned a dictionary
            if not isinstance(outputs, dict):
                raise PipelineRuntimeError(
                    f"Component '{name}' returned a value of type "
                    f"'{getattr(type(outputs), '__name__', str(type(outputs)))}' instead of a dict. "
                    "Components must always return dictionaries: check the the documentation."
                )

        except Exception as e:
            raise PipelineRuntimeError(
                f"{name} raised '{e.__class__.__name__}: {e}' \nInputs: {inputs}\n\n"
                "See the stacktrace above for more information."
            ) from e

        return outputs
