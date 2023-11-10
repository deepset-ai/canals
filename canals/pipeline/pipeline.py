# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Any, Dict, List, Union, TypeVar, Type, Set

import os
import json
import datetime
import logging
from pathlib import Path
from copy import deepcopy
from collections import defaultdict

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
from canals.pipeline.validation import validate_pipeline_input, find_pipeline_inputs
from canals.pipeline.connections import Connection, parse_connect_string, _find_matching_sockets
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
        self._connections: List[Connection] = []
        self._mandatory_connections: Dict[str, List[Connection]] = defaultdict(list)
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
        from_node, from_socket_name = parse_connect_string(connect_from)
        to_node, to_socket_name = parse_connect_string(connect_to)

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
        from_socket, to_socket = _find_matching_sockets(
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
        if to_socket.senders and not to_socket.is_variadic:
            raise PipelineConnectError(
                f"Cannot connect '{from_node}.{from_socket.name}' with '{to_node}.{to_socket.name}': "
                f"{to_node}.{to_socket.name} is already connected to {to_socket.senders}.\n"
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
        to_socket.senders.append(from_node)
        # Stores the name of the nodes that will receive their input from this socket
        from_socket.consumers.append(to_node)

        # Stores the Connection object for easier access during run()
        connection = Connection(from_node, from_socket, to_node, to_socket)
        self._connections.append(connection)
        if not connection.has_default():
            self._mandatory_connections[to_node].append(connection)

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

    def inputs(self):
        """
        Returns a dictionary with the input names and types that this pipeline accepts.
        """
        inputs = {
            comp: {socket.name: {"type": socket.type, "has_default": socket.has_default} for socket in data}
            for comp, data in find_pipeline_inputs(self.graph).items()
            if data
        }
        return inputs

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

    def run(self, data: Dict[str, Any], debug: bool = False) -> Dict[str, Any]:  # pylint: disable=too-many-locals
        """
        Runs the pipeline.

        Args:
            data: the inputs to give to the input components of the Pipeline.
            debug: whether to collect and return debug information.

        Returns:
            A dictionary with the outputs of the output components of the Pipeline.

        Raises:
            PipelineRuntimeError: if the any of the components fail or return unexpected output.
        """
        self._clear_visits_count()
        data = validate_pipeline_input(self.graph, input_values=data)
        logger.info("Pipeline execution started.")

        self.debug = {}
        if debug:
            logger.info("Debug mode ON.")
            os.makedirs("debug", exist_ok=True)

        logger.debug(
            "Mandatory connections:\n%s",
            "\n".join(
                f" - {component}: {', '.join([str(s) for s in sockets])}"
                for component, sockets in self._mandatory_connections.items()
            ),
        )

        self.warm_up()

        # Prepare the inputs buffer and components queue
        components_queue: List[str] = []
        mandatory_values_buffer: Dict[Connection, Any] = {}
        optional_values_buffer: Dict[Connection, Any] = {}
        pipeline_output: Dict[str, Dict[str, Any]] = defaultdict(dict)

        for node_name, input_data in data.items():
            for socket_name, value in input_data.items():
                connection = Connection(
                    None, None, node_name, self.graph.nodes[node_name]["input_sockets"][socket_name]
                )
                self._add_value_to_buffers(
                    value, connection, components_queue, mandatory_values_buffer, optional_values_buffer
                )

        # *** PIPELINE EXECUTION LOOP ***
        step = 0
        while components_queue:  # pylint: disable=too-many-nested-blocks
            step += 1
            if debug:
                self._record_pipeline_step(
                    step, components_queue, mandatory_values_buffer, optional_values_buffer, pipeline_output
                )

            component_name = components_queue.pop(0)
            logger.debug("> Queue at step %s: %s %s", step, component_name, components_queue)
            self._check_max_loops(component_name)

            # **** RUN THE NODE ****
            if not self._ready_to_run(component_name, mandatory_values_buffer, components_queue):
                continue

            inputs = {
                **self._extract_inputs_from_buffer(component_name, mandatory_values_buffer),
                **self._extract_inputs_from_buffer(component_name, optional_values_buffer),
            }
            outputs = self._run_component(name=component_name, inputs=dict(inputs))

            # **** PROCESS THE OUTPUT ****
            for socket_name, value in outputs.items():
                targets = self._collect_targets(component_name, socket_name)
                if not targets:
                    pipeline_output[component_name][socket_name] = value
                else:
                    for target in targets:
                        self._add_value_to_buffers(
                            value, target, components_queue, mandatory_values_buffer, optional_values_buffer
                        )

        if debug:
            self._record_pipeline_step(
                step + 1, components_queue, mandatory_values_buffer, optional_values_buffer, pipeline_output
            )
            os.makedirs(self.debug_path, exist_ok=True)
            with open(self.debug_path / "data.json", "w", encoding="utf-8") as datafile:
                json.dump(self.debug, datafile, indent=4, default=str)
            pipeline_output["_debug"] = self.debug  # type: ignore

        logger.info("Pipeline executed successfully.")
        return dict(pipeline_output)

    def _record_pipeline_step(
        self, step, components_queue, mandatory_values_buffer, optional_values_buffer, pipeline_output
    ):
        """
        Stores a snapshot of this step into the self.debug dictionary of the pipeline.
        """
        mermaid_graph = _convert_for_debug(deepcopy(self.graph))
        self.debug[step] = {
            "time": datetime.datetime.now(),
            "components_queue": components_queue,
            "mandatory_values_buffer": mandatory_values_buffer,
            "optional_values_buffer": optional_values_buffer,
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

    def _add_value_to_buffers(
        self,
        value: Any,
        connection: Connection,
        components_queue: List[str],
        mandatory_values_buffer: Dict[Connection, Any],
        optional_values_buffer: Dict[Connection, Any],
    ):
        """
        Given a value and the connection it is being sent on, it updates the buffers and the components queue.
        """
        if not connection.has_default():
            mandatory_values_buffer[connection] = value
            if connection.consumer_component and connection.consumer_component not in components_queue:
                components_queue.append(connection.consumer_component)
        else:
            optional_values_buffer[connection] = value

    def _ready_to_run(
        self, component_name: str, mandatory_values_buffer: Dict[Connection, Any], components_queue: List[str]
    ) -> bool:
        """
        Returns True if a component is ready to run, False otherwise.
        """
        received_connections = set(
            conn for conn in mandatory_values_buffer.keys() if conn.consumer_component == component_name
        )
        expected_connections = set(self._mandatory_connections[component_name])
        if expected_connections.issubset(received_connections):
            logger.debug("Component '%s' is ready to run. All mandatory values were received.", component_name)
            return True

        # Check whether the missing values are still being computed
        missing_connections: Set[Connection] = expected_connections - received_connections
        connections_to_wait = []
        for missing_conn in missing_connections:
            if any(
                networkx.has_path(self.graph, component_to_run, missing_conn.producer_component)
                for component_to_run in components_queue
            ):
                connections_to_wait.append(missing_conn)
        if not connections_to_wait:
            # Just run the component: missing sockets will never arrive
            logger.debug(
                "Component '%s' is ready to run. A variadic input parameter received all the expected values.",
                component_name,
            )
            return True

        # Wait for the values
        logger.debug(
            "Component '%s' is not ready to run, some values are still missing: %s",
            component_name,
            connections_to_wait,
        )
        components_queue.append(component_name)
        return False

    def _extract_inputs_from_buffer(self, component_name: str, buffer: Dict[Connection, Any]) -> Dict[str, Any]:
        """
        Extract a component's input values from one of the value buffers.
        """
        inputs = defaultdict(list)
        connections: List[Connection] = []

        for connection in buffer.keys():
            if connection.consumer_component == component_name:
                connections.append(connection)

        for key in connections:
            value = buffer.pop(key)
            if key.consumer_socket:
                if key.consumer_socket.is_variadic:
                    inputs[key.consumer_socket.name].append(value)
                else:
                    inputs[key.consumer_socket.name] = value
        return inputs

    def _run_component(self, name: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Once we're confident this component is ready to run, run it and collect the output.
        """
        self.graph.nodes[name]["visits"] += 1
        instance = self.graph.nodes[name]["instance"]
        try:
            logger.info("* Running %s", name)
            logger.debug("   '%s' inputs: %s", name, inputs)

            outputs = instance.run(**inputs)

            # Unwrap the output
            logger.debug("   '%s' outputs: %s\n", name, outputs)

            # Make sure the component returned a dictionary
            if not isinstance(outputs, dict):
                raise PipelineRuntimeError(
                    f"Component '{name}' returned a value of type '{_type_name(type(outputs))}' instead of a dict. "
                    "Components must always return dictionaries: check the the documentation."
                )

        except Exception as e:
            raise PipelineRuntimeError(
                f"{name} raised '{e.__class__.__name__}: {e}' \nInputs: {inputs}\n\n"
                "See the stacktrace above for more information."
            ) from e

        return outputs

    def _collect_targets(self, component_name: str, socket_name: str) -> List[Connection]:
        """
        Given and component and an output socket names, returns a list of Connections
        where these are the producers. Used to route output.
        """
        return [
            connection
            for connection in self._connections
            if connection.producer_component == component_name
            and connection.producer_socket
            and connection.producer_socket.name == socket_name
        ]
