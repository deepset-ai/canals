# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Any, Dict, List, Union, Tuple

import datetime
import logging
from pathlib import Path
from copy import deepcopy
from dataclasses import asdict

import networkx

from canals.component import Component
from canals.errors import PipelineConnectError, PipelineMaxLoops, PipelineRuntimeError, PipelineValidationError
from canals.draw import draw, convert_for_debug, RenderingEngines
from canals.pipeline.sockets import InputSocket, OutputSocket, find_input_sockets, find_output_sockets
from canals.pipeline.validation import validate_pipeline_input
from canals.pipeline.connections import parse_connection_name, find_unambiguous_connection, get_socket_type_desc


logger = logging.getLogger(__name__)


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
        self.valid_states: Dict[str, List[List[Tuple[str, str]]]] = {}
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

    def _comparable_nodes_list(self, graph: networkx.MultiDiGraph) -> List[Dict[str, Any]]:
        """
        Replaces instances of nodes with their class name and defaults list in order to make sure they're comparable.
        """
        nodes = []
        for node in graph.nodes:
            comparable_node = graph.nodes[node]
            if hasattr(comparable_node, "defaults"):
                comparable_node["defaults"] = comparable_node["instance"].defaults
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
        if not hasattr(instance, "__canals_component__"):
            raise PipelineValidationError(
                f"'{type(instance)}' doesn't seem to be a component. Is this class decorated with @component?"
            )

        # Find inputs and outputs
        input_sockets = find_input_sockets(instance)
        output_sockets = find_output_sockets(instance)

        # Add component to the graph, disconnected
        logger.debug("Adding component '%s' (%s)", name, instance)
        self.graph.add_node(
            name,
            instance=instance,
            input_sockets=input_sockets,
            output_sockets=output_sockets,
            visits=0,
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
        from_node, from_socket_name = parse_connection_name(connect_from)
        to_node, to_socket_name = parse_connection_name(connect_to)

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
                    + ", ".join(
                        [f"{name} (type {get_socket_type_desc(socket.type)})" for name, socket in from_sockets.items()]
                    )
                )
        if to_socket_name:
            to_socket = to_sockets.get(to_socket_name, None)
            if not to_socket:
                raise PipelineConnectError(
                    f"'{to_node}.{to_socket_name} does not exist. "
                    f"Input connections of {to_node} are: "
                    + ", ".join(
                        [f"{name} (type {get_socket_type_desc(socket.type)})" for name, socket in to_sockets.items()]
                    )
                )

        # Look for an unambiguous connection among the possible ones.
        # Note that if there is more than one possible connection but two sockets match by name, they're paired.
        from_sockets = [from_socket] if from_socket_name else list(from_sockets.values())
        to_sockets = [to_socket] if to_socket_name else list(to_sockets.values())
        from_socket, to_socket = find_unambiguous_connection(
            sender_node=from_node, sender_sockets=from_sockets, receiver_node=to_node, receiver_sockets=to_sockets
        )

        # Connect the components on these sockets
        self._direct_connect(from_node=from_node, from_socket=from_socket, to_node=to_node, to_socket=to_socket)

    def _direct_connect(self, from_node: str, from_socket: OutputSocket, to_node: str, to_socket: InputSocket) -> None:
        """
        Directly connect socket to socket. This method does not type-check the connections: use 'Pipeline.connect()'
        instead (which uses 'find_unambiguous_connection()' to validate types).
        """
        # Make sure the receiving socket isn't already connected - sending sockets can be connected as many times as needed,
        # so they don't need this check
        if to_socket.sender:
            raise PipelineConnectError(
                f"Cannot connect '{from_node}.{from_socket.name}' with '{to_node}.{to_socket.name}': "
                f"{to_node}.{to_socket.name} is already connected to {to_socket.sender}.\n"
            )

        # Create the connection
        logger.debug("Connecting '%s.%s' to '%s.%s'", from_node, from_socket.name, to_node, to_socket.name)
        edge_key = f"{from_socket.name}/{to_socket.name}"
        self.graph.add_edge(from_node, to_node, key=edge_key, from_socket=from_socket, to_socket=to_socket)

        # Stores the name of the node that will send its output to this socket
        to_socket.sender = from_node

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

    def draw(self, path: Path, engine: RenderingEngines = "mermaid-img") -> None:
        """
        Draws the pipeline. Requires either `graphviz` as a system dependency, or an internet connection for Mermaid.
        Run `pip install canals[graphviz]` or `pip install canals[mermaid]` to install missing dependencies.

        Args:
            path: where to save the diagram.
            engine: which format to save the graph as. Accepts 'graphviz', 'mermaid-text', 'mermaid-img'.
                Default is 'mermaid-img'.

        Returns:
            None

        Raises:
            ImportError: if `engine='graphviz'` and `pygraphviz` is not installed.
            HTTPConnectionError: (and similar) if the internet connection is down or other connection issues.
        """
        draw(graph=deepcopy(self.graph), path=path, engine=engine)

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

    def _record_pipeline_step(self, step, inputs_buffer, pipeline_output):
        """
        Stores a snapshot of this step into the self.debug dictionary of the pipeline.
        """
        mermaid_graph = convert_for_debug(deepcopy(self.graph))
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

    def _compute_valid_states(self):
        """
        Returns a list of all the valid minimal states that would lead to a specific component to run.
        These tuples are used by `_state_transition_function()` with `issubset()` to compute the next transition.
        """
        self.valid_states = {}
        for component in self.graph.nodes:
            input_from_loop, input_outside_loop = self._identify_looping_inputs(component)
            if input_from_loop and input_outside_loop:
                # Is a loop merger, so it has two valid states, one for the loop and one for the external input
                self.valid_states[component] = [
                    [(component, socket) for socket in input_from_loop],
                    [(component, socket) for socket in input_outside_loop],
                ]
                continue

            # It's a regular component, so it has one minimum valid state only
            valid_state = []
            for socket in self.graph.nodes[component]["input_sockets"]:
                if not socket in self.graph.nodes[component]["instance"].defaults:
                    valid_state.append((component, socket))

            self.valid_states[component] = [valid_state]

    def _identify_looping_inputs(self, component: str):
        """
        Identify which of the input sockets of this component are coming from a loop and which are not.
        """
        input_from_loop = []
        input_outside_loop = []

        for socket in self.graph.nodes[component]["input_sockets"]:
            sender = self.graph.nodes[component]["input_sockets"][socket].sender
            if sender and networkx.has_path(self.graph, component, sender):
                input_from_loop.append(socket)
            else:
                input_outside_loop.append(socket)
        return input_from_loop, input_outside_loop

    def _identify_looping_outputs(self, component: str):
        """
        Identify which of the output sockets of this component are going into a loop and which are not.
        """
        output_to_loop = []
        output_outside_loop = []

        for socket in self.graph.nodes[component]["output_sockets"]:
            for _, to_node, _ in self.graph.out_edges(component, keys=True):
                if to_node and networkx.has_path(self.graph, to_node, component):
                    output_to_loop.append(socket)
                else:
                    output_outside_loop.append(socket)

        return output_to_loop, output_outside_loop

    def _transition_function(self, current_state: Tuple[Tuple[str, str], ...]) -> List[str]:
        """
        Given the current state as a list of tuples of (component, socket), returns the transition to perform
        as list of components that should run.

        Args:
            current_state (Tuple[Tuple[str, str]]): the current state as a list of tuples of (component, socket)
        """
        transition = []
        for component in self.graph.nodes:
            for valid_state in self.valid_states[component]:
                if set(valid_state).issubset(set(current_state)):
                    transition.append(component)

        return transition

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
        if debug:
            logger.warning("Debug mode is still WIP")

        data = validate_pipeline_input(self.graph, input_values=data)
        self._clear_visits_count()
        self.warm_up()
        pipeline_output: Dict[str, Any] = {}

        logger.info("Pipeline execution started.")

        # List all the input/output socket pairs - for quicker access
        connections = [
            (from_node, sockets.split("/")[0], to_node, sockets.split("/")[1])
            for from_node, to_node, sockets in self.graph.edges
        ]
        self._compute_valid_states()

        # Initial state
        state: Dict[Tuple[str, str], Any] = {}
        for component, input_data in data.items():
            for socket, value in asdict(input_data).items():
                if value is not None:
                    state[(component, socket)] = value

        # Execution loop
        step = 0
        while True:
            step += 1
            logger.debug("##### %s^ transition #####", step)
            output: Dict[str, Any]
            state, output = self._compute_next_state(state, connections)
            if not state and not output:
                break
            pipeline_output = {**pipeline_output, **output}

        logger.info("Pipeline executed successfully.")

        # Clean up output dictionary from None values
        clean_output = {}
        for component, outputs in pipeline_output.items():
            if not all(value is None for value in outputs.values()):
                clean_output[component] = self.graph.nodes[component]["instance"].output(**outputs)

        return clean_output

    def _compute_next_state(
        self, state: Dict[Tuple[str, str], Any], connections: List[Tuple[str, str, str, str]]
    ) -> Tuple[Dict[Tuple[str, str], Any], Dict[str, Any]]:
        """
        Given the current state of the pipeline, compute the next state. Returns a Tuple with (state, output)
        """
        output: Dict[str, Any] = {}
        next_state = state

        # Get the transition to compute
        current_transition = self._transition_function(tuple(state.keys()))

        # Check if this is a stopping state -
        logger.debug("State: %s | Transition: %s", tuple(state.keys()), current_transition)
        if len(current_transition) == 0:
            logger.debug("   --X Reached empty transition, stopping.")
            return ({}, {})

        # Process all the component in this transition independently ("parallel branch execution")
        for component in current_transition:
            # Extract from the general state only the inputs for this component
            component_inputs = {state[1]: value for state, value in state.items() if state[0] == component}

            # Once an input is being used, remove it from the machine's state.
            # Leftover state will be carried over ("waiting for components")
            for used_input in component_inputs.keys():
                del next_state[(component, used_input)]

            # Run the component
            output_values = self._run_component(
                name=component,
                inputs=component_inputs,
            )

            # Translate output sockets into input sockets - builds the next state
            for socket, value in output_values.items():
                target_states = [
                    (to_node, to_socket)
                    for from_node, from_socket, to_node, to_socket in connections
                    if from_node == component and from_socket == socket
                ]

                # If the sockets are dangling, this value is an output
                if not target_states:
                    if component not in output:
                        output[component] = {}
                    output[component][socket] = value
                    logger.debug(" - '%s.%s' goes to the output with value '%s'", component, socket, value)

                for target in target_states:
                    logger.error(
                        " is %s a loop decision node? %s", component, self._identify_looping_outputs(component)
                    )
                    if all(self._identify_looping_outputs(component)) and value is None:
                        logger.debug("   --X Loop decision nodes do not propagate None values.")
                    else:
                        next_state[target] = value
                        logger.debug("   --> '%s.%s' received '%s'", target[0], target[1], value)

        return next_state, output

    def _run_component(self, name: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Once we're confident this component is ready to run, run it and collect the output.
        """
        self._check_max_loops(name)
        self.graph.nodes[name]["visits"] += 1
        instance = self.graph.nodes[name]["instance"]
        try:
            logger.info("* Running %s (visits: %s)", name, self.graph.nodes[name]["visits"])
            logger.debug("   '%s' inputs: %s", name, inputs)

            # Check if any None was received by a value that was not Optional:
            # if so, return an empty output dataclass ("skipping the component")
            if all(value is None for value in inputs.values()) or any(
                value is None and socket not in instance.__canals_optional_inputs__ for socket, value in inputs.items()
            ):
                logger.debug("   --X '%s' received None on a mandatory input or on all inputs: skipping.", name)
                output_dict = asdict(instance.output())
                logger.debug("   '%s' outputs: %s", name, output_dict)
                return output_dict

            # Pass the inputs as kwargs after adding the component's own defaults to them
            inputs = {**instance.defaults, **inputs}
            input_dataclass = instance.input(**inputs)

            output_dataclass = instance.run(input_dataclass)
            output_dict = asdict(output_dataclass)

            # Unwrap the output
            logger.debug("   '%s' outputs: %s", name, output_dict)

        except Exception as e:
            raise PipelineRuntimeError(
                f"{name} raised '{e.__class__.__name__}: {e}' \nInputs: {inputs}\n\n"
                "See the stacktrace above for more information."
            ) from e

        return output_dict
