# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Any, Dict, List, Union, Tuple

import datetime
import logging
from pathlib import Path
from copy import deepcopy

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
from canals.pipeline.state_transitions import (
    compute_valid_states,
    state_transition_function,
    identify_looping_outputs,
    State,
    Transition,
    MinimalValidState,
)
from canals.pipeline.connections import (
    parse_connection,
    _find_unambiguous_connection,
)
from canals.type_utils import _type_name
from canals.serialization import component_to_dict, component_from_dict

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
        Replaces instances of components with their class name in order to make sure they're comparable.

        This is a helper method for `__eq__`.
        """
        nodes = []
        for node in graph.nodes:
            comparable_node = graph.nodes[node]
            comparable_node["instance"] = comparable_node["instance"].__class__
            nodes.append(comparable_node)
        nodes.sort()
        return nodes

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
            connections.append(
                {
                    "sender": f"{sender}.{sender_socket}",
                    "receiver": f"{receiver}.{receiver_socket}",
                }
            )
        return {
            "metadata": self.metadata,
            "max_loops_allowed": self.max_loops_allowed,
            "components": components,
            "connections": connections,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **kwargs) -> "Pipeline":
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
        pipe = cls(
            metadata=metadata,
            max_loops_allowed=max_loops_allowed,
            debug_path=debug_path,
        )
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

    def add_component(self, name: str, instance: Component) -> None:
        """
        Create a node in the graph for the given component. Components are not connected to anything by default:
        use `Pipeline.connect()` to connect components together.

        Component names must be unique, and `_debug` is a reserved name. If `instance` is not a Canals component,
        a `PipelineValidationError` is raised.

        Args:
            name: the name of the component.
            instance: the component instance.

        Returns:
            None

        Raises:
            ValueError: if a component with the same name already exists or if the name is `_debug`.
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
        Directly connect socket to socket. This method DOES NOT type-check the connections: use 'Pipeline.connect()'
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
        Returns an instance of a component by name.

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
        without re-initializations.
        """
        for node in self.graph.nodes:
            if hasattr(self.graph.nodes[node]["instance"], "warm_up"):
                logger.info("Warming up component %s...", node)
                self.graph.nodes[node]["instance"].warm_up()

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

    def run(self, data: Dict[str, Any], debug: bool = False) -> Dict[str, Any]:  # pylint: disable=too-many-locals
        """
        Runs the pipeline.

        Pipelines are executed as Finite State Machines (FSM). In this context, we define:

        - state unit: a (component name, socket name) pair. In this tuple, the component name always refers to the
            component that uses the value, not the component that produces it. The same happens for the socket name:
            the pair always contains the name of the input socket, not the output one. Note: such translation is
            performed in `Pipeline._apply_transition()`. State units are represented by the `StateUnit` type.

        - state: the pipeline's state is a dictionary of `{state unit: value}`. It contains all the values that
            are available to the pipeline's component at a given step of the execution. It is represented by the `State`
            type.

        - minimal valid state: the minimal set of inputs (expressed as a list of `StateUnit`) that allow a specific
            component to run. Note that this list never includes inputs that are not "waited for": they will be present
            in the component's input if they arrived in time, but won't be considered when checking if the component
            can run. Most components have a single minimal valid state, but some (for example loop mergers) may have
            more than one. See `compute_valid_states()`. Minimal valid states are represented by the
            `MinimalValidState` type.

        - transition: a transition is a list of components that are ready to run. "Ready to run" means that the
            pipeline's state contains a combination of the values that the component needs for its `run()` method.
            Transitions are represented by the `Transition` type.

        At the start, the pipeline is in the "initial state", which is defined by the pipeline's inputs.

        Before entering the execution loop, Pipeline computes what are the minimal valid states for each component.

        The pipeline then enters the execution loop, where each iteration is a "transition". The transition is computed
        by the `_state_transition_function` function, which returns a list of components that are ready to run.
        Computing the state transition means checking how many minimal valid states are included in the current state
        and adding the respective components to the transition.

        Transitions can be empty: these represents a stopping state. If one is encountered, the pipeline stops and
        returns all the output generated so far to the user.

        Once the transition is computed, the pipeline runs all the components in the transition. The order in which
        they are run is not important, as long as they are all run before the next transition is started. The output
        of each component is then used to build the next pipeline state of the pipeline by translating the
        `producing component, output socket` tuples into `receiving component, input socket` and storing the results
        in the `state` dictionary. This translation is performed by `Pipeline._apply_transition()`.

        If there is no possible translation for a `producing component, output socket` tuple (because that output
        socket is not connected to any other component), its content is stored into the output dictionary and becomes
        part of the pipeline's output.

        In the end, the pipeline returns the `output` dictionary to the user. Note that `None` outputs are cleaned up
        before returning the dictionary to the user.

        Args:
            data: the inputs to give to the input components of the Pipeline. It should look like
            `{"component": {"value": 5} "component2": {"another_value": "hello"}}`
            debug: whether to collect and return debug information.

        Returns:
            A dictionary with the outputs of the output components of the Pipeline. Example of such output is
            `{"sum": {"result": 5}}`.

        Raises:
            PipelineRuntimeError: if the any of the components fails or return unexpected output.
        """
        if debug:
            logger.warning("Debug mode is still WIP")

        data = validate_pipeline_input(self.graph, input_values=data)
        self._clear_visits_count()
        self.warm_up()
        pipeline_output: Dict[str, Any] = {}

        logger.info("Pipeline execution started.")

        # Initial state: it is defined by the pipeline's inputs
        state: State = {}
        for component_name, input_data in data.items():
            for socket, value in input_data.items():
                if value is not None:
                    # Wrap variadic inputs into lists
                    if self.graph.nodes[component_name]["input_sockets"][socket].is_variadic:
                        value = [value]
                    state[(component_name, socket)] = value

        # List all the input/output socket pairs - for quicker access
        connections = [
            (from_node, sockets.split("/")[0], to_node, sockets.split("/")[1])
            for from_node, to_node, sockets in self.graph.edges
        ]

        # Compute the valid states for each component
        valid_states: Dict[str, List[MinimalValidState]] = compute_valid_states(self.graph)

        # Execution loop
        step = 0
        while True:
            step += 1

            # Get the transition to perform (see state_transition_function())
            transition = state_transition_function(self.graph, valid_states, current_state=tuple(state.keys()))
            logger.debug("##### %s^ TRANSITION #####", step)
            logger.debug(" State: %s", " + ".join(sorted(f"{s[0]}.{s[1]}" for s in state)))
            logger.debug(" Transition: %s", transition)

            # Termination: stopping states return an empty transition
            if not transition:
                logger.debug("   --X This is a stopping state.")
                break

            # Apply the transition to get to the next state (see _apply_transition)
            output: Dict[str, Any]
            state, output = self._apply_transition(state, transition, connections)
            pipeline_output = {**pipeline_output, **output}

        logger.info("Pipeline executed successfully.")

        # Clean up output dictionary from None values: they normally come from branches that didn't run.
        clean_output = {}
        for component_name, outputs in pipeline_output.items():
            if not all(value is None for value in outputs.values()):
                clean_output[component_name] = {key: value for key, value in outputs.items() if value is not None}

        return clean_output

    def _apply_transition(
        self, state: State, transition: Transition, connections: List[Tuple[str, str, str, str]]
    ) -> Tuple[State, Dict[str, Any]]:
        """
        Given the current state of the pipeline, compute the next state. Returns a Tuple of
        `(pipeline state, pipeline output)`.

        A "transition" is a set of components that are ready to run, because all the inptus they need are present in
        the "state" of the pipeline (the first parameter of this function). They can be run in any order, as long as
        they are all run before the next transition is started.

        See the docstring of the `Pipeline.run()` method for more information about the execution model.

        Args:
            state: the current state of the pipeline, as a dictionary of (component, socket) -> value
            transition: the list of components that are ready to run
            connections: the list of all the connections in the pipeline. This is needed to translate output sockets
                into input sockets for the receiving components

        Returns:
            a tuple of (pipeline state, pipeline output), where "state" is the next state of the pipeline and "output" is the
            data that is part of the Pipeline's global output (the values that return to the user at the end of the
            execution).
        """
        output: Dict[str, Any] = {}
        next_state = state

        # Process all the component in this transition independently
        for component_name in transition:
            # Extract from the general state only the inputs for this component
            component_inputs = {
                socket: value for (component, socket), value in state.items() if component == component_name
            }

            # Once an input is used, remove it from the state.
            # Leftover state will be carried over to the next state.
            for used_input in component_inputs.keys():
                del next_state[(component_name, used_input)]

            # Run the component
            output_values = self._run_component(
                name=component_name,
                inputs=component_inputs,
            )

            # Translate output sockets into input sockets to build the next state
            for socket, value in output_values.items():
                next_state = self._translate_output_state_to_input_state(
                    value, component_name, socket, connections, next_state, output
                )

        return next_state, output

    def _translate_output_state_to_input_state(
        self,
        value: Any,
        component_name: str,
        socket: str,
        connections: List[Tuple[str, str, str, str]],
        next_state: State,
        output: Dict[str, Any],
    ):
        target_states = [
            (to_node, to_socket)
            for from_node, from_socket, to_node, to_socket in connections
            if from_node == component_name and from_socket == socket
        ]

        # If the sockets are dangling, this value is an output
        if not target_states:
            if component_name not in output:
                output[component_name] = {}
            output[component_name][socket] = value
            logger.debug(" - '%s.%s' goes to the output with value '%s'", component_name, socket, value)

        # Otherwise, assign the values to the next state
        for target in target_states:
            if all(identify_looping_outputs(self.graph, component_name)) and value is None:
                logger.debug("   --X Loop decision nodes do not propagate None values.")
            else:
                # If the socket was marked as variadic, pile up inputs in a list
                is_variadic = self.graph.nodes[target[0]]["input_sockets"][target[1]].is_variadic
                if is_variadic:
                    if target not in next_state:
                        next_state[target] = []
                    next_state[target].append(value)
                # Non-variadic input: just store the value
                else:
                    next_state[target] = value

                logger.debug("   --> '%s.%s' received '%s'", target[0], target[1], value)

        return next_state

    def _run_component(self, name: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Once we're confident this component is ready to run, run it and collect the output.

        This method performs some additional checks, such as checking that max_loops is respected, that the component
        produced a dictionary, and so on.
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
                value is None and not self.graph.nodes[name]["input_sockets"][socket_name].is_optional
                for socket_name, value in inputs.items()
            ):
                logger.debug("   --X '%s' received None on a mandatory input: skipping.", name)
                output_dict: Dict[str, Any] = {key: None for key in self.graph.nodes[name]["output_sockets"].keys()}
                logger.debug("   '%s' outputs: %s", name, output_dict)
                return output_dict

            outputs = instance.run(**inputs)

            # Unwrap the output
            logger.debug("   '%s' outputs: %s", name, outputs)

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
