from typing import Any, Dict, List, Tuple

import logging

import networkx


logger = logging.getLogger(__name__)


#: A (component_name, socket_name) pair. This are the minimum "units" of the FSM's state.
StateUnit = Tuple[str, str]

#: A tuple of (component_name, socket_name) pairs. Represents the minimal set of inputs that make a component run.
#:  Note that this tuple never includes inputs that are not "waited for": they will be present in the component's
#:  input if they arrived in time, but won't be considered when checking if the component can run.
MinimalValidState = Tuple[StateUnit, ...]

#: Represents the full FSM's "state": a dictionary of (component_name, socket_name) -> value
#:  This dictionary is used to compute the next transition (its keys are used by `_state_transition_function`)
#:  and to run the components (see `_run_component``)
State = Dict[StateUnit, Any]

#: Represents the FSM's "transition": a list with the names of component that should run.
Transition = List[str]


def identify_looping_inputs(graph: networkx.MultiDiGraph, component_name: str):
    """
    Identify which of the input sockets of this component are coming from a loop and which are not.
    Used by `Pipeline._compute_valid_states` to ensure loop merging components' minimal valid states are build correctly.

    See the docstring of `Pipeline.run()` for a more complete overview of the algorithm.
    """
    input_from_loop = []
    input_outside_loop = []

    for socket in graph.nodes[component_name]["input_sockets"]:
        for sender in graph.nodes[component_name]["input_sockets"][socket].sender:
            if sender and networkx.has_path(graph, component_name, sender):
                input_from_loop.append(socket)
        if socket not in input_from_loop:
            input_outside_loop.append(socket)
    return input_from_loop, input_outside_loop


def identify_looping_outputs(graph: networkx.MultiDiGraph, component_name: str):
    """
    Identify which of the output sockets of this component are going into a loop and which are not.

    Used by `Pipeline._apply_transition` to make sure None values are not propagated endlessly into loops.

    See the docstring of `Pipeline.run()` for a more complete overview of the algorithm.
    """
    output_to_loop = []
    output_outside_loop = []

    for socket in graph.nodes[component_name]["output_sockets"]:
        for _, to_node, _ in graph.out_edges(component_name, keys=True):
            if to_node and networkx.has_path(graph, to_node, component_name):
                output_to_loop.append(socket)
            else:
                output_outside_loop.append(socket)

    return output_to_loop, output_outside_loop


def compute_valid_states(graph: networkx.MultiDiGraph) -> Dict[str, List[MinimalValidState]]:
    """
    Returns a list of all the valid minimal states that make a specific component run.

    Most components have a single minimal valid state that triggers their execution. However some components,
    namely loop merging ones, may have two distinct minimal valid states: one including the connection that comes
    from inside the loop, and one including the connection that comes from outside the loop.

    These tuples are used by `state_transition_function()` with `issubset()` to compute the next transition.

    See the docstring of `run()` for a more complete overview of the algorithm.
    """
    valid_states: Dict[str, List[MinimalValidState]] = {}
    for component_name in graph.nodes:
        input_from_loop, input_outside_loop = identify_looping_inputs(graph, component_name)
        if input_from_loop and input_outside_loop:
            # Is a loop merger, so it has two valid states, one for the loop and one for the external input
            valid_states[component_name] = [
                tuple((component_name, socket) for socket in input_from_loop),
                tuple((component_name, socket) for socket in input_outside_loop),
            ]
            continue

        # It's a regular component, so it has one minimum valid state only
        valid_state = []
        for socket_name, socket in graph.nodes[component_name]["input_sockets"].items():
            if not socket.has_default:
                valid_state.append((component_name, socket_name))

        valid_states[component_name] = [tuple(valid_state)]

    logger.info(
        "\nEach component will run as soon as these values are present:\n%s\n",
        _represent_valid_states_table(graph, valid_states),
    )
    return valid_states


def _represent_valid_states_table(graph: networkx.MultiDiGraph, valid_states: Dict[str, List[MinimalValidState]]):
    """
    Utility function that helps visualizing the valid states table.

    Renders it as:

        ```
        component_name   : source_component.socket1 + source_component_2.socket2  OR  source_component_3.socket3
        component_name_2 : a_component.socket3 + another_component_2.socket1 + component_2.socket2
        ```
    """
    longest_name = max(len(name) for name in graph.nodes)
    valid_states_strings = {
        key: [" + ".join(component + "." + socket for units in state for component, socket in units)]
        for key, state in valid_states.items()
    }
    states_repr = "\n".join(
        [
            f"  {component_name.ljust(longest_name)}: {'  OR  '.join(valid_state_strings)}"
            for component_name, valid_state_strings in valid_states_strings.items()
        ]
    )
    return states_repr


def state_transition_function(
    graph: networkx.MultiDiGraph, valid_states: Dict[str, List[MinimalValidState]], current_state: Tuple[StateUnit, ...]
) -> Transition:
    """
    From the keys of the current state of the pipeline (a list of StateUnits), returns the transition
    to perform (a Transition, a list of components that should run).

    This method checks which minimal valid states (tuples of StateUnits) are a subset of the current state
    (a list of StateUnits). For each match found, the respective component name is added to the transition.

    Args:
        graph: the Pipeline's graph
        valid_states: a mapping of the minimal valid state for each component. See compute_valid_states().
        current_state: the keys of the current state of the Pipeline (a list of StateUnit).

    Returns:
        A list of components that should run (a Transition). Note: the list can be empty. Such list represents
        a stopping state.
    """
    transition = []
    for component_name in graph.nodes:
        for valid_state in valid_states[component_name]:
            if set(valid_state).issubset(set(current_state)):
                transition.append(component_name)
    return transition
