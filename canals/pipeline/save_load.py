# pylint: disable=protected-access

from typing import Dict, List, Any, Tuple

import sys
import json
import logging
from pathlib import Path
from inspect import getmembers, isclass, getmodule, ismodule

from canals.pipeline.pipeline import Pipeline


logger = logging.getLogger(__name__)


def save_pipelines(
    pipelines: Dict[str, Pipeline], path: Path, _writer=lambda obj, handle: json.dump(obj, handle, indent=4)
) -> None:
    """
    Converts a dictionary of named Pipelines into a JSON file.

    Args:
        pipelines: dictionary of {name: pipeline_object}
        path: where to write the resulting file
        _writer: which function to use to write the dictionary to a file.
            Use this parameter to dump to a different format like YAML, TOML, HCL, etc.

    Returns:
        None
    """
    schema = marshal_pipelines(pipelines=pipelines)
    with open(path, "w", encoding="utf-8") as handle:
        _writer(schema, handle)


def load_pipelines(path: Path, _reader=json.load) -> Dict[str, Pipeline]:
    """
    Loads the content of a JSON file generated by `save_pipelines()` into
    a dictionary of named Pipelines.

    Args:
        path: where to read the file from
        _reader: which function to use to read the dictionary to a file.
            Use this parameter to load from a different format like YAML, TOML, HCL, etc.

    Returns:
        The pipelines as a dictionary of `{"pipeline-name": <pipeline object>}`
    """
    with open(path, "r", encoding="utf-8") as handle:
        schema = _reader(handle)
    return unmarshal_pipelines(schema=schema)


def marshal_pipelines(pipelines: Dict[str, Pipeline]) -> Dict[str, Any]:
    """
    Converts a dictionary of named Pipelines into a Python dictionary that can be
    written to a JSON file.

    Args:
        pipelines: A dictionary of `{"pipeline-name": <pipeline object>}`

    Returns:
        A Python dictionary representing the Pipelines objects above, that can be written to JSON and can be reused to
        recreate the original Pipelines.
    """
    schema: Dict[str, Any] = {}

    # Summarize pipeline configuration
    components: List[Tuple[str, str, object]] = []
    schema["pipelines"] = {}
    for pipeline_name, pipeline in pipelines.items():
        pipeline_repr: Dict[str, Any] = {}
        pipeline_repr["metadata"] = pipeline.metadata
        pipeline_repr["max_loops_allowed"] = pipeline.max_loops_allowed

        # Collect components
        pipeline_repr["components"] = {}
        for component_name in pipeline.graph.nodes:

            # Check if we saved the same instance twice (or more times) and replace duplicates with references.
            component_instance = pipeline.graph.nodes[component_name]["instance"]
            for existing_component_pipeline, existing_component_name, existing_components in components:
                if component_instance == existing_components:
                    # Build the pointer - done this way to support languages with no pointer syntax (e.g. JSON)
                    pipeline_repr["components"][component_name] = {
                        "refer_to": f"{existing_component_pipeline}.{existing_component_name}"
                    }
                    break

            # If no pointer was made in the previous step
            if not component_name in pipeline_repr["components"]:
                # Save the components in the global components list
                components.append((pipeline_name, component_name, component_instance))
                # Serialize the components
                component_repr = {
                    "type": component_instance.__class__.__name__,
                    "_init_parameters": component_instance._init_parameters,
                }
                pipeline_repr["components"][component_name] = component_repr

        pipeline_repr["connections"] = list(pipeline.graph.edges)
        schema["pipelines"][pipeline_name] = pipeline_repr

    # Collect the dependencies
    schema["dependencies"] = _discover_dependencies(components=[component[2] for component in components])
    return schema


def unmarshal_pipelines(schema: Dict[str, Any]) -> Dict[str, Pipeline]:  # pylint: disable=too-many-locals
    """
    Loads the content of a schema generated by `marshal_pipelines()` into
    a dictionary of named Pipelines.

    Args:
        schema: the schema of the pipelines, as generated by `marshal_pipelines()`.

    Returns:
        The pipelines as a dictionary of `{"pipeline-name": <pipeline object>}`.

    Raises ValueError: if any dependency is missing (see the `dependencies` field of the schema).

    """
    for dep in schema["dependencies"]:
        if not dep in sys.modules.keys():
            raise ValueError(
                f"You're loading a pipeline that depends on '{dep}', which is not in sys.modules. "
                "You need to import it before loading the pipelines."
            )

    classes = _find_decorated_classes(modules_to_search=schema["dependencies"])
    pipelines = {}
    component_instances: Dict[str, object] = {}
    for pipeline_name, pipeline_schema in schema["pipelines"].items():

        # Create the Pipeline object
        pipe_args = {"metadata": pipeline_schema.get("metadata", None)}
        if "max_loops_allowed" in pipeline_schema.keys():
            pipe_args["max_loops_allowed"] = pipeline_schema["max_loops_allowed"]
        pipe = Pipeline(**pipe_args)

        # Create the components or fish them from the buffer
        for component_name, component_schema in pipeline_schema["components"].items():
            if "refer_to" in component_schema.keys():
                pipe.add_component(
                    name=component_name,
                    instance=component_instances[component_schema["refer_to"]],
                )
            else:
                component_class = classes[component_schema["type"]]
                component_instance = component_class(**component_schema.get("_init_parameters", {}))
                component_instances[f"{pipeline_name}.{component_name}"] = component_instance
                pipe.add_component(
                    name=component_name,
                    instance=component_instance,
                )

        for connect_from, connect_to, sockets in pipeline_schema["connections"]:
            output_socket, input_socket = sockets.split("/", maxsplit=1)
            connect_from += "." + output_socket
            connect_to += "." + input_socket
            pipe.connect(connect_from=connect_from, connect_to=connect_to)

        pipelines[pipeline_name] = pipe

    return pipelines


def _discover_dependencies(components: List[object]) -> List[str]:
    """
    Given a list of components, it returns a list of all the modules that one needs to import to
    make this pipeline work.
    """
    module_names = [getmodule(component) for component in components]
    if any(not module_name for module_name in module_names):
        logger.error(
            "Can't identify the import module of some of your components. Your Pipelines might be unable to load back."
        )
    return list({module.__name__.split(".")[0] for module in module_names if module is not None}) + ["canals"]


def _find_decorated_classes(modules_to_search: List[str], decorator: str = "__canals_component__") -> Dict[str, type]:
    """
    Finds all classes decorated with `@components` in all the modules listed in `modules_to_search`.
    Returns a dictionary with the component class name and the component classes.

    Note: can be used for other decorators as well by setting the `decorator` parameter.
    """
    component_classes: Dict[str, type] = {}

    # Collect all modules
    for module in modules_to_search:

        if not module in sys.modules.keys():
            raise ValueError(f"{module} is not imported.")

        for name, entity in getmembers(sys.modules.get(module, None), ismodule):
            if f"{module}.{name}" in sys.modules.keys():
                modules_to_search.append(f"{module}.{name}")

        logger.debug("Searching under %s...", module)

        for _, entity in getmembers(sys.modules[module], isclass):
            if hasattr(entity, decorator):
                # It's a decorated class
                if getattr(entity, decorator) in component_classes:
                    logger.debug("'%s.%s' was imported more than once", module, getattr(entity, decorator))
                    continue

                component_classes[getattr(entity, decorator)] = entity
                logger.debug(" * Found class: %s", entity)

    return component_classes
