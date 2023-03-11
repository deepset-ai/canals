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

    :param pipelines: dictionary of {name: pipeline_object}
    :param path: where to write the resulting file
    :param _writer: which function to use to write the dictionary to a file.
        Use this parameter to dump to a different format like YAML, TOML, HCL, etc.
    """
    schema = marshal_pipelines(pipelines=pipelines)
    with open(path, "w", encoding="utf-8") as handle:
        _writer(schema, handle)


def load_pipelines(path: Path, _reader=json.load) -> Dict[str, Pipeline]:
    """
    Loads the content of a JSON file generated by `save_pipelines()` into
    a dictionary of named Pipelines.

    :param path: where to read the file from
    :param _reader: which function to use to read the dictionary to a file.
        Use this parameter to load from a different format like YAML, TOML, HCL, etc.
    """
    with open(path, "r", encoding="utf-8") as handle:
        schema = _reader(handle)
    return unmarshal_pipelines(schema=schema)


def marshal_pipelines(pipelines: Dict[str, Pipeline]) -> Dict[str, Any]:
    """
    Converts a dictionary of named Pipelines into a Python dictionary that can be
    written to a JSON file.

    :param pipelines: dictionary of {name: pipeline_object}
    """
    schema: Dict[str, Any] = {}

    # Summarize pipeline configuration
    nodes: List[Tuple[str, str, object]] = []
    schema["pipelines"] = {}
    for pipeline_name, pipeline in pipelines.items():
        pipeline_repr: Dict[str, Any] = {}
        pipeline_repr["metadata"] = pipeline.metadata
        pipeline_repr["max_loops_allowed"] = pipeline.max_loops_allowed

        # Collect nodes
        pipeline_repr["nodes"] = {}
        for node_name in pipeline.graph.nodes:

            # Check if we saved the same instance twice (or more times) and replace duplicates with references.
            node_instance = pipeline.graph.nodes[node_name]["instance"]
            for existing_node_pipeline, existing_node_name, existing_node in nodes:
                if node_instance == existing_node:
                    # Build the pointer - done this way to support languages with no pointer syntax (e.g. JSON)
                    pipeline_repr["nodes"][node_name] = {"refer_to": f"{existing_node_pipeline}.{existing_node_name}"}
                    break

            # If no pointer was made in the previous step
            if not node_name in pipeline_repr["nodes"]:
                # Save the node in the global nodes list
                nodes.append((pipeline_name, node_name, node_instance))
                # Serialize the node
                node_repr = {"type": node_instance.__class__.__name__, "init_parameters": node_instance.init_parameters}
                pipeline_repr["nodes"][node_name] = node_repr

            # Check for run parameters
            if pipeline.graph.nodes[node_name]["parameters"]:
                pipeline_repr["nodes"][node_name]["run_parameters"] = pipeline.graph.nodes[node_name]["parameters"]

        pipeline_repr["edges"] = list(pipeline.graph.edges)
        schema["pipelines"][pipeline_name] = pipeline_repr

    # Collect the dependencies
    schema["dependencies"] = _discover_dependencies(nodes=[node[2] for node in nodes])
    return schema


def unmarshal_pipelines(schema: Dict[str, Any]) -> Dict[str, Pipeline]:
    """
    Loads the content of a schema generated by `marshal_pipelines()` into
    a dictionary of named Pipelines.

    :param schema: the schema of the pipelines generated by `marshal_pipelines()`
    """
    for dep in schema["dependencies"]:
        if not dep in sys.modules.keys():
            raise ValueError(
                f"You're loading a pipeline that depends on '{dep}', which is not in sys.modules. "
                "You need to import it before loading the pipelines."
            )

    node_classes = _find_node_classes(modules_to_search=schema["dependencies"])
    pipelines = {}
    node_instances: Dict[str, object] = {}
    for pipeline_name, pipeline_schema in schema["pipelines"].items():

        # Create the Pipeline object
        pipe_args = {"metadata": pipeline_schema.get("metadata", None)}
        if "max_loops_allowed" in pipeline_schema.keys():
            pipe_args["max_loops_allowed"] = pipeline_schema["max_loops_allowed"]
        pipe = Pipeline(**pipe_args)

        # Create the nodes or fish them from the buffer
        for node_name, node_schema in pipeline_schema["nodes"].items():
            if "refer_to" in node_schema.keys():
                pipe.add_node(
                    name=node_name,
                    instance=node_instances[node_schema["refer_to"]],
                    parameters=node_schema.get("run_parameters", {}),
                )
            else:
                node_instance = node_classes[node_schema["type"]](**node_schema.get("init_parameters", {}))
                node_instances[f"{pipeline_name}.{node_name}"] = node_instance
                pipe.add_node(name=node_name, instance=node_instance, parameters=node_schema.get("run_parameters", {}))

        for edge in pipeline_schema["edges"]:
            pipe.connect(*edge)

        pipelines[pipeline_name] = pipe

    return pipelines


def _discover_dependencies(nodes: List[object]) -> List[str]:
    """
    Given a list of nodes, it returns a list of all the modules that one needs to import to
    make this pipeline work.
    """
    module_names = [getmodule(node) for node in nodes]
    if any(not module_name for module_name in module_names):
        logger.error(
            "Can't identify the import module of some of your nodes. Your Pipelines might be unable to load back."
        )
    return list({module.__name__.split(".")[0] for module in module_names if module is not None}) + ["canals"]


def _find_node_classes(modules_to_search: List[str]) -> Dict[str, type]:
    """
    Finds all classes decorated with `@node` in all the modules listed in `modules_to_search`.

    Returns a dictionary with the node class name and the node classes.
    """
    node_classes: Dict[str, type] = {}

    # Collect all modules
    for module in modules_to_search:
        for name, entity in getmembers(sys.modules.get(module, None), ismodule):
            if not name.startswith("@") and f"{module}.{name}" in sys.modules.keys():
                modules_to_search.append(f"{module}.{name}")

        logger.debug("Searching for nodes under %s...", module)

        if not module in sys.modules.keys():
            raise ValueError(f"{module} is not imported.")

        duplicate_names = []
        for _, entity in getmembers(sys.modules[module], isclass):
            if hasattr(entity, "__canals_node__"):
                # It's a node
                if entity.__canals_node__ in node_classes:
                    if node_classes[entity.__canals_node__] == entity:
                        logger.debug("'%s' was imported more than once", entity.__canals_node__)
                        continue

                    # Two nodes were discovered with the same name - namespace them
                    other_entity = node_classes[entity.__canals_node__]
                    other_source_module = other_entity.__module__
                    logger.info(
                        "An node with the same name was found in two separate modules!\n"
                        " - Node name: %s\n - Found in modules: '%s' and '%s'\n"
                        "They both are going to be loaded, but you will need to use a namespace "
                        "path (%s.%s and %s.%s respectively) in saved Pipelines.",
                        entity.__canals_node__,
                        other_source_module,
                        module,
                        other_source_module,
                        entity.__canals_node__,
                        module,
                        entity.__canals_node__,
                    )
                    # Add both nodes as namespaced
                    node_classes[f"{other_source_module}.{entity.__canals_node__}"] = other_entity
                    node_classes[f"{module}.{entity.__canals_node__}"] = entity
                    # Do not remove the non-namespaced one, so in the case of a third collision
                    # it gets detected properly
                    duplicate_names.append(entity.__canals_node__)

                node_classes[entity.__canals_node__] = entity
                logger.debug(" * Found node: %s", entity)

    # Now delete all remaining duplicates
    for duplicate in duplicate_names:
        del node_classes[duplicate]

    print(node_classes)
    return node_classes
