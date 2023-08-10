# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=protected-access

from typing import Dict, List, Any, Tuple
import json
import logging
from pathlib import Path

from canals.component.component import component
from canals.pipeline.pipeline import Pipeline
from canals.errors import PipelineUnmarshalError


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
    components: List[Tuple[str, str, Any]] = []
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
                    "init_parameters": component_instance.init_parameters,
                }
                pipeline_repr["components"][component_name] = component_repr

        pipeline_repr["connections"] = list(pipeline.graph.edges)
        schema["pipelines"][pipeline_name] = pipeline_repr

    return schema


def unmarshal_pipelines(schema: Dict[str, Any]) -> Dict[str, Pipeline]:  # pylint: disable=too-many-locals
    """
    Loads the content of a schema generated by `marshal_pipelines()` into
    a dictionary of named Pipelines.

    Args:
        schema: the schema of the pipelines, as generated by `marshal_pipelines()`.

    Returns:
        The pipelines as a dictionary of `{"pipeline-name": <pipeline object>}`.

    Raises PipelineUnmarshalError: if any Component class has not been imported before loading.

    """
    pipelines = {}
    component_instances: Dict[str, Any] = {}
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
                class_name = component_schema["type"]
                if class_name not in component.registry:
                    raise PipelineUnmarshalError(
                        f"Failed loading Pipeline '{pipeline_name}'. Can't find Component class '{class_name}'. "
                        "Make sure you imported this class before loading the pipelines."
                    )
                component_class = component.registry[class_name]
                component_instance = component_class(**component_schema.get("init_parameters", {}))
                component_instances[f"{pipeline_name}.{component_name}"] = component_instance
                pipe.add_component(
                    name=component_name,
                    instance=component_instance,
                )

        for connect_from, connect_to, sockets in pipeline_schema["connections"]:
            output_socket, input_socket = sockets.split("/", maxsplit=1)
            connect_from = f"{connect_from}.{output_socket}"
            connect_to = f"{connect_to}.{input_socket}"
            pipe.connect(connect_from=connect_from, connect_to=connect_to)

        pipelines[pipeline_name] = pipe

    return pipelines
