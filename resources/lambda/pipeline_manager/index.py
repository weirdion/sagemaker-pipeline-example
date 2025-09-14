import json
import logging
import traceback

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sm = boto3.client("sagemaker")


def _response(physical_id: str, data: dict | None = None):
    return {
        "PhysicalResourceId": physical_id,
        "Data": data or {},
    }


def _pipeline_exists(name: str) -> bool:
    try:
        sm.describe_pipeline(PipelineName=name)
        return True
    except sm.exceptions.ResourceNotFound:
        return False


def _create_or_update(name: str, role_arn: str, definition_body: str):
    # Prefer update if exists; otherwise create
    if _pipeline_exists(name):
        logger.info(f"Updating pipeline {name}")
        sm.update_pipeline(
            PipelineName=name,
            RoleArn=role_arn,
            PipelineDefinition=definition_body,
        )
    else:
        logger.info(f"Creating pipeline {name}")
        sm.create_pipeline(
            PipelineName=name,
            RoleArn=role_arn,
            PipelineDefinition=definition_body,
        )


def _delete(name: str):
    try:
        sm.delete_pipeline(PipelineName=name)
    except sm.exceptions.ResourceNotFound:
        pass


def on_event(event, context):
    logger.info(json.dumps(event))
    req_type = event.get("RequestType")
    props = event.get("ResourceProperties", {})
    name = props["PipelineName"]
    role_arn = props["RoleArn"]
    definition_body = props.get("PipelineDefinitionBody")

    physical_id = f"sagemaker-pipeline-{name}"

    try:
        if req_type in ("Create", "Update"):
            if not definition_body or not isinstance(definition_body, str):
                raise ValueError("PipelineDefinitionBody must be a JSON string")
            # Validate JSON parses
            json.loads(definition_body)
            _create_or_update(name, role_arn, definition_body)
            return _response(physical_id, {"PipelineName": name})
        elif req_type == "Delete":
            _delete(name)
            return _response(physical_id, {"PipelineName": name})
        else:
            return _response(physical_id, {"PipelineName": name})
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())
        raise
