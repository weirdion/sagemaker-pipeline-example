import json
import logging
import traceback
import hashlib

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


def _build_definition(cfg: dict) -> dict:
    """Construct a deterministic SageMaker Pipeline definition from config.
    Required cfg keys:
      - pipelineName, pipelineRoleArn, jobRoleArn
      - bucketName, rawPrefix, processedPrefix, modelPrefix, codePrefix
      - processingImageUri, trainingImageUri, inferenceImageUri
      - instanceType, endpointName
    """

    bucket = cfg["bucketName"]
    raw_uri = f"s3://{bucket}/{cfg['rawPrefix'].rstrip('/')}/data.csv"
    processed_prefix = f"s3://{bucket}/{cfg['processedPrefix'].rstrip('/')}/"
    model_prefix = f"s3://{bucket}/{cfg['modelPrefix'].rstrip('/')}/"
    code_s3 = f"s3://{bucket}/{cfg['codePrefix'].rstrip('/')}/"

    definition = {
        "Version": "2020-12-01",
        "Parameters": [
            {"Name": "InputDataUri", "DefaultValue": raw_uri},
            {"Name": "ProcessedPrefix", "DefaultValue": processed_prefix},
            {"Name": "ModelPrefix", "DefaultValue": model_prefix},
            {"Name": "InstanceType", "DefaultValue": cfg["instanceType"]},
        ],
        "PipelineExperimentConfig": {
            "ExperimentName": cfg["pipelineName"],
            "TrialName": "trial-1",
        },
        "Steps": [
            {
                "Name": "Preprocess",
                "Type": "Processing",
                "Arguments": {
                    "ProcessingResources": {
                        "ClusterConfig": {
                            "InstanceCount": 1,
                            "InstanceType": "ml.m5.large",
                            "VolumeSizeInGB": 30,
                        }
                    },
                    "AppSpecification": {
                        "ImageUri": cfg["processingImageUri"],
                        "ContainerEntrypoint": [
                            "python",
                            "/opt/ml/processing/input/code/preprocessing.py",
                        ],
                    },
                    "RoleArn": cfg["jobRoleArn"],
                    "ProcessingInputs": [
                        {
                            "InputName": "code",
                            "S3Input": {
                                "S3Uri": code_s3,
                                "LocalPath": "/opt/ml/processing/input/code",
                                "S3DataType": "S3Prefix",
                                "S3InputMode": "File",
                            },
                        },
                        {
                            "InputName": "raw",
                            "S3Input": {
                                "S3Uri": {"Get": "Parameters.InputDataUri"},
                                "LocalPath": "/opt/ml/processing/input/raw",
                                "S3DataType": "S3Prefix",
                                "S3InputMode": "File",
                            },
                        },
                    ],
                    "ProcessingOutputConfig": {
                        "Outputs": [
                            {
                                "OutputName": "train",
                                "S3Output": {
                                    "S3Uri": {"Get": "Parameters.ProcessedPrefix"},
                                    "LocalPath": "/opt/ml/processing/output/train",
                                    "S3UploadMode": "EndOfJob",
                                },
                            },
                            {
                                "OutputName": "test",
                                "S3Output": {
                                    "S3Uri": {"Get": "Parameters.ProcessedPrefix"},
                                    "LocalPath": "/opt/ml/processing/output/test",
                                    "S3UploadMode": "EndOfJob",
                                },
                            },
                        ]
                    },
                },
            },
            {
                "Name": "Train",
                "Type": "Training",
                "Arguments": {
                    "AlgorithmSpecification": {
                        "TrainingImage": cfg["trainingImageUri"],
                        "TrainingInputMode": "File",
                    },
                    "InputDataConfig": [
                        {
                            "ChannelName": "train",
                            "DataSource": {
                                "S3DataSource": {
                                    "S3DataType": "S3Prefix",
                                    "S3Uri": f"s3://{bucket}/{cfg['processedPrefix'].rstrip('/')}/train/",
                                    "S3DataDistributionType": "FullyReplicated",
                                }
                            },
                            "ContentType": "text/csv",
                        },
                        {
                            "ChannelName": "test",
                            "DataSource": {
                                "S3DataSource": {
                                    "S3DataType": "S3Prefix",
                                    "S3Uri": f"s3://{bucket}/{cfg['processedPrefix'].rstrip('/')}/test/",
                                    "S3DataDistributionType": "FullyReplicated",
                                }
                            },
                            "ContentType": "text/csv",
                        },
                    ],
                    "OutputDataConfig": {"S3OutputPath": {"Get": "Parameters.ModelPrefix"}},
                    "ResourceConfig": {
                        "InstanceCount": 1,
                        "InstanceType": {"Get": "Parameters.InstanceType"},
                        "VolumeSizeInGB": 50,
                    },
                    "RoleArn": cfg["jobRoleArn"],
                    "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
                },
            },
            {
                "Name": "CreateModel",
                "Type": "Model",
                "Arguments": {
                    "ExecutionRoleArn": cfg["jobRoleArn"],
                    "PrimaryContainer": {
                        "Image": cfg["inferenceImageUri"],
                        "ModelDataUrl": {"Get": "Steps.Train.ModelArtifacts.S3ModelArtifacts"},
                        "Environment": {},
                    },
                },
            },
            {
                "Name": "CreateEndpointConfig",
                "Type": "EndpointConfig",
                "Arguments": {
                    "ProductionVariants": [
                        {
                            "ModelName": {"Get": "Steps.CreateModel.ModelName"},
                            "InitialInstanceCount": 1,
                            "InstanceType": {"Get": "Parameters.InstanceType"},
                            "VariantName": "AllTraffic",
                        }
                    ]
                },
            },
            {
                "Name": "CreateEndpoint",
                "Type": "Endpoint",
                "Arguments": {
                    "EndpointConfigName": {"Get": "Steps.CreateEndpointConfig.EndpointConfigName"},
                    "EndpointName": cfg["endpointName"],
                },
            },
        ],
    }

    return definition


def on_event(event, context):
    logger.info(json.dumps(event))
    req_type = event.get("RequestType")
    props = event.get("ResourceProperties", {})
    name = props["PipelineName"]
    role_arn = props["PipelineRoleArn"]
    generator_version = props.get("GeneratorVersion", "v1")

    # Extract config and build definition deterministically
    cfg = {
        "pipelineName": name,
        "pipelineRoleArn": role_arn,
        "jobRoleArn": props["JobRoleArn"],
        "bucketName": props["BucketName"],
        "rawPrefix": props.get("RawPrefix", "raw/"),
        "processedPrefix": props.get("ProcessedPrefix", "processed/"),
        "modelPrefix": props.get("ModelPrefix", "models/"),
        "codePrefix": props.get("CodePrefix", "code/"),
        "processingImageUri": props["ProcessingImageUri"],
        "trainingImageUri": props["TrainingImageUri"],
        "inferenceImageUri": props["InferenceImageUri"],
        "instanceType": props.get("InstanceType", "ml.m5.large"),
        "endpointName": props["EndpointName"],
        "generatorVersion": generator_version,
    }

    definition_obj = _build_definition(cfg)
    definition_str = json.dumps(definition_obj, separators=(",", ":"), sort_keys=True)
    definition_hash = hashlib.sha256(definition_str.encode("utf-8")).hexdigest()

    physical_id = f"sagemaker-pipeline-{name}"

    try:
        if req_type in ("Create", "Update"):
            _create_or_update(name, role_arn, definition_str)
            return _response(physical_id, {"PipelineName": name, "DefinitionHash": definition_hash})
        elif req_type == "Delete":
            _delete(name)
            return _response(physical_id, {"PipelineName": name})
        else:
            return _response(physical_id, {"PipelineName": name})
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())
        raise
