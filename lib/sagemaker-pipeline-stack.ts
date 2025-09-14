import { Construct } from 'constructs';
import { CfnOutput, CustomResource, Duration, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { SecurityGroup, Vpc } from "aws-cdk-lib/aws-ec2";
import { Key } from "aws-cdk-lib/aws-kms";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { CfnDomain, CfnPipeline, CfnUserProfile } from "aws-cdk-lib/aws-sagemaker";
import { ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { BucketDeployment, Source } from "aws-cdk-lib/aws-s3-deployment";
import * as path from "node:path";
import { Code, Function, Runtime } from "aws-cdk-lib/aws-lambda";
import { Provider } from "aws-cdk-lib/custom-resources";
import { SnsTopic } from "aws-cdk-lib/aws-events-targets";
import { Rule } from "aws-cdk-lib/aws-events";
import { Topic } from "aws-cdk-lib/aws-sns";

export interface SagemakerPipelineStackProps extends StackProps {
  readonly projectPrefix: string;
  readonly vpc: Vpc;
  readonly securityGroup: SecurityGroup;
  readonly dataBucket: Bucket;
  readonly dataKey: Key;
}

export class SagemakerPipelineStack extends Stack {
  // https://docs.aws.amazon.com/sagemaker/latest/dg-ecr-paths/ecr-us-east-1.html#autogluon-us-east-1
  private readonly TRAINING_IMAGE = '763104351884.dkr.ecr.us-east-1.amazonaws.com/autogluon-training:1.4.0-cpu-py311-ubuntu22.04';
  private readonly INFERENCE_IMAGE = '763104351884.dkr.ecr.us-east-1.amazonaws.com/autogluon-inference:1.4.0-cpu-py311-ubuntu22.04';
  private readonly PRE_PROC_IMAGE = '683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3'
  public readonly studioDomain: CfnDomain;
  public readonly userProfile: CfnUserProfile;

  constructor(scope: Construct, id: string, props: SagemakerPipelineStackProps) {
    super(scope, id, props);

    const domainName = `${ props.projectPrefix }-domain`;
    const sagemakerExecutionRole = new Role(this, `${ props.projectPrefix }-studio-exec-role`, {
      assumedBy: new ServicePrincipal('sagemaker.amazonaws.com'),
      roleName: `${ props.projectPrefix }-studio-exec-role`,
      managedPolicies: [
        // for poc: granular least-privilege for prod
        ManagedPolicy.fromAwsManagedPolicyName('AmazonSageMakerFullAccess'),
      ],
    });

    this.studioDomain = new CfnDomain(this, domainName, {
      domainName,
      authMode: 'IAM',
      appNetworkAccessType: 'VpcOnly',
      vpcId: props.vpc.vpcId,
      subnetIds: props.vpc.isolatedSubnets.map((s) => s.subnetId),
      defaultUserSettings: {
        securityGroups: [ props.securityGroup.securityGroupId ],
        executionRole: sagemakerExecutionRole.roleArn,
        jupyterServerAppSettings: {},
        kernelGatewayAppSettings: {},
      },
      kmsKeyId: props.dataKey.keyArn,
    });

    this.userProfile = new CfnUserProfile(this, `${ props.projectPrefix }-user-weirdion`, {
      domainId: this.studioDomain.attrDomainId,
      userProfileName: 'weirdion',
      userSettings: {
        securityGroups: [ props.securityGroup.securityGroupId ],
      },
    });
    this.userProfile.addDependency(this.studioDomain);

    const sagemakerJobRole = new Role(this, `${ props.projectPrefix }-sm-job-role`, {
      roleName: `${ props.projectPrefix }-sm-job-role`,
      assumedBy: new ServicePrincipal('sagemaker.amazonaws.com'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('AmazonEC2ContainerRegistryReadOnly')
      ]
    });
    sagemakerJobRole.addToPolicy(new PolicyStatement({
      actions: [
        'logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents',
        'ecr:GetAuthorizationToken'
      ],
      resources: [ '*' ],
    }));

    props.dataBucket.grantReadWrite(sagemakerJobRole);
    props.dataKey.grantEncryptDecrypt(sagemakerJobRole);


    const pipelineRole = new Role(this, `${ props.projectPrefix }-pipeline-role`, {
      roleName: `${ props.projectPrefix }-pipeline-role`,
      assumedBy: new ServicePrincipal('sagemaker.amazonaws.com'),
    });
    pipelineRole.addToPolicy(new PolicyStatement({
      actions: [
        'sagemaker:*',
        'iam:PassRole',
        's3:*',
        'kms:Decrypt', 'kms:Encrypt', 'kms:GenerateDataKey*', 'kms:DescribeKey',
        'logs:*',
        'ecr:GetAuthorizationToken', 'ecr:BatchCheckLayerAvailability', 'ecr:GetDownloadUrlForLayer', 'ecr:BatchGetImage'
      ],
      resources: [ '*' ],
    }));
    pipelineRole.addToPolicy(new PolicyStatement({
      actions: [ 'iam:PassRole' ],
      resources: [ sagemakerJobRole.roleArn ],
    }));

    const codePrefix = 'code/';
    new BucketDeployment(this, `${ props.projectPrefix }-code-deploy`, {
      destinationBucket: props.dataBucket,
      destinationKeyPrefix: codePrefix,
      sources: [ Source.asset(path.join(__dirname, '..', 'resources', 'scripts')) ],
      prune: false,
    });

    // Data seeding Lambda + custom resource to place mock CSV
    const seedFn = new Function(this, `${ props.projectPrefix }-seed-data-fn`, {
      functionName: `${ props.projectPrefix }-seed-data`,
      runtime: Runtime.PYTHON_3_12,
      handler: 'index.on_event',
      code: Code.fromAsset(path.join(__dirname, '..', 'resources', 'lambda', 'data_seed')),
      timeout: Duration.seconds(60),
      memorySize: 256,
      environment: {
        BUCKET_NAME: props.dataBucket.bucketName,
        RAW_PREFIX: 'raw/',
      },
      vpc: props.vpc,
      securityGroups: [ props.securityGroup ],
      vpcSubnets: { subnets: props.vpc.isolatedSubnets },
    });
    props.dataBucket.grantReadWrite(seedFn);

    const provider = new Provider(this, `${ props.projectPrefix }-seed-provider`, {
      onEventHandler: seedFn,
      logGroup: new LogGroup(this, `${ props.projectPrefix }-seed-logs`, {
        retention: RetentionDays.ONE_WEEK,
        removalPolicy: RemovalPolicy.DESTROY,
      }),
    });
    new CustomResource(this, `${ props.projectPrefix }-seed-resource`, {
      serviceToken: provider.serviceToken,
      properties: {
        Bucket: props.dataBucket.bucketName,
        RawPrefix: 'raw/',
        Rows: 200,
      },
    });

    // SageMaker Pipeline definition (Processing + Training + Deploy minimal)
    const pipelineName = `${ props.projectPrefix }-tabular-classification`;
    const roleArn = pipelineRole.roleArn;
    const bucketName = props.dataBucket.bucketName;

    const pipelineDefinition = {
      Version: '2020-12-01',
      Parameters: [
        { Name: 'InputDataUri', DefaultValue: `s3://${ bucketName }/raw/data.csv` },
        { Name: 'ProcessedPrefix', DefaultValue: `s3://${ bucketName }/processed/` },
        { Name: 'ModelPrefix', DefaultValue: `s3://${ bucketName }/models/` },
        { Name: 'InstanceType', DefaultValue: 'ml.m5.large' },
      ],
      PipelineExperimentConfig: {
        ExperimentName: pipelineName,
        TrialName: 'trial-1',
      },
      Steps: [
        {
          Name: 'Preprocess',
          Type: 'Processing',
          Arguments: {
            ProcessingResources: {
              ClusterConfig: { InstanceCount: 1, InstanceType: 'ml.m5.large', VolumeSizeInGB: 30 },
            },
            AppSpecification: {
              ImageUri: this.PRE_PROC_IMAGE,
              ContainerArguments: [],
              ContainerEntrypoint: [ 'python', '/opt/ml/processing/input/code/preprocessing.py' ],
            },
            RoleArn: sagemakerJobRole.roleArn,
            ProcessingInputs: [
              {
                InputName: 'code',
                AppManaged: false,
                S3Input: {
                  S3Uri: `s3://${ bucketName }/${ codePrefix }`,
                  LocalPath: '/opt/ml/processing/input/code',
                  S3DataType: 'S3Prefix',
                  S3InputMode: 'File',
                },
              },
              {
                InputName: 'raw',
                S3Input: {
                  S3Uri: { 'Get': 'Parameters.InputDataUri' },
                  LocalPath: '/opt/ml/processing/input/raw',
                  S3DataType: 'S3Prefix',
                  S3InputMode: 'File',
                },
              },
            ],
            ProcessingOutputConfig: {
              Outputs: [
                {
                  OutputName: 'train',
                  S3Output: {
                    S3Uri: { 'Get': 'Parameters.ProcessedPrefix' },
                    LocalPath: '/opt/ml/processing/output/train',
                    S3UploadMode: 'EndOfJob',
                  },
                },
                {
                  OutputName: 'test',
                  S3Output: {
                    S3Uri: { 'Get': 'Parameters.ProcessedPrefix' },
                    LocalPath: '/opt/ml/processing/output/test',
                    S3UploadMode: 'EndOfJob',
                  },
                },
              ],
            },
          },
        },
        {
          Name: 'Train',
          Type: 'Training',
          Arguments: {
            AlgorithmSpecification: {
              TrainingImage: this.TRAINING_IMAGE,
              TrainingInputMode: 'File',
            },
            InputDataConfig: [
              {
                ChannelName: 'train',
                DataSource: {
                  S3DataSource: {
                    S3DataType: 'S3Prefix',
                    S3Uri: { 'Concat': [ { 'Get': 'Parameters.ProcessedPrefix' }, 'train/' ] },
                    S3DataDistributionType: 'FullyReplicated',
                  },
                },
                ContentType: 'text/csv',
              },
              {
                ChannelName: 'test',
                DataSource: {
                  S3DataSource: {
                    S3DataType: 'S3Prefix',
                    S3Uri: { 'Concat': [ { 'Get': 'Parameters.ProcessedPrefix' }, 'test/' ] },
                    S3DataDistributionType: 'FullyReplicated',
                  },
                },
                ContentType: 'text/csv',
              },
            ],
            OutputDataConfig: {
              S3OutputPath: { 'Get': 'Parameters.ModelPrefix' },
            },
            ResourceConfig: {
              InstanceCount: 1,
              InstanceType: { 'Get': 'Parameters.InstanceType' },
              VolumeSizeInGB: 50,
            },
            RoleArn: sagemakerJobRole.roleArn,
            StoppingCondition: { MaxRuntimeInSeconds: 3600 },
          },
        },
        {
          Name: 'CreateModel',
          Type: 'Model',
          Arguments: {
            ExecutionRoleArn: sagemakerJobRole.roleArn,
            PrimaryContainer: {
              Image: this.INFERENCE_IMAGE,
              ModelDataUrl: { 'Get': 'Steps.Train.ModelArtifacts.S3ModelArtifacts' },
              Environment: {},
            },
          },
        },
        {
          Name: 'CreateEndpointConfig',
          Type: 'EndpointConfig',
          Arguments: {
            ProductionVariants: [
              {
                ModelName: { 'Get': 'Steps.CreateModel.ModelName' },
                InitialInstanceCount: 1,
                InstanceType: { 'Get': 'Parameters.InstanceType' },
                VariantName: 'AllTraffic',
              },
            ],
          },
        },
        {
          Name: 'CreateEndpoint',
          Type: 'Endpoint',
          Arguments: {
            EndpointConfigName: { 'Get': 'Steps.CreateEndpointConfig.EndpointConfigName' },
            EndpointName: `${ props.projectPrefix }-endpoint`,
          },
        },
      ],
    };

    const pipeline = new CfnPipeline(this, `${ props.projectPrefix }-pipeline`, {
      pipelineName,
      roleArn,
      pipelineDefinition: JSON.stringify(pipelineDefinition),
    });

    // EventBridge rule for pipeline failure -> SNS
    const topic = new Topic(this, `${ props.projectPrefix }-alerts`, {
      topicName: `${ props.projectPrefix }-alerts`,
    });

    const rule = new Rule(this, `${ props.projectPrefix }-pipeline-fail-rule`, {
      eventPattern: {
        source: [ 'aws.sagemaker' ],
        detailType: [ 'SageMaker Model Building Pipeline Execution Status Change' ],
        detail: {
          currentPipelineExecutionStatus: [ 'Failed' ],
          pipelineName: [ pipelineName ],
        },
      },
    });
    rule.addTarget(new SnsTopic(topic));

    new CfnOutput(this, 'StudioDomainId', { value: this.studioDomain.attrDomainId });
    new CfnOutput(this, 'UserProfileName', { value: this.userProfile.userProfileName });
    new CfnOutput(this, 'PipelineName', { value: pipeline.pipelineName! });
    new CfnOutput(this, 'AlertsTopicArn', { value: topic.topicArn });
  }
}
