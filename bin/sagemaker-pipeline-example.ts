import { SagemakerPipelineStack } from '../lib/sagemaker-pipeline-stack';
import { App, Tags } from "aws-cdk-lib";
import { NetworkStack } from "../lib/network-stack";
import { StorageStack } from "../lib/storage-stack";

const app = new App();
const deployEnv = {
  projectPrefix: 'ml-pipeline',
};

Tags.of(app).add('Environment', 'Development');
Tags.of(app).add('Project', 'ML-Pipeline');

const networkStack = new NetworkStack(
  app, 'NetworkStack', { ...deployEnv },
);

const storageStack = new StorageStack(
  app, 'StorageStack', { ...deployEnv },
);

const sageMakerPipelineStack = new SagemakerPipelineStack(
  app, 'SagemakerPipelineStack', {
    ...deployEnv,
    vpc: networkStack.vpc,
    securityGroup: networkStack.sg,
    dataBucket: storageStack.dataBucket,
    dataKey: storageStack.dataKey,
  },
);
sageMakerPipelineStack.addDependency(networkStack);
sageMakerPipelineStack.addDependency(storageStack);
