import { CfnOutput, Stack, StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";
import {
  GatewayVpcEndpointAwsService,
  InterfaceVpcEndpointAwsService,
  SecurityGroup,
  SubnetType,
  Vpc
} from "aws-cdk-lib/aws-ec2";

export interface NetworkStackProps extends StackProps {
  readonly projectPrefix: string;
}

export class NetworkStack extends Stack {
  public readonly vpc: Vpc;
  public readonly sg: SecurityGroup;

  constructor(scope: Construct, id: string, props: NetworkStackProps) {
    super(scope, id, props);

    this.vpc = new Vpc(this, `${ props.projectPrefix }-vpc`, {
      vpcName: `${ props.projectPrefix }-vpc`,
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [
        {
          name: 'private-isolated',
          subnetType: SubnetType.PRIVATE_ISOLATED,
        },
      ],
    });

    this.sg = new SecurityGroup(this, `${ props.projectPrefix }-sg`, {
      vpc: this.vpc,
      description: 'SecurityGroup for SageMaker Studio and Jobs',
      allowAllOutbound: true,
      securityGroupName: `${ props.projectPrefix }-sg`,
    });

    this.vpc.addGatewayEndpoint(`${ props.projectPrefix }-s3-endpoint`, {
      service: GatewayVpcEndpointAwsService.S3,
      subnets: [ { subnets: this.vpc.isolatedSubnets } ],
    });

    const interfaceServices: [ string, InterfaceVpcEndpointAwsService ][] = [
      [ 'ecr-dkr', InterfaceVpcEndpointAwsService.ECR_DOCKER ],
      [ 'ecr-api', InterfaceVpcEndpointAwsService.ECR ],
      [ 'logs', InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS ],
      [ 'sts', InterfaceVpcEndpointAwsService.STS ],
      [ 'sagemaker-api', InterfaceVpcEndpointAwsService.SAGEMAKER_API ],
      [ 'sagemaker-runtime', InterfaceVpcEndpointAwsService.SAGEMAKER_RUNTIME ],
      [ 'sagemaker-studio', InterfaceVpcEndpointAwsService.SAGEMAKER_STUDIO ],
    ];
    for (const [ name, service ] of interfaceServices) {
      this.vpc.addInterfaceEndpoint(`${ props.projectPrefix }-${ name }-endpoint`, {
        service,
        securityGroups: [ this.sg ],
        subnets: { subnets: this.vpc.isolatedSubnets },
        privateDnsEnabled: true,
      });
    }

    new CfnOutput(this, 'vpc-id', { value: this.vpc.vpcId });
  }
}
