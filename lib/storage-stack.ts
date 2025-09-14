import { CfnOutput, PhysicalName, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";
import { Key } from "aws-cdk-lib/aws-kms";
import { BlockPublicAccess, Bucket, BucketEncryption, ObjectOwnership } from "aws-cdk-lib/aws-s3";

export interface StorageStackProps extends StackProps {
  readonly projectPrefix: string;
}

export class StorageStack extends Stack {
  public dataKey: Key;
  public logsBucket: Bucket;
  public dataBucket: Bucket;

  constructor(scope: Construct, id: string, props: StorageStackProps) {
    super(scope, id, props);

    this.dataKey = new Key(this, `${ props.projectPrefix }-data-kms`, {
      alias: `${ props.projectPrefix }/data`,
      enableKeyRotation: true,
      description: 'CMK for S3 data encryption',
    });

    this.logsBucket = new Bucket(this, `${ props.projectPrefix }-logs-bucket`, {
      bucketName: PhysicalName.GENERATE_IF_NEEDED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      encryption: BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.DESTROY, // for PoC; RETAIN for prod
      autoDeleteObjects: true, // for PoC; false for prod
      objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
    });

    this.dataBucket = new Bucket(this, `${ props.projectPrefix }-data-bucket`, {
      bucketName: PhysicalName.GENERATE_IF_NEEDED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      encryption: BucketEncryption.KMS,
      encryptionKey: this.dataKey,
      enforceSSL: true,
      serverAccessLogsBucket: this.logsBucket,
      serverAccessLogsPrefix: 's3-access-logs/',
      removalPolicy: RemovalPolicy.DESTROY, // for PoC; RETAIN for prod
      autoDeleteObjects: true, // for PoC; false for prod
      objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
    });

    new CfnOutput(this, 'data-bucket', { value: this.dataBucket.bucketName });
  }
}
