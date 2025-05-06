import {
  AccountPrincipal,
  ArnPrincipal,
  IPrincipal,
  Policy,
  PolicyDocument,
  Role,
  ServicePrincipal
} from 'aws-cdk-lib/aws-iam';

import { Construct } from 'constructs';

interface IamProps {
  readonly name: string;
  readonly inlinePolicyDocument?: PolicyDocument;
  readonly assumedByServicePrincipalName?: string;
  readonly assumedByPrincipalAccount?: string;
  readonly assumedByRole?: string;
}

export class IamRoleResource extends Construct {
  public readonly iamRole: Role;

  constructor(scope: Construct, id: string, props: IamProps) {
    super(scope, id);

    let assumedBy: IPrincipal;
    if (props.assumedByServicePrincipalName) {
      assumedBy = new ServicePrincipal(props.assumedByServicePrincipalName);
    } else if (props.assumedByPrincipalAccount) {
      assumedBy = new AccountPrincipal(props.assumedByPrincipalAccount);
    } else if (props.assumedByRole) {
      assumedBy = new ArnPrincipal(props.assumedByRole);
    } else {
      assumedBy = new ServicePrincipal('ec2.amazonaws.com');
    }

    this.iamRole = new Role(this, props.name, {
      roleName: props.name,
      assumedBy: assumedBy,
    });

    if (props.inlinePolicyDocument) {
      this.iamRole.attachInlinePolicy(
        new Policy(this, `${props.name}-inline-policy`, {
          document: props.inlinePolicyDocument,
        }),
      );
    }
  }
}
