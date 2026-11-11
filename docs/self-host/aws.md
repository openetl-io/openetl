---
title: Run on AWS
description: Give OpenETL's runners and connectors AWS access through IAM roles on EC2 and EKS, and see what a connector can and cannot get from the runner.
---

This page covers AWS credentials for OpenETL on EC2, with [Docker Compose](./compose.md), and on EKS, with [Kubernetes](./kubernetes.md). It covers two separate identities:

- **The runner's identity.** The runner and the engine use it for the checkpoint bucket, for secrets in AWS Secrets Manager, and for Spark and Flink steps on EMR.
- **A connector's credentials.** A connector uses them for the AWS services in your pipelines, such as Glue, S3 or Kinesis.

Use IAM roles for both. Access keys still work, but a role does not need a long-lived secret.

## The runner's identity

The runner uses the first credentials that it finds, in the order that [Buckets](./index.md#buckets) gives. Keys come before a role.

If the runner cannot get credentials from the source that it finds, it does not start. Its error names that source and what to check.

### On EC2

1. Attach an instance profile to the instance. Its role needs read and write access to the bucket.
2. Set the instance metadata hop limit to 2. A container is one network hop further from the metadata service than the instance. With a hop limit of 1, the runner in its container cannot get the role.

   ```sh
   aws ec2 modify-instance-metadata-options --instance-id INSTANCE_ID --http-tokens required --http-put-response-hop-limit 2
   ```

3. Remove `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from `.env`. Keys come first, so the runner does not use the role while they are set.

### On EKS

EKS gives a pod a role in two ways:

| | IAM roles for service accounts (IRSA) | EKS Pod Identity |
|---|---|---|
| Setup in IAM | An OIDC identity provider for the cluster's issuer, and a trust policy that names the provider and the service account | A trust policy that trusts `pods.eks.amazonaws.com` |
| Setup in the cluster | An annotation on the service account | The `eks-pod-identity-agent` add-on, and an association for each service account |
| AWS Fargate | Supported | Not supported |
| Clusters that are not EKS | Supported, with your own OIDC issuer | Not supported |

Give the role to two service accounts in the `openetl` namespace:

- `default`, which runners, job pods, the server and the orchestrators use.
- `openetl-launcher`, which the pod launcher uses.

With Pod Identity, create an association for each service account:

```sh
aws eks create-pod-identity-association --cluster-name CLUSTER --namespace openetl --service-account default --role-arn RUNNER_ROLE_ARN
aws eks create-pod-identity-association --cluster-name CLUSTER --namespace openetl --service-account openetl-launcher --role-arn RUNNER_ROLE_ARN
```

With IRSA, annotate each service account:

```sh
kubectl -n openetl annotate serviceaccount default eks.amazonaws.com/role-arn=RUNNER_ROLE_ARN
kubectl -n openetl annotate serviceaccount openetl-launcher eks.amazonaws.com/role-arn=RUNNER_ROLE_ARN
```

Restart the pods after either change, so that EKS gives them the role. Keep the nodes' instance metadata hop limit at 1 and require IMDSv2, so that pods cannot use the node's role.

## Credentials for connectors

A connector gets AWS credentials from the runner in one of two ways:

1. **A role for its source or destination**, which the runner assumes for it. Use this way.
2. **The runner's own credentials**, if the runner's `OPENETL_CONNECTOR_AWS_CREDENTIALS` is `inherit`.

Otherwise, the connector gets no AWS credentials from the runner. That is the default, `none`. A connector can also take access keys in its own config, stored as secrets. If a config has keys, the official connectors use them, even if the source or destination names a role.

### Roles for sources and destinations

The runner assumes the role with its own identity. Then it gives the role's temporary credentials to that connector only. It serves them on `127.0.0.1`, in the format that the AWS SDKs read from `AWS_CONTAINER_CREDENTIALS_FULL_URI`. The connector's SDK refreshes the credentials before they expire. The connector gets no other AWS credentials from the runner.

This works on EC2 and EKS, with any runner identity: an instance profile, IRSA, Pod Identity or keys. The role can be in another account. That account does not need an OIDC provider for the cluster.

The runner tags each session, so that a role's trust policy can limit which source or destination may use it:

| Tag | Value |
|---|---|
| `openetl:workspace` | The workspace's name |
| `openetl:source` or `openetl:destination` | The source's or destination's name |
| `openetl:pipeline` | The pipeline's name, during a sync only |

The runner sets the tags from the names that the API gives it, never from a spec. The session's name is `openetl-destination-NAME` or `openetl-source-NAME`, and CloudTrail shows it.

To give a destination named `lake` a role:

1. Let the runners assume the role. Set `OPENETL_AWS_ASSUME_ROLES` on every runner of the data plane: in `.env` with Compose, or in `config.yaml` on Kubernetes. Commas separate entries, and `*` matches any characters. Without this setting, a runner assumes no roles.

   ```sh
   OPENETL_AWS_ASSUME_ROLES=arn:aws:iam::222222222222:role/openetl-*
   ```

2. Allow the runner's role to assume the role and to tag the session. Add this statement to the runner role's permission policy:

   ```json
   {
     "Effect": "Allow",
     "Action": ["sts:AssumeRole", "sts:TagSession"],
     "Resource": "arn:aws:iam::222222222222:role/openetl-*"
   }
   ```

3. Give the role a trust policy that trusts the runner's role, and only for this destination:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": { "AWS": "arn:aws:iam::111111111111:role/openetl-runner" },
         "Action": ["sts:AssumeRole", "sts:TagSession"],
         "Condition": {
           "StringEquals": {
             "aws:RequestTag/openetl:workspace": "default",
             "aws:RequestTag/openetl:destination": "lake"
           }
         }
       }
     ]
   }
   ```

   Give the role's permission policy the access that the connector needs, and no more.

4. Name the role in the destination, under `aws`. This example uses the [S3 connector](https://github.com/openetl-io/openetl-connectors/blob/main/s3/README.md). Its config has no access keys:

   ```yaml
   apiVersion: openetl.io/v1alpha1
   kind: Destination
   metadata:
     name: lake
   spec:
     connector: s3
     config:
       url: s3://my-lake/files
       region: ap-southeast-2
     aws:
       role: arn:aws:iam::222222222222:role/openetl-lake
   ```

   If the trust policy requires an `ExternalId`, add `externalId` under `aws`. [Destination](../reference/specs/destination.md) lists the fields.

5. Apply the destination, then check it:

   ```sh
   openetl apply -f lake.yaml
   openetl check destination lake
   ```

   The check runs on a runner, which assumes the role first. If the check passes, it prints:

   ```text
   workspaces/default/destinations/lake passed its check (version 98fc31616b56)
   ```

   If AWS refuses the role, the check fails and names the runner's identity and the policies to fix.

A connector that runs inside `openetld` cannot take a role. Apply refuses `aws` for a built-in connector, and a runner refuses it for a declarative source.

### The runner's own credentials

If `OPENETL_CONNECTOR_AWS_CREDENTIALS` is `inherit`, each connector gets the runner's `AWS_` variables, and the AWS SDK in the connector can use EC2's instance metadata. A connector then has all of the runner's AWS access, including every pipeline's checkpoints and staging in the bucket.

> [!WARNING]
> With `inherit`, any connector on the runner can read and change every pipeline's checkpoints. A wrong checkpoint can make a pipeline read data again or skip data.

Use `inherit` only if one team owns every pipeline and connector on those runners. A source or destination that names a role still gets only that role.

## What OpenETL can block

A connector runs as the runner's user, on the runner's network and file system. [Connectors are trusted code](./index.md#connectors-are-trusted-code) explains this. OpenETL controls what the AWS SDKs' default credential chain finds in a connector's process. It cannot stop a connector that looks for the runner's credentials on purpose.

| What a connector could use | `none`, the default | `inherit` | A role for its source or destination | A connector that looks on purpose |
|---|---|---|---|---|
| The runner's keys | Blocked | Used | Blocked | May work: the runner's environment is readable |
| The runner's IRSA token | Blocked | Used | Blocked | May work: the token file is readable |
| The runner's Pod Identity | Blocked | Used | Blocked | May work: the token file and the agent are reachable |
| AWS profiles in the runner's home directory | Blocked | Used | Blocked | May work: the files are readable |
| EC2's instance metadata: the instance's or the node's role | Blocked for AWS SDKs; may work for other libraries | Used | Blocked for AWS SDKs; may work for other libraries | May work where the runner's network reaches it |

"Blocked" means that the AWS SDKs' default credential chain does not find the credentials. The runner sets `AWS_EC2_METADATA_DISABLED`, `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` for this. A library that is not an AWS SDK may not read these variables.

On EKS, nodes with a hop limit of 1 and IMDSv2 required keep every pod off the node's role, so the node's role is blocked for runners and connectors alike.

To keep a connector you do not fully trust away from credentials, run it on separate runners whose identity has only the access that its pipelines need.

## Errors

| Error | What to do |
|---|---|
| `no AWS credentials: … in a container on EC2, the instance's role needs an instance metadata hop limit of 2 or more` | Set the hop limit, as in [On EC2](#on-ec2), or give the runner keys. |
| `STS refused the web identity token for the role` | IRSA: check the cluster's OIDC provider in IAM, and the role's trust policy for the service account. |
| `the container credentials endpoint gave no credentials` | Pod Identity: check the association for the service account, and that the agent runs on the node. |
| `aws.role names ROLE, and this runner may assume no roles` | Set `OPENETL_AWS_ASSUME_ROLES` on the runners. |
| `aws.role names ROLE, which OPENETL_AWS_ASSUME_ROLES (…) doesn't list` | Add the role, or a pattern that matches it, to the setting. |
| `assume aws.role ROLE: STS refused it to IDENTITY` | Check the role's trust policy and the runner role's permission policy, as in [Roles for sources and destinations](#roles-for-sources-and-destinations). |
