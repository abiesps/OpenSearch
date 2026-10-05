# Shared EC2 infrastructure for the cold-path benchmarks

`coldpath-infra.yaml` is a CloudFormation template for the shared resources
every benchmark branch uses. Deploy it with `deploy.sh`, which also writes
the stack outputs to `../stack-outputs.json` (or the path given as the
first argument).

```
AWS_PROFILE=coldpath AWS_REGION=us-west-2 ./deploy.sh
```

## What it creates

- VPC `10.42.0.0/16` with a private subnet in `usw2-az2` (all hosts), a
  fallback private subnet in `usw2-az1` (only on capacity errors) and a
  public subnet that holds only the NAT gateway.
- NAT gateway for outbound downloads (corpora, Maven, GitHub, dnf), an S3
  gateway endpoint, and `ssm`, `ssmmessages`, `ec2messages` interface
  endpoints so SSM does not depend on the NAT.
- Security groups with no inbound rule from outside the VPC. Data nodes
  accept 9200 (REST) and 9600 (cache-clear agent) from load generators only.
  EFS accepts 2049 from data nodes only.
- One S3 bucket (SSE-S3, versioning, public access blocked, TLS only).
- One encrypted EFS file system (General Purpose, Elastic throughput) with a
  mount target in the primary subnet.
- IAM roles and instance profiles for data nodes, load generators and the
  builder: `AmazonSSMManagedInstanceCore` plus read/write on the bucket.
  Only the data node role can mount EFS.
- One cluster placement group per benchmark branch.
- Launch templates per host class that fix the AMI (resolved once at stack
  create), instance type, IMDSv2 (`HttpTokens=required`), encrypted EBS,
  no public IP, subnet, security group, instance profile and tags.

The bucket and the EFS file system use `DeletionPolicy: Retain`, and the
stack has termination protection. Nothing in this directory deletes
resources.

## Launching a host

```
aws ec2 run-instances \
  --launch-template LaunchTemplateId=<id>,Version=<n> \
  --placement GroupName=<branch placement group> \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=<name>},{Key=branch,Value=<branch>},{Key=project,Value=coldpath-poc},{Key=owner,Value=spsinght}]'
```

Template ids, versions and placement group names are in
`stack-outputs.json`. Repeat `project` and `owner` in any launch-time tag
specification so the tags never depend on how EC2 combines launch-time
and template tags. Check the tags after launch.
