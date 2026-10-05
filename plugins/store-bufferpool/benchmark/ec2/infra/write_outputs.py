#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
"""Write the coldpath-poc-infra stack outputs as structured JSON.

Later benchmark steps read this file to launch hosts into the shared
network. Uses the AWS CLI (profile and region from the environment).
"""
import argparse
import json
import subprocess
from datetime import datetime, timezone


def aws(*args):
    return json.loads(subprocess.check_output(["aws", *args, "--output", "json"]))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stack", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args()

    stack = aws("cloudformation", "describe-stacks", "--stack-name", a.stack)["Stacks"][0]
    o = {x["OutputKey"]: x["OutputValue"] for x in stack.get("Outputs", [])}

    ami = aws("ec2", "describe-images", "--image-ids", o["AmiId"])["Images"][0]

    doc = {
        "written_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stack_name": a.stack,
        "stack_id": stack["StackId"],
        "stack_status": stack["StackStatus"],
        "account_id": o["AccountId"],
        "region": o["Region"],
        "vpc": {
            "id": o["VpcId"],
            "cidr": o["VpcCidr"],
            "internet_gateway_id": o["InternetGatewayId"],
            "nat_gateway_id": o["NatGatewayId"],
            "nat_eip_allocation_id": o["NatEipAllocationId"],
            "private_route_table_id": o["PrivateRouteTableId"],
        },
        "subnets": {
            "private_primary": {"id": o["PrivateSubnetAId"], "az": o["PrivateSubnetAAz"]},
            "private_fallback": {"id": o["PrivateSubnetBId"], "az": o["PrivateSubnetBAz"]},
            "public_nat_only": {"id": o["PublicSubnetNatId"]},
        },
        "private_subnet_ids": [o["PrivateSubnetAId"], o["PrivateSubnetBId"]],
        "vpc_endpoints": {
            "s3_gateway": o["S3GatewayEndpointId"],
            "ssm": o["SsmEndpointId"],
            "ssmmessages": o["SsmMessagesEndpointId"],
            "ec2messages": o["Ec2MessagesEndpointId"],
        },
        "bucket": {"name": o["BucketName"], "arn": o["BucketArn"]},
        "efs": {"file_system_id": o["EfsFileSystemId"], "mount_target_primary": o["EfsMountTargetAId"]},
        "security_groups": {
            "datanode": o["DataNodeSecurityGroupId"],
            "loadgen": o["LoadGenSecurityGroupId"],
            "builder": o["BuilderSecurityGroupId"],
            "endpoints": o["EndpointSecurityGroupId"],
            "efs": o["EfsSecurityGroupId"],
        },
        "iam": {
            "bucket_access_policy_arn": o["BucketAccessPolicyArn"],
            "datanode": {
                "role_arn": o["DataNodeRoleArn"],
                "instance_profile_name": o["DataNodeInstanceProfileName"],
                "instance_profile_arn": o["DataNodeInstanceProfileArn"],
            },
            "loadgen": {
                "role_arn": o["LoadGenRoleArn"],
                "instance_profile_name": o["LoadGenInstanceProfileName"],
                "instance_profile_arn": o["LoadGenInstanceProfileArn"],
            },
            "builder": {
                "role_arn": o["BuilderRoleArn"],
                "instance_profile_name": o["BuilderInstanceProfileName"],
                "instance_profile_arn": o["BuilderInstanceProfileArn"],
            },
        },
        "placement_groups": {
            "big5-a": o["PlacementGroupBig5A"],
            "big5-b": o["PlacementGroupBig5B"],
            "big5-c": o["PlacementGroupBig5C"],
            "nyc_taxis": o["PlacementGroupNycTaxis"],
            "http_logs": o["PlacementGroupHttpLogs"],
            "pmc": o["PlacementGroupPmc"],
            "storage-efs": o["PlacementGroupStorageEfs"],
            "big5-1000gb": o["PlacementGroupBig51000Gb"],
        },
        "launch_templates": {
            "datanode": {"id": o["DataNodeLaunchTemplateId"], "version": o["DataNodeLaunchTemplateVersion"]},
            "loadgen": {"id": o["LoadGenLaunchTemplateId"], "version": o["LoadGenLaunchTemplateVersion"]},
            "builder": {"id": o["BuilderLaunchTemplateId"], "version": o["BuilderLaunchTemplateVersion"]},
        },
        "ami": {
            "id": ami["ImageId"],
            "name": ami["Name"],
            "creation_date": ami["CreationDate"],
            "architecture": ami["Architecture"],
        },
    }
    with open(a.out, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
