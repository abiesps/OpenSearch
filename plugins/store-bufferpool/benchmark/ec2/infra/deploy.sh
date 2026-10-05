#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Deploy (create or update) the shared cold-path benchmark stack and write
# its outputs as JSON. Never deletes anything: stateful resources carry
# DeletionPolicy: Retain, and this script has no delete path.
#
# Usage: deploy.sh [stack-outputs.json path]
# Env:   AWS_PROFILE (default coldpath), AWS_REGION (default us-west-2),
#        STACK_NAME (default coldpath-poc-infra)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
export AWS_PROFILE="${AWS_PROFILE:-coldpath}"
export AWS_REGION="${AWS_REGION:-us-west-2}"
stack="${STACK_NAME:-coldpath-poc-infra}"
out="${1:-$here/../stack-outputs.json}"

aws cloudformation validate-template \
  --template-body "file://$here/coldpath-infra.yaml" >/dev/null

aws cloudformation deploy \
  --stack-name "$stack" \
  --template-file "$here/coldpath-infra.yaml" \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags project=coldpath-poc owner=spsinght \
  --no-fail-on-empty-changeset

# Termination protection on the stack itself (it only blocks deletes).
aws cloudformation update-termination-protection \
  --stack-name "$stack" --enable-termination-protection >/dev/null

python3 "$here/write_outputs.py" --stack "$stack" --out "$out"
echo "outputs written to $out"
