import os
import random

import boto3

s3 = boto3.client("s3")


def generate_row(num_features=3):
    # simple synthetic classification: sum of features > threshold => 1 else 0
    features = [round(random.uniform(0, 1), 4) for _ in range(num_features)]
    label = 1 if sum(features) > (num_features * 0.5) else 0
    return features + [label]


def build_csv(rows=200, num_features=3):
    headers = [f"f{i + 1}" for i in range(num_features)] + ["label"]
    lines = [",".join(headers)]
    for _ in range(rows):
        row = generate_row(num_features)
        lines.append(",".join(str(x) for x in row))
    return "\n".join(lines) + "\n"


def put_object(bucket, key, body):
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def on_event(event, context):
    # Provider framework handles responses; just throw on error for rollback
    request_type = event.get("RequestType")
    props = event.get("ResourceProperties", {})
    bucket = os.environ.get("BUCKET_NAME") or props.get("Bucket")
    raw_prefix = os.environ.get("RAW_PREFIX", "raw/") or props.get("RawPrefix", "raw/")
    rows = int(props.get("Rows", os.environ.get("ROWS", 200)))

    # Keep PhysicalResourceId stable across Create/Update/Delete
    physical_id = event.get("PhysicalResourceId") or f"seed-{bucket}-{raw_prefix.rstrip('/')}"

    if request_type in ["Create", "Update"]:
        csv_body = build_csv(rows=rows)
        key = raw_prefix.rstrip("/") + "/data.csv"
        put_object(bucket, key, csv_body)
        return {
            "PhysicalResourceId": physical_id,
            "Data": {
                "S3Uri": f"s3://{bucket}/{key}",
                "Rows": rows
            }
        }
    elif request_type == "Delete":
        # best-effort delete of the object
        try:
            key = raw_prefix.rstrip("/") + "/data.csv"
            s3.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass
        return {
            "PhysicalResourceId": physical_id,
            "Data": {}
        }
    else:
        # no-op
        return {"PhysicalResourceId": physical_id, "Data": {}}
