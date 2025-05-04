import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from urllib3.exceptions import InsecureRequestWarning
import urllib3

# ——— Setup once at module load ———
# Suppress the MinIO insecure-SSL warnings
urllib3.disable_warnings(InsecureRequestWarning)

S3_CLIENT = boto3.session.Session().client(
    service_name="s3",
    endpoint_url="https://play.min.io:9000",
    aws_access_key_id="Q3AM3UQ867SPQQA43P2F",
    aws_secret_access_key="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    config=boto3.session.Config(signature_version="s3v4"),
    verify=False,
)

def ensure_bucket(bucket_name: str) -> None:
    """Create the bucket if it doesn’t already exist."""
    try:
        S3_CLIENT.head_bucket(Bucket=bucket_name)
    except ClientError as err:
        # 404 means “Not Found,” so create it; otherwise re-raise
        code = int(err.response["Error"]["Code"])
        if code == 404:
            S3_CLIENT.create_bucket(Bucket=bucket_name)
        else:
            raise

def upload_file(
    file_path: str,
    bucket_name: str,
    object_name: str = None
) -> bool:
    """
    Upload a file to S3. Returns True on success, False on error.
    """
    if object_name is None:
        object_name = os.path.basename(file_path)

    try:
        S3_CLIENT.upload_file(file_path, bucket_name, object_name)
        return True
    except (ClientError, NoCredentialsError) as e:
        print(f" Upload failed for {file_path!r}: {e}")
        return False

def main():
    files = [
        "cache/survey_combined.csv",
        "cache/annual_salary_adjusted_by_location_and_age.csv",
        "cache/annual_salary_adjusted_by_location_and_education.csv",
    ]
    bucket = "ist356mafudge"

    # make sure the bucket exists just once
    ensure_bucket(bucket)

    # upload each file
    for path in files:
        success = upload_file(path, bucket)
        status = "✅" if success else "❌"
        print(f"{status} {os.path.basename(path)} → s3://{bucket}/{os.path.basename(path)}")

if __name__ == "__main__":
    main()
