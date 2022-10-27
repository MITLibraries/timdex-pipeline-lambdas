import os

import boto3
import pytest
from moto import mock_s3


@pytest.fixture(autouse=True)
def test_env():
    os.environ = {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "TIMDEX_ALMA_EXPORT_BUCKET_ID": "test-alma-bucket",
        "TIMDEX_S3_EXTRACT_BUCKET_ID": "test-timdex-bucket",
        "WORKSPACE": "test",
    }


@pytest.fixture(autouse=True)
def mocked_s3():
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-timdex-bucket")
        client.create_bucket(Bucket="test-alma-bucket")
        with open(
            "tests/fixtures/TIMDEX_ALMA_EXPORT_DAILY_20220912_210929[053]_delete.tar.gz",
            "rb",
        ) as file:
            client.put_object(
                Bucket="test-alma-bucket",
                Key="exlibris/timdex/TIMDEX_ALMA_EXPORT_DAILY_20220912_210929[053]_delete"
                ".tar.gz",
                Body=file,
            )
        with open(
            "tests/fixtures/TIMDEX_ALMA_EXPORT_DAILY_20220912_210929[053]_new_1.tar.gz",
            "rb",
        ) as file:
            client.put_object(
                Bucket="test-alma-bucket",
                Key="exlibris/timdex/TIMDEX_ALMA_EXPORT_DAILY_20220912_210929[053]_new_1"
                ".tar.gz",
                Body=file,
            )
        with open(
            "tests/fixtures/TIMDEX_ALMA_EXPORT_DAILY_20220912_210929[053]_new_2.tar.gz",
            "rb",
        ) as file:
            client.put_object(
                Bucket="test-alma-bucket",
                Key="exlibris/timdex/TIMDEX_ALMA_EXPORT_DAILY_20220912_210929[053]_new_2"
                ".tar.gz",
                Body=file,
            )
        yield client


@pytest.fixture()
def s3_client():
    yield boto3.client("s3")
