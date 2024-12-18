import boto3
import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("TIMDEX_ALMA_EXPORT_BUCKET_ID", "test-alma-bucket")
    monkeypatch.setenv("TIMDEX_S3_EXTRACT_BUCKET_ID", "test-timdex-bucket")
    monkeypatch.setenv("WORKSPACE", "test")
    # NOTE: FEATURE FLAG: remove after v2 work is complete
    monkeypatch.setenv("ETL_VERSION", "1")


@pytest.fixture(autouse=True)
def mocked_s3():
    with mock_aws():
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


@pytest.fixture
def s3_client():
    # ruff: noqa: PT022
    yield boto3.client("s3")


@pytest.fixture
def run_id():
    return "run-abc-123"


# NOTE: FEATURE FLAG: remove after v2 work is complete
@pytest.fixture
def etl_version_2(monkeypatch):
    etl_version = 2
    monkeypatch.setenv("ETL_VERSION", f"{etl_version}")
    return etl_version
