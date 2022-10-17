import pytest
from botocore.exceptions import ClientError

from lambdas import alma_prep


def test_extract_file_from_source_bucket_to_target_bucket(s3_client):
    with pytest.raises(ClientError):
        s3_client.head_object(Bucket="test-timdex-bucket", Key="extracted.xml")

    alma_prep.extract_file_from_source_bucket_to_target_bucket(
        s3_client=s3_client,
        source_bucket="test-alma-bucket",
        source_file_key="exlibris/timdex/TIMDEX_ALMA_EXPORT_20220912_210929[053]"
        "_delete.tar.gz",
        target_bucket="test-timdex-bucket",
        target_file_key="extracted.xml",
    )

    response = s3_client.head_object(Bucket="test-timdex-bucket", Key="extracted.xml")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_extract_tarfile():
    with open(
        "tests/fixtures/TIMDEX_ALMA_EXPORT_20220912_210929[053]_delete.tar.gz", "rb"
    ) as tar:
        extracted = alma_prep.extract_tarfile(tar)
        xml = next(extracted).read().decode("utf-8")
        assert xml.startswith('<?xml version="1.0" encoding="UTF-8"?>')


def test_get_load_type_and_sequence_from_alma_export_filename_with_sequence():
    file_name = "exlibris/timdex/TIMDEX_ALMA_EXPORT_20220912_210929[053]_new_1.tar.gz"
    (
        load_type,
        sequence,
    ) = alma_prep.get_load_type_and_sequence_from_alma_export_filename(file_name)
    assert load_type == "index"
    assert sequence == "01"


def test_get_load_type_and_sequence_from_alma_export_filename_without_sequence():
    file_name = "exlibris/timdex/TIMDEX_ALMA_EXPORT_20220912_210929[053]_delete.tar.gz"
    (
        load_type,
        sequence,
    ) = alma_prep.get_load_type_and_sequence_from_alma_export_filename(file_name)
    assert load_type == "delete"
    assert sequence is None


def test_prepare_alma_export_files(s3_client):
    starting_files_in_timdex_bucket = s3_client.list_objects_v2(
        Bucket="test-timdex-bucket"
    )["KeyCount"]
    assert starting_files_in_timdex_bucket == 0
    alma_prep.prepare_alma_export_files("2022-09-12", "daily", "test-timdex-bucket")
    ending_files_in_timdex_bucket = s3_client.list_objects_v2(
        Bucket="test-timdex-bucket"
    )["KeyCount"]
    assert ending_files_in_timdex_bucket == 3
