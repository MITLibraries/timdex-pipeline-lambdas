import pytest
from freezegun import freeze_time

from lambdas import config, errors, helpers


def test_format_run_date_valid_run_date_string():
    assert helpers.format_run_date("2022-01-02T12:13:14Z") == "2022-01-02"


def test_format_run_date_invalid_run_date_string_raises_error():
    with pytest.raises(ValueError) as error:
        helpers.format_run_date("20220102")
    assert (
        "Input 'run-date' value must be one of the following date string formats: "
        f"{config.VALID_DATE_FORMATS}. Value provided was '20220102'"
    ) in str(error.value)


def test_generate_harvest_from_date():
    assert helpers.generate_harvest_from_date("2022-01-02") == "2022-01-01"


@freeze_time("2022-01-02 12:13:14")
def test_generate_index_name():
    assert helpers.generate_index_name("testsource") == "testsource-2022-01-02t12-13-14"


def test_generate_step_output_filename_with_sequence():
    assert (
        helpers.generate_step_output_filename("index", "prefix", "extract", "01")
        == "prefix-to-index_01.xml"
    )


def test_generate_step_output_filename_without_sequence():
    assert (
        helpers.generate_step_output_filename("delete", "prefix", "transform")
        == "prefix-to-delete.txt"
    )


def test_generate_step_output_prefix():
    assert (
        helpers.generate_step_output_prefix(
            "testsource", "2022-01-02", "full", "extract"
        )
        == "testsource/testsource-2022-01-02-full-extracted-records"
    )


def test_get_load_type_and_sequence_from_timdex_filename_with_sequence():
    assert helpers.get_load_type_and_sequence_from_timdex_filename(
        "testsource/testsource-2022-01-02-full-extracted-records-to-index_05.xml"
    ) == ("index", "05")


def test_get_load_type_and_sequence_from_timdex_filename_without_sequence():
    assert helpers.get_load_type_and_sequence_from_timdex_filename(
        "testsource/testsource-2022-01-02-full-transformed-records-to-delete.json"
    ) == ("delete", None)


def test_list_s3_files_by_prefix(s3_client):
    s3_client.put_object(
        Bucket="test-timdex-bucket",
        Key="the/right-prefix-for-a-file.txt",
        Body="I am a file",
    )
    s3_client.put_object(
        Bucket="test-timdex-bucket",
        Key="the/right-prefix-for-another-file.txt",
        Body="I am a file",
    )
    s3_client.put_object(
        Bucket="test-timdex-bucket",
        Key="a-different/prefix-for-aa-file.txt",
        Body="I am a file",
    )
    response = helpers.list_s3_files_by_prefix("test-timdex-bucket", "the/right-prefix")
    assert response == [
        "the/right-prefix-for-a-file.txt",
        "the/right-prefix-for-another-file.txt",
    ]


def test_list_s3_files_by_prefix_no_files_raises_error():
    with pytest.raises(errors.NoFilesError):
        helpers.list_s3_files_by_prefix("test-timdex-bucket", "the/right-prefix")
