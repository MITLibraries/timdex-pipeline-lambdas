# ruff: noqa: PT011

import pytest
from freezegun import freeze_time

from lambdas import errors, helpers
from lambdas.config import Config
from lambdas.format_input import InputPayload

CONFIG = Config()


def test_validate_input_missing_required_field_raises_error():
    event = {
        "next-step": "transform",
        "run-type": "full",
        "source": "testsource",
    }
    with pytest.raises(ValueError) as error:
        InputPayload.validate_input(event)
    assert "Input must include all required fields. Missing fields: ['run-date']" in str(
        error.value
    )


def test_validate_input_with_invalid_next_step_raises_error():
    event = {
        "next-step": "wrong",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
    }
    with pytest.raises(ValueError) as error:
        InputPayload.validate_input(event)
    assert (
        "Input 'next-step' value must be one of: "
        f"{CONFIG.VALID_STEPS}. Value provided was 'wrong'" in str(error.value)
    )


def test_validate_input_with_invalid_run_type_raises_error():
    event = {
        "next-step": "load",
        "run-date": "2022-01-02",
        "run-type": "wrong",
        "source": "testsource",
    }
    with pytest.raises(ValueError) as error:
        InputPayload.validate_input(event)
    assert (
        f"Input 'run-type' value must be one of: {CONFIG.VALID_RUN_TYPES}. "
        "Value provided was 'wrong'" in str(error.value)
    )


def test_validate_input_with_missing_harvest_fields_raises_error():
    event = {
        "next-step": "extract",
        "run-date": "2022-01-02:T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
        "oai-pmh-host": "https://example.com/oai",
    }
    with pytest.raises(ValueError) as error:
        InputPayload.validate_input(event)
    assert (
        "Input must include all required harvest fields when starting with "
        "harvest step. Missing fields: ['oai-metadata-format']" in str(error.value)
    )


def test_validate_input_with_all_required_fields_returns_none():
    event = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
    }
    assert InputPayload.validate_input(event) is None


def test_validate_input_with_all_required_harvest_fields_returns_none():
    event = {
        "next-step": "extract",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
    }
    assert InputPayload.validate_input(event) is None


def test_validate_input_mitlibwebsite_missing_harvest_fields_raises_error():
    event = {
        "next-step": "extract",
        "run-date": "2022-01-02",
        "run-type": "full",
        "source": "mitlibwebsite",
        "btrix-config-yaml-file": "s3://bucket/config.yaml",
        "btrix-sitemaps": ["https://example.com/sitemap.xml"],
    }
    with pytest.raises(ValueError) as error:
        InputPayload.validate_input(event)
    assert "Input must include all required harvest fields when starting with " in str(
        error.value
    )
    assert "'btrix-sitemap-urls-output-file'" in str(error.value)


def test_validate_input_mitlibwebsite_daily_missing_previous_urls_raises_error():
    event = {
        "next-step": "extract",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "mitlibwebsite",
        "btrix-config-yaml-file": "s3://bucket/config.yaml",
        "btrix-sitemaps": ["https://example.com/sitemap.xml"],
        "btrix-sitemap-urls-output-file": "s3://bucket/output.txt",
    }
    with pytest.raises(ValueError) as error:
        InputPayload.validate_input(event)
    assert (
        "Field 'btrix-previous-sitemap-urls-file' required when 'run-type=daily'"
        in str(error.value)
    )


def test_validate_input_mitlibwebsite_full_without_previous_urls_returns_none():
    event = {
        "next-step": "extract",
        "run-date": "2022-01-02",
        "run-type": "full",
        "source": "mitlibwebsite",
        "btrix-config-yaml-file": "s3://bucket/config.yaml",
        "btrix-sitemaps": ["https://example.com/sitemap.xml"],
        "btrix-sitemap-urls-output-file": "s3://bucket/output.txt",
    }
    assert InputPayload.validate_input(event) is None


def test_validate_input_mitlibwebsite_daily_with_all_required_fields_returns_none():
    event = {
        "next-step": "extract",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "mitlibwebsite",
        "btrix-config-yaml-file": "s3://bucket/config.yaml",
        "btrix-sitemaps": ["https://example.com/sitemap.xml"],
        "btrix-sitemap-urls-output-file": "s3://bucket/output.txt",
        "btrix-previous-sitemap-urls-file": "s3://bucket/previous.txt",
    }
    assert InputPayload.validate_input(event) is None


def test_format_run_date_valid_run_date_string():
    assert helpers.format_run_date("2022-01-02T12:13:14Z") == "2022-01-02"


def test_format_run_date_invalid_run_date_string_raises_error():
    with pytest.raises(ValueError) as error:
        helpers.format_run_date("20220102")
    assert (
        "Input 'run-date' value must be one of the following date string formats: "
        f"{CONFIG.VALID_DATE_FORMATS}. Value provided was '20220102'"
    ) in str(error.value)


def test_generate_harvest_from_date():
    assert helpers.generate_harvest_from_date("2022-01-02") == "2022-01-01"


@freeze_time("2022-01-02 12:13:14")
def test_generate_index_name():
    assert helpers.generate_index_name("testsource") == "testsource-2022-01-02t12-13-14"


def test_generate_step_output_filename_geoharvester_file_type():
    assert (
        helpers.generate_step_output_filename("gismit", "index", "prefix", "extract")
        == "prefix-to-index.jsonl"
    )
    assert (
        helpers.generate_step_output_filename("gisogm", "index", "prefix", "extract")
        == "prefix-to-index.jsonl"
    )


def test_generate_step_output_filename_with_sequence():
    assert (
        helpers.generate_step_output_filename(
            "source", "index", "prefix", "extract", "01"
        )
        == "prefix-to-index_01.xml"
    )


def test_generate_step_output_filename_without_sequence():
    assert (
        helpers.generate_step_output_filename("source", "delete", "prefix", "transform")
        == "prefix-to-delete.txt"
    )


def test_generate_step_output_prefix(run_id, run_timestamp):
    event = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
        "run-id": run_id,
        "run-timestamp": run_timestamp,
    }
    input_payload = InputPayload.from_event(event)
    assert (
        helpers.generate_step_output_prefix(input_payload, "extract")
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
