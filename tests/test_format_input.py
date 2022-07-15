import pytest

from lambdas import format_input


# lambda_handler() tests
def test_lambda_handler_with_starting_step_load():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "load",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
    }
    output = format_input.lambda_handler(event, {})
    assert output == {
        "starting-step": "load",
        "load": {
            "commands": [
                "--url=https://example.com/opensearch",
                "ingest",
                "--source=test-source",
                "--new",
                "--auto",
                "s3://test-bucket/test-source-full-transformed-records-2022-01-01.json",
            ]
        },
    }


def test_lambda_handler_with_starting_step_transform():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "transform",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
        "index-prefix": "rdi",
    }
    output = format_input.lambda_handler(event, {})
    assert output == {
        "starting-step": "transform",
        "load": {
            "commands": [
                "--url=https://example.com/opensearch",
                "ingest",
                "--source=rdi-test-source",
                "--new",
                "s3://test-bucket/test-source-full-transformed-records-2022-01-01.json",
            ]
        },
        "transform": {
            "commands": [
                (
                    "--input-file=s3://test-bucket/"
                    "test-source-full-harvested-records-2022-01-01.xml"
                ),
                (
                    "--output-file=s3://test-bucket/"
                    "test-source-full-transformed-records-2022-01-01.json"
                ),
                "--source=test-source",
            ],
            "result-file": {
                "bucket": "test-bucket",
                "key": "test-source-full-transformed-records-2022-01-01.json",
            },
        },
    }


def test_lambda_handler_with_starting_step_harvest():
    event = {
        "harvest-type": "daily",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "harvest",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
        "oai-set-spec": "Collection_1",
    }
    output = format_input.lambda_handler(event, {})
    assert output == {
        "starting-step": "harvest",
        "load": {
            "commands": [
                "--url=https://example.com/opensearch",
                "ingest",
                "--source=test-source",
                "s3://test-bucket/test-source-daily-transformed-records-2022-01-01.json",
            ]
        },
        "transform": {
            "commands": [
                (
                    "--input-file=s3://test-bucket/"
                    "test-source-daily-harvested-records-2022-01-01.xml"
                ),
                (
                    "--output-file=s3://test-bucket/"
                    "test-source-daily-transformed-records-2022-01-01.json"
                ),
                "--source=test-source",
            ],
            "result-file": {
                "bucket": "test-bucket",
                "key": "test-source-daily-transformed-records-2022-01-01.json",
            },
        },
        "harvest": {
            "commands": [
                "--host=https://example.com/oai",
                "--output-file=s3://test-bucket/"
                "test-source-daily-harvested-records-2022-01-01.xml",
                "harvest",
                "--metadata-format=oai_dc",
                "--set-spec=Collection_1",
                "--from-date=2022-01-01",
                "--until-date=2022-01-01",
                "--exclude-deleted",
            ],
            "result-file": {
                "bucket": "test-bucket",
                "key": "test-source-daily-harvested-records-2022-01-01.xml",
            },
        },
    }


# validate_input() tests
def test_validate_input_missing_required_field_raises_error():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "transform",
        "source": "test-source",
    }
    with pytest.raises(ValueError) as error:
        format_input.validate_input(event)
    assert "Input must include all required fields. Missing fields: ['time']" in str(
        error.value
    )


def test_validate_input_with_invalid_starting_step_raises_error():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "wrong",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
    }
    with pytest.raises(ValueError) as error:
        format_input.validate_input(event)
    assert (
        "Input 'starting-step' value must be one of: "
        f"{format_input.VALID_STARTING_STEPS}. Value provided was 'wrong'"
        in str(error.value)
    )


def test_validate_input_with_invalid_harvest_type_raises_error():
    event = {
        "harvest-type": "wrong",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "load",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
    }
    with pytest.raises(ValueError) as error:
        format_input.validate_input(event)
    assert (
        f"Input 'harvest-type' value must be one of: {format_input.VALID_HARVEST_TYPES}. "
        "Value provided was 'wrong'" in str(error.value)
    )


def test_validate_input_with_missing_harvest_fields_raises_error():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "harvest",
        "source": "test-source",
        "time": "2022-01-02:T12:13:14Z",
        "oai-pmh-host": "https://example.com/oai",
    }
    with pytest.raises(ValueError) as error:
        format_input.validate_input(event)
    assert (
        "Input must include all required harvest fields when starting with "
        "harvest step. Missing fields: ['oai-metadata-format']" in str(error.value)
    )


def test_validate_input_with_all_required_fields_returns_none():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "transform",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
    }
    assert format_input.validate_input(event) is None


def test_validate_input_with_all_required_harvest_fields_returns_none():
    event = {
        "harvest-type": "daily",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "harvest",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
    }
    assert format_input.validate_input(event) is None


# generate_harvest_end_date_string() tests
def test_generate_harvest_end_date_string_with_short_format_returns_string():
    date_string = format_input.generate_harvest_end_date_string("2022-01-02")
    assert date_string == "2022-01-01"


def test_generate_harvest_end_date_string_with_long_format_returns_string():
    date_string = format_input.generate_harvest_end_date_string("2022-01-02T12:13:14Z")
    assert date_string == "2022-01-01"


def test_generate_harvest_end_date_string_with_invalid_format_raises_error():
    with pytest.raises(ValueError) as error:
        format_input.generate_harvest_end_date_string("2022")
    assert (
        "Input 'time' value must be one of the following date string formats: "
        f"{format_input.VALID_DATE_FORMATS}. Value provided was '2022'"
        in str(error.value)
    )


# generate_load_input() tests
def test_generate_load_input_daily_with_required_fields_only():
    event = {
        "harvest-type": "daily",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "load",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
    }
    output = format_input.generate_load_input(
        event, "daily", "test-source-daily-transformed-records-2022-01-01.json"
    )
    assert output == {
        "commands": [
            "--url=https://example.com/opensearch",
            "ingest",
            "--source=test-source",
            "s3://test-bucket/test-source-daily-transformed-records-2022-01-01.json",
        ]
    }


def test_generate_load_input_daily_with_relevant_optional_fields():
    event = {
        "harvest-type": "daily",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "load",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
        "index-prefix": "rdi",
    }
    output = format_input.generate_load_input(
        event, "daily", "test-source-daily-transformed-records-2022-01-01.json"
    )
    assert output == {
        "commands": [
            "--url=https://example.com/opensearch",
            "ingest",
            "--source=rdi-test-source",
            "s3://test-bucket/test-source-daily-transformed-records-2022-01-01.json",
        ]
    }


def test_generate_load_input_full_with_required_fields_only():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "load",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
    }
    output = format_input.generate_load_input(
        event, "full", "test-source-full-transformed-records-2022-01-01.json"
    )
    assert output == {
        "commands": [
            "--url=https://example.com/opensearch",
            "ingest",
            "--source=test-source",
            "--new",
            "--auto",
            "s3://test-bucket/test-source-full-transformed-records-2022-01-01.json",
        ]
    }


def test_generate_load_input_full_with_relevant_optional_fields():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "load",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
        "index-prefix": "rdi",
    }
    output = format_input.generate_load_input(
        event, "full", "test-source-full-transformed-records-2022-01-01.json"
    )
    assert output == {
        "commands": [
            "--url=https://example.com/opensearch",
            "ingest",
            "--source=rdi-test-source",
            "--new",
            "s3://test-bucket/test-source-full-transformed-records-2022-01-01.json",
        ]
    }


# generate_transform_input() tests
def test_generate_transform_input_with_required_fields_only():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "transform",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
    }
    output = format_input.generate_transform_input(
        event,
        "test-source-full-harvested-records-2022-01-01.xml",
        "test-source-full-transformed-records-2022-01-01.json",
    )
    assert output == {
        "commands": [
            (
                "--input-file=s3://test-bucket/"
                "test-source-full-harvested-records-2022-01-01.xml"
            ),
            (
                "--output-file=s3://test-bucket/"
                "test-source-full-transformed-records-2022-01-01.json"
            ),
            "--source=test-source",
        ],
        "result-file": {
            "bucket": "test-bucket",
            "key": "test-source-full-transformed-records-2022-01-01.json",
        },
    }


def test_generate_transform_input_with_relevant_optional_fields():
    event = {
        "harvest-type": "daily",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "transform",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
        "verbose": "true",
    }
    output = format_input.generate_transform_input(
        event,
        "test-source-daily-harvested-records-2022-01-01.xml",
        "test-source-daily-transformed-records-2022-01-01.json",
    )
    assert output == {
        "commands": [
            (
                "--input-file=s3://test-bucket/"
                "test-source-daily-harvested-records-2022-01-01.xml"
            ),
            (
                "--output-file=s3://test-bucket/"
                "test-source-daily-transformed-records-2022-01-01.json"
            ),
            "--source=test-source",
            "--verbose",
        ],
        "result-file": {
            "bucket": "test-bucket",
            "key": "test-source-daily-transformed-records-2022-01-01.json",
        },
    }


# generate_harvest_input() tests
def test_generate_harvest_input_full_with_required_fields_only():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "harvest",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
    }
    output = format_input.generate_harvest_input(
        event,
        "2022-01-01",
        "full",
        "test-source-full-harvested-records-2022-01-01.xml",
    )
    assert output == {
        "commands": [
            "--host=https://example.com/oai",
            "--output-file=s3://test-bucket/"
            "test-source-full-harvested-records-2022-01-01.xml",
            "harvest",
            "--metadata-format=oai_dc",
            "--until-date=2022-01-01",
            "--exclude-deleted",
        ],
        "result-file": {
            "bucket": "test-bucket",
            "key": "test-source-full-harvested-records-2022-01-01.xml",
        },
    }


def test_generate_harvest_input_daily_with_relevant_optional_fields():
    event = {
        "harvest-type": "daily",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "harvest",
        "source": "test-source",
        "time": "2022-01-02T12:13:14Z",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
        "oai-set-spec": "Collection_1",
        "verbose": "true",
    }
    output = format_input.generate_harvest_input(
        event,
        "2022-01-01",
        "daily",
        "test-source-daily-harvested-records-2022-01-01.xml",
    )
    assert output == {
        "commands": [
            "--host=https://example.com/oai",
            "--output-file=s3://test-bucket/"
            "test-source-daily-harvested-records-2022-01-01.xml",
            "--verbose",
            "harvest",
            "--metadata-format=oai_dc",
            "--set-spec=Collection_1",
            "--from-date=2022-01-01",
            "--until-date=2022-01-01",
            "--exclude-deleted",
        ],
        "result-file": {
            "bucket": "test-bucket",
            "key": "test-source-daily-harvested-records-2022-01-01.xml",
        },
    }


def test_generate_harvest_input_with_aspace_source_inserts_method():
    event = {
        "harvest-type": "full",
        "opensearch-url": "https://example.com/opensearch",
        "output-bucket": "test-bucket",
        "starting-step": "harvest",
        "source": "aspace",
        "time": "2022-01-02T12:13:14Z",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_ead",
    }
    output = format_input.generate_harvest_input(
        event,
        "2022-01-01",
        "full",
        "aspace-full-harvested-records-2022-01-01.xml",
    )
    assert output == {
        "commands": [
            "--host=https://example.com/oai",
            "--output-file=s3://test-bucket/"
            "aspace-full-harvested-records-2022-01-01.xml",
            "harvest",
            "--method=get",
            "--metadata-format=oai_ead",
            "--until-date=2022-01-01",
            "--exclude-deleted",
        ],
        "result-file": {
            "bucket": "test-bucket",
            "key": "aspace-full-harvested-records-2022-01-01.xml",
        },
    }
