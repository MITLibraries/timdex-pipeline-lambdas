from unittest.mock import patch

from lambdas import format_input


def test_lambda_handler_with_next_step_extract():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "testsource",
        "run-id": "run-abc-123",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
    }
    output = format_input.lambda_handler(event, {})
    assert output == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "verbose": False,
        "harvester-type": "oai",
        "next-step": "transform",
        "extract": {
            "extract-command": [
                "--host=https://example.com/oai",
                "--output-file=s3://test-timdex-bucket/testsource/"
                "testsource-2022-01-02-daily-extracted-records-to-index.xml",
                "harvest",
                "--metadata-format=oai_dc",
                "--from-date=2022-01-01",
            ]
        },
    }


def test_lambda_handler_with_next_step_transform_files_present(s3_client, run_timestamp):
    s3_client.put_object(
        Bucket="test-timdex-bucket",
        Key="testsource/testsource-2022-01-02-daily-extracted-records-to-index.xml",
        Body="I am a file",
    )
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "transform",
        "source": "testsource",
        "run-id": "run-abc-123",
        "run-timestamp": run_timestamp,
        "verbose": "true",
    }
    assert format_input.lambda_handler(event, {}) == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "verbose": True,
        "next-step": "load",
        "transform": {
            "files-to-transform": [
                {
                    "transform-command": [
                        "--input-file=s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-daily-extracted-records-to-index.xml",
                        "--output-location=s3://test-timdex-bucket/dataset",
                        "--source=testsource",
                        "--run-id=run-abc-123",
                        f"--run-timestamp={run_timestamp}",
                    ]
                }
            ]
        },
    }


def test_lambda_handler_with_next_step_transform_alma_files_present(run_timestamp):
    event = {
        "run-date": "2022-09-12",
        "run-type": "daily",
        "next-step": "transform",
        "source": "alma",
        "run-id": "run-abc-123",
        "run-timestamp": run_timestamp,
        "verbose": "False",
    }
    assert format_input.lambda_handler(event, {}) == {
        "run-date": "2022-09-12",
        "run-type": "daily",
        "source": "alma",
        "verbose": False,
        "next-step": "load",
        "transform": {
            "files-to-transform": [
                {
                    "transform-command": [
                        "--input-file=s3://test-timdex-bucket/alma/"
                        "alma-2022-09-12-daily-extracted-records-to-delete.xml",
                        "--output-location=s3://test-timdex-bucket/dataset",
                        "--source=alma",
                        "--run-id=run-abc-123",
                        f"--run-timestamp={run_timestamp}",
                    ]
                },
                {
                    "transform-command": [
                        "--input-file=s3://test-timdex-bucket/alma/"
                        "alma-2022-09-12-daily-extracted-records-to-index_01.xml",
                        "--output-location=s3://test-timdex-bucket/dataset",
                        "--source=alma",
                        "--run-id=run-abc-123",
                        f"--run-timestamp={run_timestamp}",
                    ]
                },
                {
                    "transform-command": [
                        "--input-file=s3://test-timdex-bucket/alma/"
                        "alma-2022-09-12-daily-extracted-records-to-index_02.xml",
                        "--output-location=s3://test-timdex-bucket/dataset",
                        "--source=alma",
                        "--run-id=run-abc-123",
                        f"--run-timestamp={run_timestamp}",
                    ]
                },
            ]
        },
    }


def test_lambda_handler_with_next_step_transform_no_files_present_alma():
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "transform",
        "source": "alma",
        "run-id": "run-abc-123",
    }
    assert format_input.lambda_handler(event, {}) == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "alma",
        "verbose": False,
        "failure": "There were no transformed files present in the TIMDEX S3 bucket "
        "for the provided date and source, something likely went wrong.",
    }


def test_lambda_handler_with_next_step_transform_no_files_present_full():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "next-step": "transform",
        "source": "testsource",
        "run-id": "run-abc-123",
    }
    assert format_input.lambda_handler(event, {}) == {
        "run-date": "2022-01-02",
        "run-type": "full",
        "source": "testsource",
        "verbose": False,
        "failure": "There were no transformed files present in the TIMDEX S3 bucket "
        "for the provided date and source, something likely went wrong.",
    }


def test_lambda_handler_with_next_step_transform_no_files_present_daily():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "transform",
        "source": "testsource",
        "run-id": "run-abc-123",
    }
    assert format_input.lambda_handler(event, {}) == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "verbose": False,
        "success": "There were no daily new/updated/deleted records to harvest.",
    }


def test_lambda_handler_with_next_step_load_files_present(s3_client):
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "load",
        "source": "testsource",
        "run-id": "run-abc-123",
    }

    with patch(
        "lambdas.helpers.dataset_records_exist_for_run",
        return_value=True,
    ) as _mocked_record_count:
        response = format_input.lambda_handler(event, {})

    assert response == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "verbose": False,
        "load": {
            "bulk-update-command": [
                "bulk-update",
                "--run-date",
                "2022-01-02",
                "--run-id",
                "run-abc-123",
                "--source",
                "testsource",
                "s3://test-timdex-bucket/dataset",
            ]
        },
    }


def test_lambda_handler_with_next_step_load_no_files_present():
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "load",
        "source": "testsource",
        "run-id": "run-abc-123",
    }

    with patch(
        "lambdas.helpers.dataset_records_exist_for_run",
        return_value=False,
    ) as _mocked_record_count:
        response = format_input.lambda_handler(event, {})

    assert response == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "verbose": False,
        "failure": (
            "No records were found in the TIMDEX dataset for "
            "run_date '2022-01-02', run_id 'run-abc-123'."
        ),
    }
