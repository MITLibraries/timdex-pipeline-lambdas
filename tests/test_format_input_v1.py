from lambdas import format_input

# NOTE: FEATURE FLAG: this file can be FULLY removed after v2 work is complete


def test_lambda_handler_with_next_step_extract():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "testsource",
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


def test_lambda_handler_with_next_step_transform_files_present(s3_client):
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
                        "--output-file=s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-daily-transformed-records-to-index.json",
                        "--source=testsource",
                    ]
                }
            ]
        },
    }


def test_lambda_handler_with_next_step_transform_alma_files_present():
    event = {
        "run-date": "2022-09-12",
        "run-type": "daily",
        "next-step": "transform",
        "source": "alma",
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
                        "--output-file=s3://test-timdex-bucket/alma/"
                        "alma-2022-09-12-daily-transformed-records-to-delete.txt",
                        "--source=alma",
                    ]
                },
                {
                    "transform-command": [
                        "--input-file=s3://test-timdex-bucket/alma/"
                        "alma-2022-09-12-daily-extracted-records-to-index_01.xml",
                        "--output-file=s3://test-timdex-bucket/alma/"
                        "alma-2022-09-12-daily-transformed-records-to-index_01.json",
                        "--source=alma",
                    ]
                },
                {
                    "transform-command": [
                        "--input-file=s3://test-timdex-bucket/alma/"
                        "alma-2022-09-12-daily-extracted-records-to-index_02.xml",
                        "--output-file=s3://test-timdex-bucket/alma/"
                        "alma-2022-09-12-daily-transformed-records-to-index_02.json",
                        "--source=alma",
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
    }
    assert format_input.lambda_handler(event, {}) == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "verbose": False,
        "success": "There were no daily new/updated/deleted records to harvest.",
    }


def test_lambda_handler_with_next_step_load_files_present(s3_client):
    s3_client.put_object(
        Bucket="test-timdex-bucket",
        Key="testsource/testsource-2022-01-02-daily-transformed-records-to-index.json",
        Body="I am a file",
    )
    s3_client.put_object(
        Bucket="test-timdex-bucket",
        Key="testsource/testsource-2022-01-02-daily-transformed-records-to-delete.txt",
        Body="record-id",
    )
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "load",
        "source": "testsource",
    }
    assert format_input.lambda_handler(event, {}) == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "verbose": False,
        "load": {
            "files-to-index": [
                {
                    "load-command": [
                        "bulk-index",
                        "--source",
                        "testsource",
                        "s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-daily-transformed-records-to-index.json",
                    ]
                },
            ],
            "files-to-delete": [
                {
                    "load-command": [
                        "bulk-delete",
                        "--source",
                        "testsource",
                        "s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-daily-transformed-records-to-delete.txt",
                    ]
                },
            ],
        },
    }


def test_lambda_handler_with_next_step_load_no_files_present():
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "load",
        "source": "testsource",
    }
    assert format_input.lambda_handler(event, {}) == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "verbose": False,
        "failure": "There were no transformed files present in the TIMDEX S3 bucket "
        "for the provided date and source, something likely went wrong.",
    }
