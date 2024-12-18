# ruff: noqa: FBT003

from lambdas import commands

# NOTE: FEATURE FLAG: this file can be FULLY removed after v2 work is complete


def test_generate_transform_commands_required_input_fields(run_id):
    input_data = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
    }
    extract_output_files = [
        "testsource/testsource-2022-01-02-full-extracted-records-to-index.xml"
    ]
    assert commands.generate_transform_commands(
        extract_output_files, input_data, "2022-01-02", "test-timdex-bucket", run_id
    ) == {
        "files-to-transform": [
            {
                "transform-command": [
                    "--input-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-full-extracted-records-to-index.xml",
                    "--output-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-full-transformed-records-to-index.json",
                    "--source=testsource",
                ]
            }
        ]
    }


def test_generate_transform_commands_all_input_fields(run_id):
    input_data = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "source": "testsource",
    }
    extract_output_files = [
        "testsource/testsource-2022-01-02-daily-extracted-records-to-index_01.xml",
        "testsource/testsource-2022-01-02-daily-extracted-records-to-index_02.xml",
        "testsource/testsource-2022-01-02-daily-extracted-records-to-delete.xml",
    ]
    assert commands.generate_transform_commands(
        extract_output_files, input_data, "2022-01-02", "test-timdex-bucket", run_id
    ) == {
        "files-to-transform": [
            {
                "transform-command": [
                    "--input-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-extracted-records-to-index_01.xml",
                    "--output-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-transformed-records-to-index_01.json",
                    "--source=testsource",
                ]
            },
            {
                "transform-command": [
                    "--input-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-extracted-records-to-index_02.xml",
                    "--output-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-transformed-records-to-index_02.json",
                    "--source=testsource",
                ]
            },
            {
                "transform-command": [
                    "--input-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-extracted-records-to-delete.xml",
                    "--output-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-transformed-records-to-delete.txt",
                    "--source=testsource",
                ]
            },
        ]
    }
