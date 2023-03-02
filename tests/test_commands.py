from freezegun import freeze_time

from lambdas import commands


def test_generate_extract_command_required_input_fields():
    input_data = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "testsource",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
    }
    assert commands.generate_extract_command(
        input_data, "2022-01-02", "test-timdex-bucket", False
    ) == {
        "extract-command": [
            "--host=https://example.com/oai",
            "--output-file=s3://test-timdex-bucket/testsource/"
            "testsource-2022-01-02-daily-extracted-records-to-index.xml",
            "harvest",
            "--metadata-format=oai_dc",
            "--from-date=2022-01-01",
        ]
    }


def test_generate_extract_command_all_input_fields():
    input_data = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "next-step": "extract",
        "source": "aspace",
        "verbose": "true",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
        "oai-set-spec": "Collection1",
    }
    assert commands.generate_extract_command(
        input_data, "2022-01-02", "test-timdex-bucket", True
    ) == {
        "extract-command": [
            "--host=https://example.com/oai",
            "--output-file=s3://test-timdex-bucket/aspace/"
            "aspace-2022-01-02-full-extracted-records-to-index.xml",
            "--verbose",
            "harvest",
            "--metadata-format=oai_dc",
            "--method=get",
            "--set-spec=Collection1",
            "--exclude-deleted",
        ]
    }


def test_generate_transform_commands_required_input_fields():
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
        extract_output_files, input_data, "2022-01-02", "test-timdex-bucket"
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


def test_generate_transform_commands_all_input_fields():
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
        extract_output_files, input_data, "2022-01-02", "test-timdex-bucket"
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


def test_generate_load_commands_daily():
    transform_output_files = [
        "testsource/testsource-2022-01-02-daily-transformed-records-to-index_01.json",
        "testsource/testsource-2022-01-02-daily-transformed-records-to-index_02.json",
        "testsource/testsource-2022-01-02-daily-transformed-records-to-delete.txt",
    ]
    assert commands.generate_load_commands(
        transform_output_files, "daily", "testsource", "test-timdex-bucket"
    ) == {
        "files-to-index": [
            {
                "load-command": [
                    "bulk-index",
                    "--source",
                    "testsource",
                    "s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-transformed-records-to-index_01.json",
                ]
            },
            {
                "load-command": [
                    "bulk-index",
                    "--source",
                    "testsource",
                    "s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-transformed-records-to-index_02.json",
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
            }
        ],
    }


@freeze_time("2022-01-02 12:13:14")
def test_generate_load_commands_full_not_aliased():
    transform_output_files = [
        "testsource/testsource-2022-01-02-full-transformed-records-to-index.json"
    ]
    assert commands.generate_load_commands(
        transform_output_files, "full", "testsource", "test-timdex-bucket"
    ) == {
        "create-index-command": ["create", "--index", "testsource-2022-01-02t12-13-14"],
        "files-to-index": [
            {
                "load-command": [
                    "bulk-index",
                    "--index",
                    "testsource-2022-01-02t12-13-14",
                    (
                        "s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-full-transformed-records-to-index.json"
                    ),
                ]
            }
        ],
        "promote-index-command": [
            "promote",
            "--index",
            "testsource-2022-01-02t12-13-14",
        ],
    }


@freeze_time("2022-01-02 12:13:14")
def test_generate_load_commands_full_aliased():
    transform_output_files = [
        "alma/alma-2022-01-02-full-transformed-records-to-index.json"
    ]
    assert commands.generate_load_commands(
        transform_output_files, "full", "alma", "test-timdex-bucket"
    ) == {
        "create-index-command": ["create", "--index", "alma-2022-01-02t12-13-14"],
        "files-to-index": [
            {
                "load-command": [
                    "bulk-index",
                    "--index",
                    "alma-2022-01-02t12-13-14",
                    (
                        "s3://test-timdex-bucket/alma/"
                        "alma-2022-01-02-full-transformed-records-to-index.json"
                    ),
                ]
            }
        ],
        "promote-index-command": [
            "promote",
            "--index",
            "alma-2022-01-02t12-13-14",
            "--alias",
            "timdex",
        ],
    }


@freeze_time("2022-01-02 12:13:14")
def test_generate_load_commands_full_with_deletes_logs_error(caplog):
    transform_output_files = [
        "alma/alma-2022-01-02-full-transformed-records-to-delete.txt"
    ]
    commands.generate_load_commands(
        transform_output_files, "full", "alma", "test-timdex-bucket"
    )
    assert (
        "lambdas.commands",
        40,
        "alma full ingest had a deleted records file: "
        "alma/alma-2022-01-02-full-transformed-records-to-delete.txt",
    ) in caplog.record_tuples


def test_generate_load_command_unexpected_input():
    transform_output_files = [
        "alma/alma-2022-01-02-full-transformed-records-to-index.json"
    ]
    assert commands.generate_load_commands(
        transform_output_files, "wrong", "alma", "test-timdex-bucket"
    ) == {
        "failure": "Something unexpected went wrong. Please check input and try again."
    }
