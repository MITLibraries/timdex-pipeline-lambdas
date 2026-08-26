from unittest.mock import patch

from lambdas import format_input


def test_lambda_handler_with_next_step_extract():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "researchdatabases",
        "run-id": "run-abc-123",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
    }
    output = format_input.lambda_handler(event, {})
    assert output == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "run-id": "run-abc-123",
        "source": "researchdatabases",
        "verbose": False,
        "harvester-type": "oai",
        "next-step": "transform",
        "extract": {
            "extract-command": [
                "--host=https://example.com/oai",
                (
                    "--output-file=s3://test-timdex-bucket/researchdatabases/"
                    "researchdatabases-2022-01-02-daily-extracted-records-to-index.xml"
                ),
                "harvest",
                "--metadata-format=oai_dc",
                "--from-date=2022-01-01",
            ]
        },
    }


def test_lambda_handler_with_next_step_extract_mitlibwebsite_full():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "next-step": "extract",
        "source": "mitlibwebsite",
        "run-id": "run-abc-123",
        "btrix-config-yaml-file": "s3://bucket/config.yaml",
        "btrix-sitemaps": [
            "https://libraries.mit.edu/sitemap.xml",
            "https://libraries.mit.edu/news/sitemap.xml",
        ],
        "btrix-sitemap-urls-output-file": "s3://bucket/output.txt",
    }
    output = format_input.lambda_handler(event, {})
    assert output == {
        "run-date": "2022-01-02",
        "run-type": "full",
        "run-id": "run-abc-123",
        "source": "mitlibwebsite",
        "verbose": False,
        "harvester-type": "browsertrix",
        "next-step": "transform",
        "extract": {
            "extract-command": [
                "harvest",
                "--config-yaml-file=s3://bucket/config.yaml",
                (
                    "--records-output-file=s3://test-timdex-bucket/mitlibwebsite/"
                    "mitlibwebsite-2022-01-02-full-extracted-records-to-index.jsonl"
                ),
                "--sitemap=https://libraries.mit.edu/sitemap.xml",
                "--sitemap=https://libraries.mit.edu/news/sitemap.xml",
                "--sitemap-urls-output-file=s3://bucket/output.txt",
            ]
        },
    }


def test_lambda_handler_with_next_step_extract_mitlibwebsite_daily():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "mitlibwebsite",
        "run-id": "run-abc-123",
        "btrix-config-yaml-file": "s3://bucket/config.yaml",
        "btrix-sitemaps": ["https://libraries.mit.edu/sitemap.xml"],
        "btrix-sitemap-urls-output-file": "s3://bucket/output.txt",
        "btrix-previous-sitemap-urls-file": "s3://bucket/previous.txt",
    }
    output = format_input.lambda_handler(event, {})
    assert output == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "run-id": "run-abc-123",
        "source": "mitlibwebsite",
        "verbose": False,
        "harvester-type": "browsertrix",
        "next-step": "transform",
        "extract": {
            "extract-command": [
                "harvest",
                "--config-yaml-file=s3://bucket/config.yaml",
                (
                    "--records-output-file=s3://test-timdex-bucket/mitlibwebsite/"
                    "mitlibwebsite-2022-01-02-daily-extracted-records-to-index.jsonl"
                ),
                "--sitemap=https://libraries.mit.edu/sitemap.xml",
                "--sitemap-from-date=2022-01-01",
                "--sitemap-urls-output-file=s3://bucket/output.txt",
                "--previous-sitemap-urls-file=s3://bucket/previous.txt",
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
        "run-id": "run-abc-123",
        "source": "testsource",
        "verbose": True,
        "next-step": "load",
        "transform": {
            "files-to-transform": [
                {
                    "transform-command": [
                        (
                            "--input-file=s3://test-timdex-bucket/testsource/"
                            "testsource-2022-01-02-daily-extracted-records-to-index.xml"
                        ),
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
        "run-id": "run-abc-123",
        "source": "alma",
        "verbose": False,
        "next-step": "load",
        "transform": {
            "files-to-transform": [
                {
                    "transform-command": [
                        (
                            "--input-file=s3://test-timdex-bucket/alma/"
                            "alma-2022-09-12-daily-extracted-records-to-delete.xml"
                        ),
                        "--output-location=s3://test-timdex-bucket/dataset",
                        "--source=alma",
                        "--run-id=run-abc-123",
                        f"--run-timestamp={run_timestamp}",
                    ]
                },
                {
                    "transform-command": [
                        (
                            "--input-file=s3://test-timdex-bucket/alma/"
                            "alma-2022-09-12-daily-extracted-records-to-index_01.xml"
                        ),
                        "--output-location=s3://test-timdex-bucket/dataset",
                        "--source=alma",
                        "--run-id=run-abc-123",
                        f"--run-timestamp={run_timestamp}",
                    ]
                },
                {
                    "transform-command": [
                        (
                            "--input-file=s3://test-timdex-bucket/alma/"
                            "alma-2022-09-12-daily-extracted-records-to-index_02.xml"
                        ),
                        "--output-location=s3://test-timdex-bucket/dataset",
                        "--source=alma",
                        "--run-id=run-abc-123",
                        f"--run-timestamp={run_timestamp}",
                    ]
                },
            ]
        },
    }


def test_lambda_handler_with_next_step_transform_auto_generated_timestamp(s3_client):
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
        "verbose": "true",
    }

    with patch("lambdas.format_input.datetime") as mock_datetime:
        mock_datetime.now.return_value.isoformat.return_value = (
            "2025-06-18T12:34:56.789000"
        )
        mock_datetime.UTC = format_input.datetime.UTC

        result = format_input.lambda_handler(event, {})

    assert result == {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "run-id": "run-abc-123",
        "source": "testsource",
        "verbose": True,
        "next-step": "load",
        "transform": {
            "files-to-transform": [
                {
                    "transform-command": [
                        (
                            "--input-file=s3://test-timdex-bucket/testsource/"
                            "testsource-2022-01-02-daily-extracted-records-to-index.xml"
                        ),
                        "--output-location=s3://test-timdex-bucket/dataset",
                        "--source=testsource",
                        "--run-id=run-abc-123",
                        "--run-timestamp=2025-06-18T12:34:56.789000",
                    ]
                }
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
        "next-step": "exit-error",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "run-id": "run-abc-123",
        "source": "alma",
        "verbose": False,
        "message": "There were no transformed files present in the TIMDEX S3 bucket "
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
        "next-step": "exit-error",
        "run-date": "2022-01-02",
        "run-type": "full",
        "run-id": "run-abc-123",
        "source": "testsource",
        "verbose": False,
        "message": "There were no transformed files present in the TIMDEX S3 bucket "
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
        "next-step": "exit-ok",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "run-id": "run-abc-123",
        "source": "testsource",
        "verbose": False,
        "message": "There were no daily new/updated/deleted records to harvest.",
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
        "next-step": "enrichment",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "run-id": "run-abc-123",
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
        "next-step": "exit-ok",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "run-id": "run-abc-123",
        "source": "testsource",
        "verbose": False,
        "message": (
            "No transformed records to index or delete "
            "were found for run_id 'run-abc-123'."
        ),
    }


def test_parallel_enrichments_step_normalizes_enrichments():
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "enrichment",
        "source": "dspace",
        "run-id": "run-abc-123",
        "enrichments": {"fulltexts": {"skip": True}},
    }

    response = format_input.lambda_handler(event, {})

    assert response["enrichments"] == {
        "skip": False,
        "embeddings": {"skip": False},
        "fulltexts": {"skip": True},
    }


def test_parallel_enrichments_step_defaults_missing_branch_skip_key():
    """Enrichment branches passed without "skip" are defaulted to skip=False."""
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "enrichment",
        "source": "dspace",
        "run-id": "run-abc-123",
        "enrichments": {"embeddings": {"foo": "bar"}},
    }

    response = format_input.lambda_handler(event, {})

    assert response["enrichments"] == {
        "skip": False,
        "embeddings": {"foo": "bar", "skip": False},
        "fulltexts": {"skip": False},
    }


def test_lambda_handler_with_next_step_embeddings_create_skip_source():
    """Source in SKIP_EMBEDDINGS_SOURCES exits early with message."""
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "embeddings-create",
        "source": "alma",
        "run-id": "run-abc-123",
    }

    with patch("lambdas.format_input.TIMDEXDataset") as mock_dataset:
        mock_dataset.return_value.conn.query.return_value.fetchone.return_value = (0,)
        response = format_input.lambda_handler(event, {})

    assert response["next-step"] == "exit-ok"
    assert response["message"] == "Not currently creating embeddings for source 'alma'"


def test_lambda_handler_with_next_step_embeddings_load_skip_source():
    """Source in SKIP_EMBEDDINGS_SOURCES exits early with message."""
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "embeddings-load",
        "source": "gisogm",
        "run-id": "run-abc-123",
    }

    with patch("lambdas.format_input.TIMDEXDataset") as mock_dataset:
        mock_dataset.return_value.conn.query.return_value.fetchone.return_value = (0,)
        response = format_input.lambda_handler(event, {})

    assert response["next-step"] == "exit-ok"
    assert response["message"] == "Not currently indexing embeddings for source 'gisogm'"


def test_enrichment_options_echoed_before_parallel_enrichments_step():
    """Caller-supplied enrichment options pass through all steps untouched.

    The StepFunction per-branch skip Choices (e.g. '$.enrichments.fulltexts.skip')
    are evaluated after the branch prep lambda, so the options must be echoed by
    every step to remain visible to the state machine.
    """
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "fulltexts-harvest",
        "source": "alma",
        "run-id": "run-abc-123",
        "enrichments": {"fulltexts": {"skip": True}},
    }

    response = format_input.lambda_handler(event, {})

    assert response["enrichments"] == {"fulltexts": {"skip": True}}


def test_lambda_handler_with_next_step_fulltexts_harvest_skip_source():
    """Source not in VALID_FULLTEXTS_SOURCES exits early with message."""
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "fulltexts-harvest",
        "source": "alma",
        "run-id": "run-abc-123",
    }

    response = format_input.lambda_handler(event, {})

    assert response["next-step"] == "exit-ok"
    assert response["message"] == "Not currently harvesting fulltexts for source 'alma'"


def test_lambda_handler_with_next_step_fulltexts_harvest_no_records(run_id):
    """No indexed records for the run exits early with message."""
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "fulltexts-harvest",
        "source": "dspace",
        "run-id": run_id,
    }

    with patch("lambdas.format_input.TIMDEXDataset") as mock_dataset:
        mock_dataset.return_value.conn.query.return_value.fetchone.return_value = (0,)
        response = format_input.lambda_handler(event, {})

    assert response["next-step"] == "exit-ok"
    assert response["message"] == (
        f"No records found for run '{run_id}', no fulltexts to harvest."
    )


def test_lambda_handler_with_next_step_fulltexts_harvest_records_present(run_id):
    """Indexed records present yields fulltexts harvest command."""
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "fulltexts-harvest",
        "source": "dspace",
        "run-id": run_id,
    }

    with patch("lambdas.format_input.TIMDEXDataset") as mock_dataset:
        mock_dataset.return_value.conn.query.return_value.fetchone.return_value = (42,)
        response = format_input.lambda_handler(event, {})

    assert response["next-step"] == "fulltexts-load"
    assert response["fulltexts"] == {
        "harvester-type": "dspace-fulltext-harvester",
        "harvest": {"command": ["--verbose", "harvest", f"--run-id={run_id}"]},
    }


def test_lambda_handler_with_next_step_fulltexts_load_no_fulltexts(run_id):
    """No harvested fulltexts exits early with message."""
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "fulltexts-load",
        "source": "dspace",
        "run-id": run_id,
    }

    with patch("lambdas.format_input.TIMDEXDataset") as mock_dataset:
        mock_dataset.return_value.conn.query.return_value.fetchone.return_value = (0,)
        response = format_input.lambda_handler(event, {})

    assert response["next-step"] == "exit-ok"
    assert response["message"] == f"No fulltexts found for run '{run_id}'."


def test_lambda_handler_with_next_step_fulltexts_load_fulltexts_present(run_id):
    """Harvested fulltexts yields bulk-update-fulltexts command."""
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "fulltexts-load",
        "source": "dspace",
        "run-id": run_id,
    }

    with patch("lambdas.format_input.TIMDEXDataset") as mock_dataset:
        mock_dataset.return_value.conn.query.return_value.fetchone.return_value = (42,)
        response = format_input.lambda_handler(event, {})

    assert response["next-step"] == "end"
    assert response["fulltexts"] == {
        "load": {
            "bulk-update-fulltexts-command": [
                "--verbose",
                "bulk-update-fulltexts",
                "--source=dspace",
                f"--run-id={run_id}",
                "s3://test-timdex-bucket/dataset",
            ],
        }
    }


def test_lambda_handler_with_next_step_finalize_attaches_metrics(run_id):
    """Finalize ends the run and attaches run metrics to the output."""
    run_metrics = {
        "records": {"count": 42, "actions": {"index": 42}},
        "embeddings": {"count": 42},
        "fulltexts": {"count": 10},
    }
    event = {
        "run-date": "2022-01-02",
        "run-type": "daily",
        "next-step": "finalize",
        "source": "dspace",
        "run-id": run_id,
    }

    with patch(
        "lambdas.helpers.get_run_metrics", return_value=run_metrics
    ) as _mocked_metrics:
        response = format_input.lambda_handler(event, {})

    assert response["next-step"] == "end"
    assert response["metrics"] == run_metrics
