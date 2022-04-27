import format_input


def test_format_input_daily_update():
    test_input = {
        "harvest-type": "update",
        "time": "2022-03-10T16:30:23Z",
        "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
        "oai-metadata-format": "oai_ead",
        "oai-set-spec": "collection_1",
        "source": "aspace",
        "output-bucket": "YOURBUCKET",
        "elasticsearch-url": "https://YOUR-ES-URL"
    }

    expected_output = {
        "harvest": {
            "commands": [
                "--host=https://YOUR-OAI-SOURCE/oai",
                "--output-file=s3://YOURBUCKET/aspace-daily-harvest-oai_ead-"
                "2022-03-09.xml",
                "harvest",
                "--metadata-format=oai_ead",
                "--from-date=2022-03-09",
                "--until-date=2022-03-09",
                "--exclude-deleted",
                "--set-spec=collection_1"
            ],
            "result-file": {
                "bucket": "YOURBUCKET",
                "key": "aspace-daily-harvest-oai_ead-2022-03-09.xml"
            }
        },
        "ingest": {
            "commands": [
                "--url=https://YOUR-ES-URL",
                "ingest",
                "--source=aspace",
                "s3://YOURBUCKET/aspace-daily-harvest-oai_ead-2022-03-09.xml"
            ]
        }
    }
    assert expected_output == format_input.lambda_handler(test_input, {})


def test_format_input_full():
    test_input = {
        "harvest-type": "full",
        "time": "2022-03-10T16:30:23Z",
        "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
        "oai-metadata-format": "oai_ead",
        "source": "aspace",
        "output-bucket": "YOURBUCKET",
        "elasticsearch-url": "https://YOUR-ES-URL"
    }

    expected_output = {
        "harvest": {
            "commands": [
                "--host=https://YOUR-OAI-SOURCE/oai",
                "--output-file=s3://YOURBUCKET/aspace-full-harvest-oai_ead-"
                "2022-03-10.xml",
                "harvest",
                "--metadata-format=oai_ead",
                "--exclude-deleted"
            ],
            "result-file": {
                "bucket": "YOURBUCKET",
                "key": "aspace-full-harvest-oai_ead-2022-03-10.xml"
            }
        },
        "ingest": {
            "commands": [
                "--url=https://YOUR-ES-URL",
                "ingest",
                "--source=aspace",
                "--new",
                "--auto",
                "s3://YOURBUCKET/aspace-full-harvest-oai_ead-2022-03-10.xml"
            ]
        }
    }
    assert expected_output == format_input.lambda_handler(test_input, {})
