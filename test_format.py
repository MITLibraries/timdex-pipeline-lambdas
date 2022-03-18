import format


def test_format():
    input = {
        "harvest-type": "update",
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
                "--out=s3://YOURBUCKET/aspace-daily-harvest-oai_ead-2022-03-09.xml",
                "harvest",
                "--from_date=2022-03-09",
                "--until=2022-03-09",
                "--format=oai_ead"
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
    assert expected_output == format.lambda_handler(input, {})
