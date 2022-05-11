import unittest

from format import lambda_handler


class TestFormatLambdaHandler(unittest.TestCase):

    def test_format_input_daily_update(self):
        self.maxDiff = None
        test_input = {
            "harvest-type": "update",
            "starting-step": "harvest",
            "time": "2022-03-10T16:30:23Z",
            "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
            "oai-metadata-format": "oai_ead",
            "oai-set-spec": "collection_1",
            "source": "aspace",
            "output-bucket": "YOURBUCKET",
            "elasticsearch-url": "https://YOUR-ES-URL",
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
            "transform": {
                "commands": [
                    "--input-file=s3://YOURBUCKET/aspace-daily-harvest-oai_ead-"
                    "2022-03-09.xml",
                    "--output-file=s3://YOURBUCKET/aspace-daily-timdex-json-2022-03-09.json",
                    "--source=aspace"
                ],
                "result-file": {
                    "bucket": "YOURBUCKET",
                    "key": "aspace-daily-timdex-json-2022-03-09.json",
                }
            },
            "load": {
                "commands": [
                    "--url=https://YOUR-ES-URL",
                    "ingest",
                    "--source=aspace",
                    "s3://YOURBUCKET/aspace-daily-timdex-json-2022-03-09.json"
                ]
            },
            "starting-step": "harvest"
        }

        self.assertEqual(lambda_handler(test_input, {}), expected_output)

    def test_format_input_full(self):
        self.maxDiff = None
        test_input = {
            "harvest-type": "full",
            "starting-step": "load",
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
                    "--exclude-deleted",
                ],
                "result-file": {
                    "bucket": "YOURBUCKET",
                    "key": "aspace-full-harvest-oai_ead-2022-03-10.xml"
                }
            },
            "transform": {
                "commands": [
                    "--input-file=s3://YOURBUCKET/aspace-full-harvest-oai_ead-"
                    "2022-03-10.xml",
                    "--output-file=s3://YOURBUCKET/aspace-full-timdex-json-2022-03-10.json",
                    "--source=aspace"
                ],
                "result-file": {
                    "bucket": "YOURBUCKET",
                    "key": "aspace-full-timdex-json-2022-03-10.json",
                }
            },
            "load": {
                "commands": [
                    "--url=https://YOUR-ES-URL",
                    "ingest",
                    "--source=aspace",
                    "--new",
                    "--auto",
                    "s3://YOURBUCKET/aspace-full-timdex-json-2022-03-10.json"
                ]
            },
            "starting-step": "load"
        }

        self.assertEqual(lambda_handler(test_input, {}), expected_output)

    def test_invalid_starting_step_raises_error(self):
        test_input = {
            "harvest-type": "update",
            "starting-step": "wrong",
            "time": "2022-03-10T16:30:23Z",
            "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
            "oai-metadata-format": "oai_ead",
            "oai-set-spec": "collection_1",
            "source": "aspace",
            "output-bucket": "YOURBUCKET",
            "elasticsearch-url": "https://YOUR-ES-URL"
        }
        with self.assertRaises(ValueError):
            lambda_handler(test_input, {})

    def test_invalid_harvest_type_raises_error(self):
        test_input = {
            "harvest-type": "wrong",
            "starting-step": "transform",
            "time": "2022-03-10T16:30:23Z",
            "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
            "oai-metadata-format": "oai_ead",
            "oai-set-spec": "collection_1",
            "source": "aspace",
            "output-bucket": "YOURBUCKET",
            "elasticsearch-url": "https://YOUR-ES-URL"
        }
        with self.assertRaises(ValueError):
            lambda_handler(test_input, {})


if __name__ == '__main__':
    unittest.main()
