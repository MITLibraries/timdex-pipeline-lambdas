import datetime


def lambda_handler(event, context):

    run_date = datetime.datetime.strptime(event["time"], "%Y-%m-%dT%H:%M:%SZ")
    harvest_date = run_date - datetime.timedelta(days=1)
    formatted_harvest_date = harvest_date.strftime("%Y-%m-%d")

    harvest_type = event["harvest-type"]

    if harvest_type == "update":
        output_file = (
            f"{event['source']}-daily-harvest-{event['oai-metadata-format']}"
            f"-{formatted_harvest_date}.xml"
        )
        output = {
            "harvest": {
                "commands": [
                    f"--host={event['oai-pmh-host']}",
                    f"--out=s3://{event['output-bucket']}/{output_file}",
                    "harvest",
                    f"--from_date={formatted_harvest_date}",
                    f"--until={formatted_harvest_date}",
                    f"--format={event['oai-metadata-format']}"
                ],
                "result-file": {
                    "bucket": event['output-bucket'],
                    "key": output_file
                }
            },
            "ingest": {
                "commands": [
                    f"--url={event['elasticsearch-url']}",
                    "ingest",
                    f"--source={event['source']}",
                    f"s3://{event['output-bucket']}/{output_file}"
                ]
            }
        }

    elif harvest_type == "full":
        output_file = (
            f"{event['source']}-full-harvest-{event['oai-metadata-format']}"
            f"-{formatted_harvest_date}.xml"
        )
        output = {
            "harvest": {
                "commands": [
                    f"--host={event['oai-pmh-host']}",
                    f"--out=s3://{event['output-bucket']}/{output_file}",
                    "harvest",
                    f"--until={formatted_harvest_date}",
                    f"--format={event['oai-metadata-format']}"
                ],
                "result-file": {
                    "bucket": event['output-bucket'],
                    "key": output_file
                }
            },
            "ingest": {
                "commands": [
                    f"--url={event['elasticsearch-url']}",
                    "ingest",
                    f"--source={event['source']}",
                    "--new",
                    "--auto",
                    f"s3://{event['output-bucket']}/{output_file}"
                ]
            }
        }

    return output
