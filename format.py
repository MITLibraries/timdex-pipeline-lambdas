import datetime


def lambda_handler(event, context):

    run_date = datetime.datetime.strptime(event["time"], "%Y-%m-%dT%H:%M:%SZ")
    formatted_run_date = run_date.strftime("%Y-%m-%d")

    harvest_date = run_date - datetime.timedelta(days=1)
    formatted_harvest_date = harvest_date.strftime("%Y-%m-%d")

    harvest_type = event["harvest-type"]

    if harvest_type == "update":
        output_file = (
            f"{event['source']}-daily-harvest-{event['oai-metadata-format']}"
            f"-{formatted_harvest_date}.xml"
        )
        harvest_commands = [
            f"--host={event['oai-pmh-host']}",
            f"--output-file=s3://{event['output-bucket']}/{output_file}",
            "harvest",
            f"--metadata-format={event['oai-metadata-format']}",
            f"--from-date={formatted_harvest_date}",
            f"--until-date={formatted_harvest_date}",
            "--exclude-deleted",
        ]
        if event.get("oai-set-spec"):
            harvest_commands.append(f"--set-spec={event['oai-set-spec']}")

        output = {
            "harvest": {
                "commands": harvest_commands,
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
            f"-{formatted_run_date}.xml"
        )
        harvest_commands = [
            f"--host={event['oai-pmh-host']}",
            f"--output-file=s3://{event['output-bucket']}/{output_file}",
            "harvest",
            f"--metadata-format={event['oai-metadata-format']}",
            "--exclude-deleted",
        ]
        if event.get("oai-set-spec"):
            harvest_commands.append(f"--set-spec={event['oai-set-spec']}")

        output = {
            "harvest": {
                "commands": harvest_commands,
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
