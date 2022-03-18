# TIMDEX Pipeline Lambdas

TIMDEX Pipeline Lambdas is a collection of lambdas used in the TIMDEX Ingest Pipeline.

## Format Handler

Takes an input from EventBridge and formats the event data to work throughout the entire ingest pipeline.

### Example Format input

```json
{
  "harvest-type": "update",
  "time": "2022-03-10T16:30:23Z",
  "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
  "oai-metadata-format": "oai_ead",
  "source": "aspace",
  "output-bucket": "YOURBUCKET",
  "elasticsearch-url": "https://YOUR-ES-URL"
}
```

### Example Format output

```json
{
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
```

## Ping Handler

Useful for testing and little else.

### Example Ping input

`{}`

### Example Ping output

`pong`

## Developing locally

<https://docs.aws.amazon.com/lambda/latest/dg/images-test.html>

### Build the container

```bash
docker build -t timdex-pipeline-lambdas .
```

### Run the default handler for the container

```bash
docker run -p 9000:8080 timdex-pipeline-lambdas:latest
```

### POST to the container

```bash
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{
                                      "harvest-type": "update",
                                      "time": "2022-03-10T16:30:23Z",
                                      "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
                                      "oai-metadata-format": "oai_ead",
                                      "source": "aspace",
                                      "output-bucket": "YOURBUCKET",
                                      "elasticsearch-url": "https://YOUR-ES-URL"
                                    }'
```

### Observe output

```json
{
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
```

### Run a different handler in the container

You can call any handler you copy into the container (see Dockerfile) by name as part of the `docker run` command.

```bash
docker run -p 9000:8080 timdex-pipeline-lambdas:latest ping.lambda_handler
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

Should result in `pong` as the output.
