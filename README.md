# TIMDEX Pipeline Lambdas

TIMDEX Pipeline Lambdas is a collection of lambdas used in the TIMDEX Ingest Pipeline.

## Format Input Handler

Takes input JSON (usually from EventBridge although it can be passed to a manual Step Function execution), and returns reformatted JSON matching the expected input data needed for the remaining steps in the TIMDEX pipeline Step Function.

### Input fields

#### Required fields

- `harvest-type`: Must be one of `["full", "daily"]`. The provided harvest type is used in the input/output file naming scheme for all steps of the pipeline. It also determines the harvest logic as follows:
  - `full`: Perform a full harvest of all records from the provided `oai-pmh-host`.
  - `daily`: Harvest only records added to or updated in the provided `oai-pmh-host` during the previous calendar day. Previous day is relative to the provided `time` field date, *not* the date this process is run, although those will be equivalent in most cases.
- `opensearch-url`: The full URL for the OpenSearch instance to ingest records into during the load step.
- `output-bucket`: The name of the S3 bucket (just the name, not the URI) in which to retrieve and deposit input/output files.
- `starting-step`: Must be one of `["harvest", "transform", "load"]`:
  - `harvest`: Start the pipeline at the harvest step and continue through transform and load.
  - `transform`: Skip harvest and start the pipeline at the transform step, using existing harvest output file matching the provided source, harvest type, and time, and continue through load.
  - `load`: Skip harvest and transform steps and start the pipeline at the load step, using existing transform output file matching the provided source, harvest type, and time.
- `source`: Short name for the source repository, must match one of the source names configured for use in transform and load apps. The provided source is passed to the transform and load app CLI commands, and is also used in the input/output file naming scheme for all steps of the pipeline.
  - *Note*: if provided source is "aspace", a method option is passed to the harvest command (if starting at the harvest step) to ensure that we use the "get" harvest method instead of the default "list" method used for all other sources. This is required because ArchivesSpace inexplicably provides incomplete oai-pmh responses using the "list" method.
- `time`: required. Must be in one of the formats ["yyyy-mm-dd", "yyyy-mm-ddThh:mm:ssZ"]. The provided date *minus one day* is used in the input/output file naming scheme for all steps of the pipeline, and as the harvest end date (if starting at the harvest step)

#### Required harvest fields

- `oai-pmh-host`: *required if starting with harvest step*, not needed otherwise. Should be the base OAI-PMH URL for the source repository.
- `oai-metadata-format`: *required if starting with harvest step*, optional otherwise. The metadata prefix to use for the OAI-PMH harvest command, must match an available metadata prefix provided by the `oai-pmh-host` (see source repository OAI-PMH documentation for details).

#### Optional fields

- `index-prefix`: optional, only used for RDI sources. If included, prepends the standard index name with this prefix when identifying or creating an OpenSearch index during the load step.
  - *Note:* The index prefix `"rdi"` *must* be used for RDI sources to enable their inclusion in the RDI UI. The resulting index name will be formatted as `"rdi-source-timestamp"`.
- `oai-set-spec`: optional, only used when limiting the record harvest to a single set from the source repository.
- `verbose`: optional, if provided with value `"true"` (case-insensitive) will pass the `--verbose` option (debug level logging) to harvest and transform steps.

### Example format input with all fields

```json
{
  "harvest-type": "daily",
  "opensearch-url": "https://YOUR-OPENSEARCH-URL",
  "output-bucket": "YOUR-BUCKET",
  "starting-step": "harvest",
  "source": "YOUR-SOURCE",
  "time": "2022-03-10T16:30:23Z",
  "index-prefix": "rdi",
  "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
  "oai-metadata-format": "oai_dc",
  "oai-set-spec": "YOUR-SET-SPEC",
  "verbose": "true"
}
```

### Example format output from the above input

Note: the output will vary slightly depending on the provided `source` and, if provided, `index-prefix`, as these sometimes require different command logic. See test cases for input/output representations of all expected logic.

```json
{
  "starting-step": "harvest",
  "load": {
    "commands": [
      "--url=https://YOUR-OPENSEARCH-URL",
      "ingest",
      "--source=rdi-YOUR-SOURCE",
      "s3://YOUR-BUCKET/YOUR-SOURCE-daily-transformed-records-2022-03-09.json"
      ]
    },
  "transform": {
    "commands": [
      "--input-file=s3://YOUR-BUCKET/YOUR-SOURCE-daily-harvested-records-2022-03-09.xml",
      "--output-file=s3://YOUR-BUCKET/YOUR-SOURCE-daily-transformed-records-2022-03-09.json",
      "--source=YOUR-SOURCE"
    ],
    "result-file": {
      "bucket": "YOUR-BUCKET",
      "key": "YOUR-SOURCE-daily-transformed-records-2022-03-09.json"
    }
  },
  "harvest": {
    "commands": [
      "--host=https://YOUR-OAI-SOURCE/oai",
      "--output-file=s3://YOUR-BUCKET/YOUR-SOURCE-daily-harvested-records-2022-03-09.xml",
      "harvest",
      "--metadata-format=oai_dc",
      "--set-spec=YOUR-SET-SPEC",
      "--from-date=2022-03-09",
      "--until-date=2022-03-09",
      "--exclude-deleted"
    ],
    "result-file": {
      "bucket": "YOUR-BUCKET",
      "key": "YOUR-SOURCE-daily-harvested-records-2022-03-09.xml"
    }
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

### Makefile commands for installation and dependency management

```bash
make install # installs with dev dependencies
make test # runs tests and outputs coverage report
make lint # runs code linting, quality, and safety checks
make update # updates dependencies
```

### Build the container

```bash
make dist-dev
```

### Run the default handler for the container

```bash
docker run -p 9000:8080 timdex-pipeline-lambdas-dev:latest
```

### POST to the container

```bash
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{
  "harvest-type": "daily",
  "opensearch-url": "https://YOUR-OPENSEARCH-URL",
  "output-bucket": "YOUR-BUCKET",
  "starting-step": "harvest",
  "source": "YOUR-SOURCE",
  "time": "2022-03-10T16:30:23Z",
  "index-prefix": "rdi",
  "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
  "oai-metadata-format": "oai_dc",
  "oai-set-spec": "YOUR-SET-SPEC",
  "verbose": "true"
}'
```

### Observe output

```json
{
  "starting-step": "harvest",
  "load": {
    "commands": [
      "--url=https://YOUR-OPENSEARCH-URL",
      "ingest",
      "--source=rdi-YOUR-SOURCE",
      "s3://YOUR-BUCKET/YOUR-SOURCE-daily-transformed-records-2022-03-09.json"
      ]
    },
  "transform": {
    "commands": [
      "--input-file=s3://YOUR-BUCKET/YOUR-SOURCE-daily-harvested-records-2022-03-09.xml",
      "--output-file=s3://YOUR-BUCKET/YOUR-SOURCE-daily-transformed-records-2022-03-09.json",
      "--source=YOUR-SOURCE"
    ],
    "result-file": {
      "bucket": "YOUR-BUCKET",
      "key": "YOUR-SOURCE-daily-transformed-records-2022-03-09.json"
    }
  },
  "harvest": {
    "commands": [
      "--host=https://YOUR-OAI-SOURCE/oai",
      "--output-file=s3://YOUR-BUCKET/YOUR-SOURCE-daily-harvested-records-2022-03-09.xml",
      "harvest",
      "--metadata-format=oai_dc",
      "--set-spec=YOUR-SET-SPEC",
      "--from-date=2022-03-09",
      "--until-date=2022-03-09",
      "--exclude-deleted"
    ],
    "result-file": {
      "bucket": "YOUR-BUCKET",
      "key": "YOUR-SOURCE-daily-harvested-records-2022-03-09.xml"
    }
  }
}
```

### Run a different handler in the container

You can call any handler you copy into the container (see Dockerfile) by name as part of the `docker run` command.

```bash
docker run -p 9000:8080 timdex-pipeline-lambdas-dev:latest lambdas.ping.lambda_handler
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

Should result in `pong` as the output.

## Makefile Use for AWS

The Makefile includes account specific "dist" "publish" and "update-format-lambda" commands.

"Update-format-lambda" is required anytime an image is published to the ECR that contains a change to the format function in order for the format-lambda to use the updated code.

The github action updates this every push to main no matter what changes are made right now.
