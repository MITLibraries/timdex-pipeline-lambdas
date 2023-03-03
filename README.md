# TIMDEX Pipeline Lambdas

TIMDEX Pipeline Lambdas is a collection of lambdas used in the TIMDEX Ingest Pipeline.

## Required env variables

- `TIMDEX_ALMA_EXPORT_BUCKET_ID`: the name of the Alma SFTP export S3 bucket, set by Terraform on AWS.
- `TIMDEX_S3_EXTRACT_BUCKET_ID`: the name of the TIMDEX pipeline S3 bucket, set by Terraform on AWS.
- `WORKSPACE`: set to `dev` for local development, set by Terraform on AWS.

## Format Input Handler

Takes input JSON (usually from EventBridge although it can be passed to a manual Step Function execution), and returns reformatted JSON matching the expected input data needed for the remaining steps in the TIMDEX pipeline Step Function.

### Input fields

#### Required fields

- `next-step`: The next step of the pipeline to be performed, must be one of `["extract", "transform", "load"]`. Determines which task run commands will be generated as output from the format lambda.
- `run-date`: Must be in one of the formats ["yyyy-mm-dd", "yyyy-mm-ddThh:mm:ssZ"]. The provided date is used in the input/output file naming scheme for all steps of the pipeline.
- `run-type`: Must be one of `["full", "daily"]`. The provided run type is used in the input/output file naming scheme for all steps of the pipeline. It also determines logic for both the OAI-PMH harvest and load commands as follows:
  - `full`: Perform a full harvest of all records from the provided `oai-pmh-host`. During load, create a new OpenSearch index, load all records into it, and then promote the new index.
  - `daily`: Harvest only records added to or updated in the provided `oai-pmh-host` since the previous calendar day. Previous day is relative to the provided `run-date` field date, *not* the date this process is run, although those will be equivalent in most cases. During load, index/delete records into the current production OpenSearch index for the source.
- `source`: Short name for the source repository, must match one of the source names configured for use in transform and load apps. The provided source is passed to the transform and load app CLI commands, and is also used in the input/output file naming scheme for all steps of the pipeline.
  - *Note*: if provided source is "aspace" or "dspace", a method option is passed to the harvest command (if starting at the extract step) to ensure that we use the "get" harvest method instead of the default "list" method used for all other sources. This is required because ArchivesSpace inexplicably provides incomplete oai-pmh responses using the "list" method and DSpace@MIT needs to skip some records by ID, which can only be done using the "get" method.

#### Required OAI-PMH harvest fields

- `oai-pmh-host`: *required if next-step is extract via OAI-PMH harvest*, not needed otherwise. Should be the base OAI-PMH URL for the source repository.
- `oai-metadata-format`: *required if next-step is extract via OAI-PMH harvest*, optional otherwise. The metadata prefix to use for the OAI-PMH harvest command, must match an available metadata prefix provided by the `oai-pmh-host` (see source repository OAI-PMH documentation for details).

#### Optional fields

- `oai-set-spec`: optional, only used when limiting the OAI-PMH record harvest to a single set from the source repository.
- `verbose`: optional, if provided with value `"true"` (case-insensitive) will pass the `--verbose` option (debug level logging) to all pipeline task run commands.

### Example format input with all fields

```json
{
  "next-step": "extract",
  "run-date": "2022-03-10T16:30:23Z",
  "run-type": "daily",
  "source": "YOURSOURCE",
  "verbose": "true",
  "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
  "oai-metadata-format": "oai_dc",
  "oai-set-spec": "YOUR-SET-SPEC"
}
```

### Example format output from the above input

Note: the output will vary slightly depending on the provided `source`, as these sometimes require different command logic. See test cases for input/output representations of all expected logic.

```json
{
  "next-step": "transform",
  "run-date": "2022-03-10T16:30:23Z",
  "run-type": "daily",
  "source": "YOURSOURCE",
  "verbose": true,
  "extract": {
    "extract-command": [
      "--host=https://YOUR-OAI-SOURCE/oai",
      "--output-file=s3://TIMDEX-BUCKET-FROM-ENV/YOURSOURCE/YOURSOURCE-2022-03-09-daily-extracted-records-to-index.xml",
      "--verbose",
      "harvest",
      "--metadata-format=oai_dc",
      "--set-spec=YOUR-SET-SPEC",
      "--from-date=2022-03-09"
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
docker run -e TIMDEX_ALMA_EXPORT_BUCKET_ID=alma-bucket-name -e TIMDEX_S3_EXTRACT_BUCKET_ID=timdex-bucket-name -e WORKSPACE=dev -p 9000:8080 timdex-pipeline-lambdas-dev:latest
```

### POST to the container

Note: running this with next-step transform or load involves an actual S3 connection and is thus tricky to test locally. Better to push the image to Dev1 and test there.

```bash
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{
  "next-step": "extract",
  "run-date": "2022-03-10T16:30:23Z",
  "run-type": "daily",
  "source": "YOURSOURCE",
  "verbose": "true",
  "oai-pmh-host": "https://YOUR-OAI-SOURCE/oai",
  "oai-metadata-format": "oai_dc",
  "oai-set-spec": "YOUR-SET-SPEC"
}'
```

### Observe output

```json
{
  "run-date": "2022-03-10",
  "run-type": "daily",
  "source": "YOURSOURCE",
  "verbose": true,
  "next-step": "transform",
  "extract": {
    "extract-command": [
      "--host=https://YOUR-OAI-SOURCE/oai",
      "--output-file=s3://timdex-bucket-name/YOURSOURCE/YOURSOURCE-2022-03-09-daily-extracted-records-to-index.xml",
      "--verbose",
      "harvest",
      "--metadata-format=oai_dc",
      "--set-spec=YOUR-SET-SPEC",
      "--from-date=2022-03-09"
    ]
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
