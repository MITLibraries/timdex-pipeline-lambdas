# TIMDEX Pipeline Lambdas

TIMDEX Pipeline Lambdas is a collection of lambdas used in the TIMDEX Ingest Pipeline.

## Format Input Handler

Takes input JSON (usually from EventBridge although it can be passed to a manual Step Function execution), and returns reformatted JSON matching the expected input data needed for the remaining steps in the TIMDEX pipeline Step Function.

### Event Fields

#### Required

- `next-step`: The next step of the pipeline to be performed, must be one of `["extract", "transform", "load"]`. Determines which task run commands will be generated as output from the format lambda.
- `run-date`: Must be in one of the formats ["yyyy-mm-dd", "yyyy-mm-ddThh:mm:ssZ"]. The provided date is used in the input/output file naming scheme for all steps of the pipeline.
- `run-type`: Must be one of `["full", "daily"]`. The provided run type is used in the input/output file naming scheme for all steps of the pipeline. It also determines logic for both the OAI-PMH harvest and load commands as follows:
  - `full`: Perform a full harvest of all records from the provided `oai-pmh-host`. During load, create a new OpenSearch index, load all records into it, and then promote the new index.
  - `daily`: Harvest only records added to or updated in the provided `oai-pmh-host` since the previous calendar day. Previous day is relative to the provided `run-date` field date, *not* the date this process is run, although those will be equivalent in most cases. During load, index/delete records into the current production OpenSearch index for the source.
- `source`: Short name for the source repository, must match one of the source names configured for use in transform and load apps. The provided source is passed to the transform and load app CLI commands, and is also used in the input/output file naming scheme for all steps of the pipeline.
  - *Note*: if provided source is "aspace" or "dspace", a method option is passed to the harvest command (if starting at the extract step) to ensure that we use the "get" harvest method instead of the default "list" method used for all other sources. This is required because ArchivesSpace inexplicably provides incomplete oai-pmh responses using the "list" method and DSpace@MIT needs to skip some records by ID, which can only be done using the "get" method.

#### Required for OAI-PMH Harvest

- `oai-pmh-host`: *required if next-step is extract via OAI-PMH harvest*, not needed otherwise. Should be the base OAI-PMH URL for the source repository.
- `oai-metadata-format`: *required if next-step is extract via OAI-PMH harvest*, optional otherwise. The metadata prefix to use for the OAI-PMH harvest command, must match an available metadata prefix provided by the `oai-pmh-host` (see source repository OAI-PMH documentation for details).

#### Optional Fields

- `oai-set-spec`: optional, only used when limiting the OAI-PMH record harvest to a single set from the source repository.
- `verbose`: optional, if provided with value `"true"` (case-insensitive) will pass the `--verbose` option (debug level logging) to all pipeline task run commands.

### Example Format Input Event

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

### Example Format Input Result

Note: This result is based on the previous example.

The output will vary slightly depending on the provided `source`, as these sometimes require different command logic. See test cases for input/output representations of all expected logic.

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

### Example Ping Event

```json
{}
```

### Example Ping Result

```json
pong
```

## Development

* To preview a list of available Makefile commands: `make help`
* To install with dev dependencies: `make install`
* To update dependencies: `make update`
* To run unit tests: `make test`
* To lint the repo: `make lint`

The Makefile also includes account specific `dist`, `publish`, and `update-format-lambda` commands.

The `update-format-lambda` is required anytime an image contains a change to the format function is published to the ECR in order for the Format Input Lambda to use the updated code.

GitHub Actions is configured to update the Lambda function with every push to the `main` branch.

### Running Locally with Docker

- Build the container

  ```bash
  make dist-dev
  ```

- Run the default handler for the container

   ```bash
   docker run -e TIMDEX_ALMA_EXPORT_BUCKET_ID=alma-bucket-name \
   -e TIMDEX_S3_EXTRACT_BUCKET_ID=timdex-bucket-name \
   -e WORKSPACE=dev \
   -p 9000:8080 timdex-pipeline-lambdas-dev:latest
   ```

- POST to the container
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
  
- Observe output
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

### Running a Specific Handler Locally with Docker
You can call any handler you copy into the container (see Dockerfile) by name as part of the `docker run` command.

```bash
docker run -p 9000:8080 timdex-pipeline-lambdas-dev:latest lambdas.ping.lambda_handler
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

## Environment Variables

### Required

```shell
TIMDEX_ALMA_EXPORT_BUCKET_ID=### The name of the Alma SFTP export S3 bucket, set by Terraform on AWS.
TIMDEX_S3_EXTRACT_BUCKET_ID=### The name of the TIMDEX pipeline S3 bucket, set by Terraform on AWS.
WORKSPACE=### Set to `dev` for local development; this will be set to `stage` and `prod` in those environments by Terraform on AWS.
```

### Optional

```shell
ETL_VERSION=### Version number of the TIMDEX ETL infrastructure. This can be used to align application behavior with the requirements of other applications in the TIMDEX ETL pipeline.
```



