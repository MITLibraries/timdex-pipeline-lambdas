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
- `run-id`: an ETL run id that gets included for CLI commands generated; minted if not provided
- `run-timestamp`: an ETL timestamp that gets included for CLI commands generated; minted if not provided

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

## Development

* To preview a list of available Makefile commands: `make help`
* To install with dev dependencies: `make install`
* To update dependencies: `make update`
* To run unit tests: `make test`
* To lint the repo: `make lint`

The Makefile also includes account specific `dist`, `publish`, and `update-format-lambda` commands.

The `update-format-lambda` is required anytime an image contains a change to the format function is published to the ECR in order for the Format Input Lambda to use the updated code.

GitHub Actions is configured to update the Lambda function with every push to the `main` branch.

### Running Locally with [AWS SAM](https://aws.amazon.com/serverless/sam/)

Ensure that AWS SAM CLI is installed: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html.

All following actions and commands should be performed from the root of the project (i.e. same directory as the `Dockerfile`).

1- Create an environment variables override file:

```shell
cp tests/sam/env.json.template tests/sam/env.json
```

Then update as needed.  Defaults are okay for `extract` commands, but real bucket names will be needed for `transform` and `load` commands.

**NOTE:** AWS credentials are automatically passed from the terminal context where `sam invoke ...` is called; they do not need to be explicitly set as env vars in the `env.json` file that provides container overrides.

2- Build the SAM docker image:

```shell
make sam-build
```

3- Run a test invocation:
```shell
make sam-example-libguides-extract
```

Note the final lines of the output in the terminal is what the lambda would have returned:

```json
{"run-date": "2025-10-14", "run-type": "full", "source": "libguides", "verbose": true, "harvester-type": "oai", "next-step": "transform", "extract": {"extract-command": ["--verbose", "--host=https://libguides.mit.edu/oai.php", "--output-file=s3://timdex-bucket/libguides/libguides-2025-10-14-full-extracted-records-to-index.xml", "harvest", "--metadata-format=oai_dc", "--exclude-deleted", "--set-spec=guides"]}}
```

4- Run your own, custom invocation by preparing a JSON payload.  This can be achieved either by passing a JSON file like the `Makefile` example `sam-example-libguides-extract` does, or by providing a `stdin` JSON string like this:

```shell
echo '{"next-step": "extract", "run-date": "2025-10-14", "run-type": "full", "source": "libguides", "verbose": "true", "oai-pmh-host": "https://libguides.mit.edu/oai.php", "oai-metadata-format": "oai_dc", "oai-set-spec": "guides", "run-id": "abc123"}' | sam local invoke -e -
```

Note that `--env-vars tests/sam/env.json` was not passed or needed.  The [template YAML file](tests/sam/template.yaml) provides default values for env vars, and because they weren't actually used by the `extract` command generation work, the defaults were fine.  Overrides to sensitive env vars are generally only needed when they will actually be used.

## Environment Variables

### Required

```shell
TIMDEX_ALMA_EXPORT_BUCKET_ID=### The name of the Alma SFTP export S3 bucket, set by Terraform on AWS.
TIMDEX_S3_EXTRACT_BUCKET_ID=### The name of the TIMDEX pipeline S3 bucket, set by Terraform on AWS.
WORKSPACE=### Set to `dev` for local development; this will be set to `stage` and `prod` in those environments by Terraform on AWS.
```

### Optional

None at this time.



