import logging
import os

INDEX_ALIASES = {
    "rdi": ["jpal", "whoas", "zenodo"],
    "timdex": ["alma", "aspace", "dspace"],
    "geo": ["gismit", "gisogm"],
}
REQUIRED_ENV = {
    "TIMDEX_ALMA_EXPORT_BUCKET_ID",
    "TIMDEX_S3_EXTRACT_BUCKET_ID",
    "WORKSPACE",
}
REQUIRED_FIELDS = ("next-step", "run-date", "run-type", "source")
REQUIRED_HARVEST_FIELDS = ("oai-pmh-host", "oai-metadata-format")
VALID_DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ")
VALID_RUN_TYPES = ("full", "daily")
VALID_STEPS = ("extract", "transform", "load")


def check_verbosity(verbose: bool | str) -> bool:
    """Determine whether verbose is True or False given a boolean or string value."""
    if isinstance(verbose, bool):
        return verbose
    return verbose.lower() == "true"


def configure_logger(
    logger: logging.Logger,
    # ruff: noqa: FBT001
    verbose: bool,
) -> str:
    """Configure provided logger with level debug if verbose, otherwise info."""
    if verbose:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(name)s.%(funcName)s() line %(lineno)d: "
            "%(message)s",
        )
        logger.setLevel(logging.DEBUG)
        for handler in logging.root.handlers:
            handler.addFilter(logging.Filter("lambdas"))
    else:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(name)s.%(funcName)s(): %(message)s"
        )
        logger.setLevel(logging.INFO)
    return (
        f"Logger '{logger.name}' configured with level="
        f"{logging.getLevelName(logger.getEffectiveLevel())}"
    )


def validate_input(input_data: dict) -> None:
    """Validate input to the lambda function.

    Ensures that all requiered input fields are present and contain valid data.
    """
    # All required fields are present
    if missing_fields := [field for field in REQUIRED_FIELDS if field not in input_data]:
        message = (
            f"Input must include all required fields. Missing fields: {missing_fields}"
        )
        raise ValueError(message)

    # Valid next step
    next_step = input_data["next-step"]
    if next_step not in VALID_STEPS:
        message = (
            f"Input 'next-step' value must be one of: {VALID_STEPS}. Value "
            f"provided was '{next_step}'"
        )
        raise ValueError(message)

    # Valid run type
    run_type = input_data["run-type"]
    if run_type not in VALID_RUN_TYPES:
        message = (
            f"Input 'run-type' value must be one of: {VALID_RUN_TYPES}. Value "
            f"provided was '{run_type}'"
        )
        raise ValueError(message)

    # If next step is extract step, required harvest fields are present
    # ruff: noqa: SIM102
    if input_data["next-step"] == "extract":
        if missing_harvest_fields := [
            field for field in REQUIRED_HARVEST_FIELDS if field not in input_data
        ]:
            message = (
                "Input must include all required harvest fields when starting with "
                f"harvest step. Missing fields: {missing_harvest_fields}"
            )
            raise ValueError(message)


def verify_env() -> None:
    """Confirm that required env variables are set."""
    for key in REQUIRED_ENV:
        if not os.getenv(key):
            message = f"Required env variable {key} is not set"
            raise RuntimeError(message)
