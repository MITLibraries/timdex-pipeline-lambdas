import logging
import os
from typing import Any, ClassVar


class Config:
    REQUIRED_ENV_VARS = (
        "TIMDEX_ALMA_EXPORT_BUCKET_ID",
        "TIMDEX_S3_EXTRACT_BUCKET_ID",
        "WORKSPACE",
    )
    OPTIONAL_ENV_VARS = ()

    GIS_SOURCES = ("gismit", "gisogm")
    INDEX_ALIASES: ClassVar = {
        "rdi": ["jpal", "whoas", "zenodo"],
        "timdex": ["alma", "aspace", "dspace"],
        "geo": GIS_SOURCES,
    }
    REQUIRED_FIELDS = ("next-step", "run-date", "run-type", "source")
    REQUIRED_OAI_HARVEST_FIELDS = ("oai-pmh-host", "oai-metadata-format")
    VALID_DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ")
    VALID_RUN_TYPES = ("full", "daily")
    VALID_STEPS = ("extract", "transform", "load")

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        """Provide dot notation access to configurations and env vars on this class."""
        if name in self.REQUIRED_ENV_VARS or name in self.OPTIONAL_ENV_VARS:
            return os.getenv(name)
        message = f"'{name}' not a valid configuration variable"
        raise AttributeError(message)

    def check_required_env_vars(self) -> None:
        """Method to raise exception if required env vars not set."""
        missing_vars = [var for var in self.REQUIRED_ENV_VARS if not os.getenv(var)]
        if missing_vars:
            message = f"Missing required environment variables: {', '.join(missing_vars)}"
            raise OSError(message)

    @staticmethod
    def check_verbosity(verbose: bool | str) -> bool:  # noqa: FBT001
        """Determine whether verbose is True or False given a boolean or string value."""
        if isinstance(verbose, bool):
            return verbose
        return verbose.lower() == "true"

    @classmethod
    def configure_logger(
        cls,
        root_logger: logging.Logger,
        *,
        verbose: bool = False,
        warning_only_loggers: str | None = None,
    ) -> str:
        """Configure application via passed application root logger.

        If verbose=True, 3rd party libraries can be quite chatty.  For convenience, they
        can be set to WARNING level by either passing a comma seperated list of logger
        names to 'warning_only_loggers' or by setting the env var WARNING_ONLY_LOGGERS.
        """
        if verbose:
            root_logger.setLevel(logging.DEBUG)
            logging_format = (
                "%(asctime)s %(levelname)s %(name)s.%(funcName)s() "
                "line %(lineno)d: %(message)s"
            )
        else:
            root_logger.setLevel(logging.INFO)
            logging_format = (
                "%(asctime)s %(levelname)s %(name)s.%(funcName)s(): %(message)s"
            )

        warning_only_loggers = os.getenv("WARNING_ONLY_LOGGERS", warning_only_loggers)
        if warning_only_loggers:
            for name in warning_only_loggers.split(","):
                logging.getLogger(name).setLevel(logging.WARNING)

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(logging_format))
        root_logger.addHandler(handler)

        return (
            f"Logger '{root_logger.name}' configured with level="
            f"{logging.getLevelName(root_logger.getEffectiveLevel())}"
        )

    @classmethod
    def validate_input(cls, input_data: dict) -> None:
        """Validate input to the lambda function.

        Ensures that all requiered input fields are present and contain valid data.
        """
        # All required fields are present
        if missing_fields := [
            field for field in cls.REQUIRED_FIELDS if field not in input_data
        ]:
            message = (
                "Input must include all required fields. "
                f"Missing fields: {missing_fields}"
            )
            raise ValueError(message)

        # Valid next step
        next_step = input_data["next-step"]
        if next_step not in cls.VALID_STEPS:
            message = (
                f"Input 'next-step' value must be one of: {cls.VALID_STEPS}. Value "
                f"provided was '{next_step}'"
            )
            raise ValueError(message)

        # Valid run type
        run_type = input_data["run-type"]
        if run_type not in cls.VALID_RUN_TYPES:
            message = (
                f"Input 'run-type' value must be one of: {cls.VALID_RUN_TYPES}. Value "
                f"provided was '{run_type}'"
            )
            raise ValueError(message)

        # If next step is extract step, required harvest fields are present
        # ruff: noqa: SIM102
        if input_data["next-step"] == "extract":
            if input_data["source"] not in cls.GIS_SOURCES:
                if missing_harvest_fields := [
                    field
                    for field in cls.REQUIRED_OAI_HARVEST_FIELDS
                    if field not in input_data
                ]:
                    message = (
                        "Input must include all required harvest fields when starting "
                        f"with harvest step. Missing fields: {missing_harvest_fields}"
                    )
                    raise ValueError(message)
