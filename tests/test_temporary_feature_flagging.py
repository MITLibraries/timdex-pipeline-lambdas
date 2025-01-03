# ruff: noqa: PLR2004

from unittest.mock import patch

import pytest

from lambdas import format_input
from lambdas.config import get_etl_version

# NOTE: FEATURE FLAG: this test file can be removed completely after v2 parquet work.


@pytest.fixture
def mocked_etl_v1_generate_transform_commands_method():
    with patch(
        "lambdas.commands._etl_v1_generate_transform_commands_method"
    ) as mocked_method:
        yield mocked_method


@pytest.fixture
def mocked_etl_v2_generate_transform_commands_method():
    with patch(
        "lambdas.commands._etl_v2_generate_transform_commands_method"
    ) as mocked_method:
        yield mocked_method


@pytest.fixture
def mocked_etl_v1_generate_load_commands_method():
    with patch("lambdas.commands.generate_load_commands_v1") as mocked_method:
        yield mocked_method


@pytest.fixture
def mocked_etl_v2_generate_load_commands_method():
    with patch("lambdas.commands.generate_load_commands") as mocked_method:
        yield mocked_method


def test_etl_version_helper_function_no_env_var_is_v1(monkeypatch):
    monkeypatch.delenv("ETL_VERSION")
    assert get_etl_version() == 1


def test_etl_version_helper_function_env_var_is_1_is_v1(monkeypatch):
    monkeypatch.setenv("ETL_VERSION", "1")
    assert get_etl_version() == 1


def test_etl_version_helper_function_env_var_is_2_is_v2(monkeypatch):
    monkeypatch.setenv("ETL_VERSION", "2")
    assert get_etl_version() == 2


@pytest.mark.parametrize(
    "env_value",
    [
        "pumpkin_pie",  # throws ValueError because not integer
        "3",  # throws ValueError because not 1 or 2
    ],
)
def test_etl_version_helper_function_env_var_value_is_unsupported(env_value, monkeypatch):
    monkeypatch.setenv("ETL_VERSION", env_value)
    with pytest.raises(ValueError):  # noqa: PT011
        get_etl_version()


def test_lambda_handler_etl_version_v1_with_next_step_transform_invokes_v1_code(
    mocked_etl_v1_generate_transform_commands_method, s3_client
):
    s3_client.put_object(
        Bucket="test-timdex-bucket",
        Key="testsource/testsource-2022-01-02-daily-extracted-records-to-index.xml",
        Body="I am a file",
    )
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "transform",
        "source": "testsource",
        "verbose": "true",
    }
    format_input.lambda_handler(event, {})
    mocked_etl_v1_generate_transform_commands_method.assert_called()


def test_lambda_handler_etl_version_v2_with_next_step_transform_invokes_v2_code(
    mocked_etl_v2_generate_transform_commands_method, monkeypatch, s3_client
):
    monkeypatch.setenv("ETL_VERSION", "2")
    s3_client.put_object(
        Bucket="test-timdex-bucket",
        Key="testsource/testsource-2022-01-02-daily-extracted-records-to-index.xml",
        Body="I am a file",
    )
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "transform",
        "source": "testsource",
        "verbose": "true",
    }
    format_input.lambda_handler(event, {})
    mocked_etl_v2_generate_transform_commands_method.assert_called()


def test_lambda_handler_etl_version_v1_with_next_step_load_invokes_v1_code(
    mocked_etl_v1_generate_load_commands_method, s3_client
):
    s3_client.put_object(
        Bucket="test-timdex-bucket",
        Key="testsource/testsource-2022-01-02-daily-transformed-records-to-index.json",
        Body="I am a file",
    )
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "load",
        "source": "testsource",
    }
    format_input.lambda_handler(event, {})
    mocked_etl_v1_generate_load_commands_method.assert_called()


def test_lambda_handler_etl_version_v2_with_next_step_load_invokes_v2_code(
    mocked_etl_v2_generate_load_commands_method, monkeypatch, s3_client
):
    monkeypatch.setenv("ETL_VERSION", "2")
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "load",
        "source": "testsource",
    }
    with patch(
        "lambdas.helpers.dataset_records_exist_for_run",
        return_value=True,
    ) as _mocked_record_count:
        format_input.lambda_handler(event, {})
        mocked_etl_v2_generate_load_commands_method.assert_called()
