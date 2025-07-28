# ruff: noqa: FBT003

import pytest

from lambdas.config import Config

CONFIG = Config()


def test_verbosity_returns_bool_if_bool():
    assert CONFIG.check_verbosity(True) is True


def test_verbosity_returns_true_if_true_string():
    assert CONFIG.check_verbosity("True") is True


def test_verbosity_returns_false_if_false_string():
    assert CONFIG.check_verbosity("Anything-but-true") is False


def test_verify_env_missing_env_raises_error(monkeypatch):
    monkeypatch.delenv("WORKSPACE", raising=False)
    with pytest.raises(
        OSError,
        match="Missing required environment variables: WORKSPACE",
    ):
        CONFIG.check_required_env_vars()


def test_verify_env_all_present_returns_none():
    assert CONFIG.check_required_env_vars() is None
