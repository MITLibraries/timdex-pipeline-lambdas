import pytest

from lambdas import config


def test_verbosity_returns_bool_if_bool():
    assert config.check_verbosity(True) is True


def test_verbosity_returns_true_if_true_string():
    assert config.check_verbosity("True") is True


def test_verbosity_returns_false_if_false_string():
    assert config.check_verbosity("Anything-but-true") is False


def test_validate_input_missing_required_field_raises_error():
    event = {
        "next-step": "transform",
        "run-type": "full",
        "source": "testsource",
    }
    with pytest.raises(ValueError) as error:
        config.validate_input(event)
    assert (
        "Input must include all required fields. Missing fields: ['run-date']"
        in str(error.value)
    )


def test_validate_input_with_invalid_next_step_raises_error():
    event = {
        "next-step": "wrong",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
    }
    with pytest.raises(ValueError) as error:
        config.validate_input(event)
    assert (
        "Input 'next-step' value must be one of: "
        f"{config.VALID_STEPS}. Value provided was 'wrong'" in str(error.value)
    )


def test_validate_input_with_invalid_run_type_raises_error():
    event = {
        "next-step": "load",
        "run-date": "2022-01-02",
        "run-type": "wrong",
        "source": "testsource",
    }
    with pytest.raises(ValueError) as error:
        config.validate_input(event)
    assert (
        f"Input 'run-type' value must be one of: {config.VALID_RUN_TYPES}. "
        "Value provided was 'wrong'" in str(error.value)
    )


def test_validate_input_with_missing_harvest_fields_raises_error():
    event = {
        "next-step": "extract",
        "run-date": "2022-01-02:T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
        "oai-pmh-host": "https://example.com/oai",
    }
    with pytest.raises(ValueError) as error:
        config.validate_input(event)
    assert (
        "Input must include all required harvest fields when starting with "
        "harvest step. Missing fields: ['oai-metadata-format']" in str(error.value)
    )


def test_validate_input_with_all_required_fields_returns_none():
    event = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
    }
    assert config.validate_input(event) is None


def test_validate_input_with_all_required_harvest_fields_returns_none():
    event = {
        "next-step": "extract",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
    }
    assert config.validate_input(event) is None


def test_verify_env_missing_env_raises_error(monkeypatch):
    monkeypatch.delenv("WORKSPACE", raising=False)
    with pytest.raises(RuntimeError) as error:
        config.verify_env()
    assert "Required env variable WORKSPACE is not set" in str(error)


def test_verify_env_all_present_returns_none():
    assert config.verify_env() is None
