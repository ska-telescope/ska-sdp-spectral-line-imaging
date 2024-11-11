import pytest

from ska_sdp_piper.piper.configurations import ConfigParam


def test_should_update_config_param_value():
    config_param = ConfigParam(
        int,
        1,
        description="Number of major cycle iterations. "
        " If 0, only dirty image is generated.",
    )

    config_param.value = 2
    assert config_param.value == 2


def test_should_throw_type_error_if_config_types_dont_match():
    config_param = ConfigParam(
        int,
        1,
        description="Number of major cycle iterations. "
        " If 0, only dirty image is generated.",
    )

    with pytest.raises(TypeError):
        config_param.value = "new_value"


def test_should_check_nullable_values():
    config_param = ConfigParam(
        int,
        1,
        description="Number of major cycle iterations. "
        " If 0, only dirty image is generated.",
        nullable=False,
    )

    with pytest.raises(TypeError):
        config_param.value = None


def test_should_check_for_allowed_values():
    config_param = ConfigParam(
        int,
        1,
        description="Number of major cycle iterations. "
        " If 0, only dirty image is generated.",
        allowed_values=[1, 2, 3],
    )

    with pytest.raises(ValueError):
        config_param.value = 42
