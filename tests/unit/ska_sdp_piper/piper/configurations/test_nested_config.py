import mock
import pytest

from ska_sdp_piper.piper.configurations.nested_config import (
    ConfigParam,
    NestedConfigParam,
)


def test_should_instantiate_a_nested_configuration():
    nested_config_param = NestedConfigParam(
        description="Nested Configuration",
        param1=ConfigParam(int, 1, description="Parameter 1"),
    )

    assert issubclass(nested_config_param.__class__, ConfigParam)
    assert nested_config_param.value == {"param1": 1}

    assert nested_config_param._type == NestedConfigParam


def test_should_get_value_by_key():
    nested_config_param = NestedConfigParam(
        description="Nested Configuration",
        param1=ConfigParam(
            int,
            1,
            description="Parameter 1",
            nullable=False,
            allowed_values=[1, 2, 3],
        ),
        param2=ConfigParam(str, "param 2", description="Parameter 2"),
    )

    assert nested_config_param["param1"] == 1
    assert nested_config_param["param2"] == "param 2"
    assert nested_config_param.get("param3", "default") == "default"
    assert nested_config_param.get("param4") is None


def test_should_update_the_value():
    nested_config_param = NestedConfigParam(
        description="Nested Configuration",
        param1=ConfigParam(
            int,
            1,
            description="Parameter 1",
            nullable=False,
            allowed_values=[1, 2, 3],
        ),
        param2=ConfigParam(str, "param 2", description="Parameter 2"),
    )

    nested_config_param.value = {"param2": "New Value"}

    assert nested_config_param.value == {"param1": 1, "param2": "New Value"}


def test_should_validate_parameter_updates_for_nested_configuration():
    nested_config_param = NestedConfigParam(
        description="Nested Configuration",
        param1=ConfigParam(
            int,
            1,
            description="Parameter 1",
            nullable=False,
            allowed_values=[1, 2, 3],
        ),
        param2=ConfigParam(str, "param 2", description="Parameter 2"),
        param3=NestedConfigParam(
            "param 3",
            param3_1=ConfigParam(int, 1, allowed_values=[1, 2]),
            param3_2=ConfigParam(str, "value"),
        ),
    )

    with pytest.raises(TypeError):
        nested_config_param.value = "New Value"

    with pytest.raises(TypeError):
        nested_config_param.value = {"param1": None}

    with pytest.raises(TypeError):
        nested_config_param.value = {"param1": "New Value"}

    with pytest.raises(ValueError):
        nested_config_param.value = {"param1": 42}

    with pytest.raises(ValueError):
        nested_config_param.value = {"param3": {"param3_1": 42}}


@mock.patch("ska_sdp_piper.piper.configurations.config_groups.logger")
def test_should_warn_for_non_existing_config_key(logging_mock):
    nested_config_param = NestedConfigParam(
        description="Nested Configuration",
        param1=ConfigParam(
            int,
            1,
            description="Parameter 1",
            nullable=False,
            allowed_values=[1, 2, 3],
        ),
        param2=ConfigParam(str, "param 2", description="Parameter 2"),
        param3=NestedConfigParam(
            "param 3",
            param3_1=ConfigParam(int, 1, allowed_values=[1, 2]),
            param3_2=ConfigParam(str, "value"),
        ),
    )

    nested_config_param.value = dict(bad_config="value")

    logging_mock.warning.assert_called_once_with(
        'Property "bad_config" is invalid. Ignoring and continuing the '
        "pipeline."
    )
