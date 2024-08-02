import pytest

from ska_sdp_piper.piper.model.named_instance import NamedInstance


@pytest.fixture(scope="function", autouse=True)
def clear_named_instances():
    NamedInstance._instances = dict()


class NamedInstanceClass(metaclass=NamedInstance):
    def __init__(self, name, _existing_instance_=False):
        pass


class NamedInstanceClass2(metaclass=NamedInstance):
    def __init__(self, name, _existing_instance_=False):
        pass


def test_should_create_single_named_instance_for_a_class():
    instance_1 = NamedInstanceClass("instance_1")
    instance_2 = NamedInstanceClass2("instance_1")
    assert instance_1 == NamedInstanceClass(
        "instance_1", _existing_instance_=True
    )
    assert instance_2 == NamedInstanceClass2(
        "instance_1", _existing_instance_=True
    )
    assert instance_1 != instance_2


def test_should_overwrite_instance():
    instance_1 = NamedInstanceClass("instance_1")
    instance_2 = NamedInstanceClass("instance_1")
    assert instance_2 == NamedInstanceClass(
        "instance_1", _existing_instance_=True
    )
    assert instance_1 != NamedInstanceClass(
        "instance_1", _existing_instance_=True
    )


def test_should_return_none_if_name_doesnt_exist():
    instance_1 = NamedInstanceClass("instance_1", _existing_instance_=True)
    assert instance_1 is None
