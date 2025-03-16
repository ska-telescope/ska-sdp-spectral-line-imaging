# pragma: exclude file
from typing import Any, List, Literal, NamedTuple

from sphinx.application import Sphinx
from sphinx.config import Config


class SphinxConfiguration(NamedTuple):
    """
    Represents a configuration for a Sphinx extension.
    Please refer to the :func:`sphinx.add_config_value`
    for more description of these attributes.

    Description of the attributes
    -----------------------------
    name: str
        The name of the config
    default: Any
        The default of the config
    rebuild: Literal["env", "html", ""]
        The rebuild mode for the config
    types: List[Any] | Any
        The expected type or types of the config. These
        types are verified by the sphinx post initialization
        of the config.
    """

    name: str
    default: Any  # :noindex:
    rebuild: Literal["env", "html", ""]  # :noindex:
    types: List[Any] | Any  # :noindex:
    # description: Optional[str] = "" # Supported in Sphinx 7.4+


class SphinxExtensionConfig:
    """
    A collection of SphinxConfiguration objects which are defined
    by sphinx extensions. This will allow sphinx extension to
    register their own configuration with the Sphinx application.
    """

    def __init__(self, extension_configs: list[SphinxConfiguration]):
        """
        Parameters
        ----------
        extension_configs: list of SphinxConfiguration
            List of objects which define propeties
            of the configuration
        """
        self.__extension_configs = extension_configs

    def register(self, app: Sphinx):
        """
        Adds configurations to the sphinx app
        """
        for ext_config in self.__extension_configs:
            app.add_config_value(**ext_config._asdict())

    def get_config(self, config: Config) -> dict:
        """
        Reads the sphinx runtime config, and returns a dictionary
        mapping from the extension config name to the config value
        """
        return {
            f"{ext_config.name}": getattr(config, ext_config.name)
            for ext_config in self.__extension_configs
        }
