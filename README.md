# SKA SDP Spectral Line Imaging Pipeline

A spectral line imaging pipeline developed by Team DHRUVA for SKAO.

The repository is [hosted on gitlab](https://gitlab.com/ska-telescope/sdp/science-pipeline-workflows/ska-sdp-spectral-line-imaging).
The documentation is available [at this page](https://developer.skao.int/projects/ska-sdp-spectral-line-imaging/en/latest/).

This package contains 2 sub-packages:

1. The **spectral line imaging pipeline**

1. The **piper** pipeline framework

If you wish to contribute to this repository, please refer [Developer Guide](./DEVELOPMENT.md)

## Using the pipeline

### Installation with pip

The latest release is available in SKA's pip reposoitory. You can install this package using following command:

```bash
pip install --extra-index-url https://artefact.skao.int/repository/pypi-internal/simple ska-sdp-spectral-line-imaging
```

> 📝 For xradio to work on macOS, it is required to pre-install `python-casacore` using `pip install python-casacore`.

Once installed, the spectral line imaging pipeline is available as a python package, and as `spectral-line-imaging-pipeline` cli command.

Run `spectral-line-imaging-pipeline --help` to get help on different subcommands.

> Above command also installs **piper** framework and the `piper` cli command.
> Refer to the [documentation](https://developer.skao.int/projects/ska-sdp-spectral-line-imaging/en/latest/piper.html) to understand how to it.

### Containerized usage

The pipeline can also be deployed inside a oci container.

1. Run following command to pull the oci image.

    ```bash
    docker pull harbor.skao.int/production/ska-sdp-spectral-line-imaging:0.3.0
    ```

    The entrypoint of above image is set to the executable `spectral-line-imaging-pipeline`.

1. Run image with volume mounts to enable read write to storage.

    ```bash
    docker run [-v local:container] <image-name> [run | install-config] ...
    ```

### Install the config

Install the default config YAML of the pipeline to a specific directory using the `install-config` subcommand.

```bash
spectral-line-imaging-pipeline install-config --config-install-path path/to/dir
```

### Run the pipeline

Run the spectral line pipeline using the `run` subcommand.

To run on the accompanying [SKA LOW](#ska-low-data) simulated data, run:

```bash
spectral-line-imaging-pipeline run \
--input ska_low_simulated_data.ps \
--config ska_low_config.yml
```

For all the options, run `spectral-line-imaging-pipeline run --help`.
