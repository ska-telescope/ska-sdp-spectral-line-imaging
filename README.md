# SKA SDP Spectral Line Imaging Pipeline

## Description

A spectral line imaging pipeline developed by Team DHRUVA for SKAO.

The repository is [hosted on gitlab](https://gitlab.com/ska-telescope/sdp/science-pipeline-workflows/ska-sdp-spectral-line-imaging).
The documentation is available [at this page](https://developer.skao.int/projects/ska-sdp-spectral-line-imaging/en/latest/).

This package contains 2 sub-packages:

1. The **spectral line imaging pipeline**

1. The **piper** pipeline framework

If you wish to contribute to this repository, please refer [Developer Guide](./DEVELOPMENT.md)

## Getting Started

### Installation with pip

The latest release is available in SKA's pip reposoitory. You can install this package using following command:

```bash
pip install --extra-index-url https://artefact.skao.int/repository/pypi-internal/simple ska-sdp-spectral-line-imaging
```

Once installed, the spectral line imaging pipeline is available as a python package, and as `spectral-line-imaging-pipeline` cli command.

Run `spectral-line-imaging-pipeline --help` to get help on different subcommands.

> Above command also installs **piper** framework and the `piper` cli command.
> Refer to the [documentation](https://developer.skao.int/projects/ska-sdp-spectral-line-imaging/en/latest/piper.html) to understand how to it.

### Containerized usage

The pipeline can also be deployed inside a oci container.

1. Run following command to pull the oci image.

    ```bash
    docker pull harbor.skao.int/production/ska-sdp-spectral-line-imaging:0.1.0
    ```

    The entrypoint of above image is set to the executable `spectral-line-imaging-pipeline`.

1. Run image with volume mounts to enable read write to storage.

    ```bash
    docker run [-v local:container] <image-name> [run | install-config] ...
    ```

### Install the config and run the pipeline

Install the default config YAML of the pipeline to a specific directory using the `install-config` subcommand.

```bash
spectral-line-imaging-pipeline install-config --config-install-path path/to/dir
```

Run the spectral line pipeline using the `run` subcommand.

```bash
spectral-line-imaging-pipeline run --input /path/to/processing_set \
    --config /path/to/config \
    --output /path/to/output \
    --dask-scheduler /url/of/the/dask/scheduler \
    --stages STAGES
```
