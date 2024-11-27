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
    docker pull artefact.skao.int/ska-sdp-spectral-line-imaging:0.6.2
    ```

    The entrypoint of above image is set to the executable `spectral-line-imaging-pipeline`.

1. Run image with volume mounts to enable read write to storage.

    ```bash
    docker run [-v local:container] <image-name> [run | install-config] ...
    ```

You can also spin up local containerized dask cluster using the docker image, and run pipeline within it. Please refer to ["Running on a Local Dask Cluster"](https://developer.skao.int/projects/ska-sdp-spectral-line-imaging/en/latest/local_dist_env.html) section of the documentation.

### Install the config

Install the default config YAML of the pipeline to a specific directory using the `install-config` subcommand.

```bash
spectral-line-imaging-pipeline install-config --config-install-path path/to/dir
```

Parameters of the default configuration can be overriden

```bash
spectral-line-imaging-pipeline install-config --config-install-path path/to/dir \
                    --set parameters.imaging.gridding_params.cell_size 0.2 \
                    --set parameters.predict_stage.cell_size 0.2 \
                    --set parameters.read_model.pols [XX,YY]
```

### Run the pipeline

Run the spectral line pipeline using the `run` subcommand.

To run on the accompanying [SKA MID](#ska-mid-data) simulated data, run:

```bash
spectral-line-imaging-pipeline run \
--input ska_low_simulated_data.ps \
--config ska_low_config.yml
```

For all the options, run `spectral-line-imaging-pipeline run --help`.

### Autocompletions for bash and zsh

#### bash

```bash
export YAML_PATH=/path/to/pipeline/default
source ./scripts/bash-completions.bash
```

#### zsh

```zsh
source ./scripts/bash-completions.bash
bashcompinit
```

## Some pre-requisites of the pipeline

### SKA MID Data

The data `sim_mid_contsubbed_xrad31_freq1.ps`, the config file `spectral_line_imaging_pipeline.yml` and a psf image `ska_mid_gen_psf.fits` are present in SKA's google cloud bucket.
Please contact members from team DHRUVA to know how to access it.

### Running pipeline on custom MSv2 dataset

In order to run the pipeline on a custom MSv2 dataset, you have to convert it to a xradio processing set.

1. Follow the instructions on [xradio github](https://github.com/casangi/xradio/tree/main?tab=readme-ov-file#installing) to install xradio package.

1. Have a look at the [example jupyter notebooks](https://github.com/casangi/xradio/blob/main/demo/demo.ipynb) present in the xradio github to know how to convert the data from MSv2 to a xradio processing set.

### Regarding the model visibilities and model images

<!-- The relative hyperlink to this section is present in docstring and configuration of "read_model" stage -->

If your MSv2 data already contains `MODEL_DATA` column , you don’t need to run the **read_model** and **predict_stage** stages, which can be turned off using the config file. Continuum subtraction stage will operate on `VISIBILITY` and existing `VISIBILITY_MODEL` variables.

If `MODEL_DATA` column is not present, you can predict the model visibilities by passing model FITS images to the pipeline via **read_model** stage.
The **predict_stage** will generate `VISIBILITY_MODEL` data by running predict operation on the model image data.

About the model FITS images:

1. If FITS image is spectral cube, it must have same frequency coordinates (the reference frequency, frequency delta and number of channels) as the measurement set.

1. Currently **read_model** does not support reading data of multiple polarizations from a single FITS file. So for each polarization value in the processing set, there has to be a seperate FITS image.
