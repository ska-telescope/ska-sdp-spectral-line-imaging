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
    docker pull artefact.skao.int/ska-sdp-spectral-line-imaging:0.5.0
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

Parameters of the default configuration can be overriden

```bash
spectral-line-imaging-pipeline install-config --config-install-path path/to/dir \
                    --set parameters.imaging.gridding_params.cell_size 0.2 \
                    --set parameters.predict_stage.cell_size 0.2 \
                    --set parameters.read_model.pols [XX,YY]
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

## Some pre-requisites of the pipeline

### SKA LOW Data

The data `ska_low_simulated_data.ps` and the config file `ska_low_config.yml` are present in SKA's google cloud bucket.
Please contact members from team DHRUVA to know how to access it.

### Running pipeline on custom MSv2 dataset

In order to run the pipeline on a custom MSv2 dataset, you have to convert it to a xradio processing set.

1. Follow the instructions on [xradio github](https://github.com/casangi/xradio/tree/main?tab=readme-ov-file#installing) to install xradio package.

1. Have a look at the [example jupyter notebooks](https://github.com/casangi/xradio/blob/main/doc/meerkat_conversion.ipynb) present in the xradio github to know how to convert the data from MSv2 to a xradio processing set.

### Regarding the model visibilities

If your MSv2 data already contains `MODEL_DATA` column , you don’t need to run the **read_model** and **predict_stage** stages, which can be turned off using the config file. Continuum subtraction stage will operate on `VISIBILITY` and existing `VISIBILITY_MODEL` variables.

If `MODEL_DATA` column is not present, you can predict the model visibilities by inputting model FITS images to the pipeline.
The **predict_stage** will generate `VISIBILITY_MODEL` variable by running `ducc0.wgridder.dirty2ms` operation on the FITS images.
These FITS images are read as part of the **read_model** stage.

Some important points regarding the parameters of the **read_model** stage present in the YAML file:

1. The `pols` param in **read_model** stage must contain a list of all the polarizations for which an image is available.

1. For each polarization, a FITS image with name `<image_name>-<pol>-image.fits` should be present.

    1. The `<image_name>` is another parameter of the **read_model** stage, which is the prefix of your image. For example, if the image is present at path `/home/user/ska_low_256-XX-image.fits`, then parameter should be `image_name : /home/user/ska_low_256`.

    1. The `<pol>` should match with the polarization names present in the pols param. In above case, the parameter.
