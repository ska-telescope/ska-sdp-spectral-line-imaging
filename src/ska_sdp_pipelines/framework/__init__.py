r"""Pipeline Framework

A complete installable data analysis pipeline can be defined with the help of
the `ska-sdp-pipeline` module.

A pipeline consisting of the stages, and their default configurations can be
defined by decorating a standard python function using
:py:class:`ConfigurableStage`,
and creating a pipeline object specifying the order of the stage executions.

The :py:class`ConfigurableStage` serves two purposes

1. Defining a stage with default configurations
2. Extracting the default configuration as a YAML file when needed.

The framework by default reads an xradio processing set as the first step
and makes it available to all the stages defined in the pipeline during
execution. The user can override the data read function, to support custom
data formats.

The scheduling and execution is managed by the framework. All the stages are
scheduled using `dask.delayed`, and hence the pipeline defininition should
not include a call to `dask.compute()` as it will execute the scheduled
dask graph till that point, and can lead to unforseen errors. The framework
based approach removes the need to manage dask all together from the pipeline
definition.

A custom defined pipeline can be installed as an executable using the
`sdp-pipeline install` command. The installation process creates an executable
data processing pipeline using the pipeline name, and also extracts the
default configuration to a path provided by the user, or in the same
directory as the pipeline script if the path is not provided.

Post installation the pipeline can be executed via CLI to perform data
processing and data analysis.

The four classes required to define a complete pipeline are

- :py:class:`ConfigurableStage`
- :py:class:`Configuration`
- :py:class:`ConfigParam`
- :py:class:`Pipeline`


Examples
--------
Continuum subtraction Pipeline
------------------------------

This is an example pipeline to demonstarate how to define a continuum
subtraction pipeline with some default configuration. The pipeline consists of
three stages

- `select_field`: Select the appropriate field from the processing set
- `read_model`: Reads the model data
- `cont_sub`: Subtracts the model data from the visibility data

Defining configurable stage
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A configurable stage is defined by decorating a standard python function with
:py:class`ConfigurableStage`. The wrapper allows for the definition of a stage
having a name and the configurable parameters which are made available to the
function as function arguments. The first function argument is reserved for the
pipeline metadata, which is a dictionary consisting of the input processing set
(input_data), the output from the upstream stage (output) and the output path
(output_dir) for the pipeline which can be used for writing out data from
within the stages

`select_field` stage
  Configurable parameters
    * intent (str): default - "Intent1"
    * field_id (int): default - 0
    * ddi (int): default - 0

>>> @ConfigurableStage(
...     "select_vis",
...     configuration=Configuration(
...         intent=ConfigParam(str, "Intent1"),
...         field_id=ConfigParam(int, 0),
...         ddi=ConfigParam(int, 0),
...     ),
... )
... def select_field(pipeline_data, intent, field_id, ddi):
...     ps = pipeline_data["input_data"]
...     psname = list(ps.keys())[0].split(".ps")[0]
...
...     sel = f"{psname}.ps_ddi_{ddi}_intent_{intent}_field_id_{field_id}"
...     return {"ps": ps[sel].unify_chunks()}

`cont_sub` stage:
  Configurable parameters: None

>>> @ConfigurableStage("continuum_subtraction")
... def cont_sub(pipeline_data):
...     ps = pipeline_data["output"]["ps"]
...     model = pipeline_data["output"]["model_vis"]
...     return {"ps": ps.assign({"VISIBILITY": ps.VISIBILITY - model})}


Once the stages are defined, a pipeline object is defined, which takes a name,
which serves as the name of the pipeline, and eventually the executable
generated post installation of the pipeline. The order of execution of the
stages is provided to the pipeline throught the `stages` argument. The pipeline
executes the stages in the order as provided.

>>> Pipeline(
...     "continuum_subtraction_pipeline",
...     stages=[
...         select_field,
...         read_model,
...         cont_sub
...     ]
... )

Full Code
~~~~~~~~~

>>> import astropy.io.fits as fits
... import numpy as np
... import xarray as xr
...
... from ska_sdp_pipelines.framework.configurable_stage import (
...     ConfigurableStage
... )
... from ska_sdp_pipelines.framework.configuration import (
...     ConfigParam,
...     Configuration,
... )
... from ska_sdp_pipelines.framework.pipeline import Pipeline
...
... @ConfigurableStage(
...     "select_vis",
...     configuration=Configuration(
...         intent=ConfigParam(str, "Intent1"),
...         field_id=ConfigParam(int, 0),
...         ddi=ConfigParam(int, 0),
...     ),
... )
... def select_field(pipeline_data, intent, field_id, ddi):
...     ps = pipeline_data["input_data"]
...     psname = list(ps.keys())[0].split(".ps")[0]
...     sel = f"{psname}.ps_ddi_{ddi}_intent_{intent}_field_id_{field_id}"
...     return {"ps": ps[sel].unify_chunks()}
...
... @ConfigurableStage(
...     "read_model",
...     configuration=Configuration(
...         image_name=ConfigParam(str, "wsclean"),
...         pols=ConfigParam(list, ["I", "Q", "U", "V"]),
...     ),
... )
... def read_model(pipeline_data, image_name, pols):
...     ps = pipeline_data["output"]["ps"]
...     images = []
...     for pol in pols:
...         with fits.open(f"{image_name}-{pol}-image.fits") as f:
...             images.append(f[0].data.squeeze())
...     image_stack = xr.DataArray(
...         np.stack(images), dims=["polarization", "ra", "dec"]
...     )
...     return {"ps": ps, "model_image": image_stack}
...
... @ConfigurableStage("continuum_subtraction")
... def cont_sub(pipeline_data):
...     ps = pipeline_data["output"]["ps"]
...     model = pipeline_data["output"]["model_vis"]
...     return {"ps": ps.assign({"VISIBILITY": ps.VISIBILITY - model})}
...
... Pipeline(
...     "continuum_subtraction_pipeline",
...     stages=[
...         select_field,
...         read_model,
...         cont_sub
...     ]
... )


Installing the Pipeline
~~~~~~~~~~~~~~~~~~~~~~~

A python file containing the above definition of the pipeline can be installed
with the help of the `sdp-pipelines` command.::

  sdp-pipelines install \
    /path/to/continuum_subtraion_pipeline.py \
    --config-install-path=/path/to/save/default/config.yml


If the `--config-install` path is not provided, the default config will be
generated at the location of the pipeline definition file.

Pipeline Configuration File
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default configuration is saved as YAML file during the installation
process.::

    parameters:
      continuum_subtraction: {}
      read_model:
        image_name: wsclean
        pols:
        - I
        - Q
        - U
        - V
      select_vis:
        ddi: 0
        field_id: 0
        intent: null
    pipeline:
      continuum_subtraction: true
      read_model: true
      select_vis: true


The generated configuration consists of two sections

1. Pipeline Section
    This section indicates which all stages would be run during the pipeline
    execution, and contains the list stages along with a boolean value
    defaulted to `true`.
2. Parameters Section
    This section contains the list of stages and their corresponding
    configurable parameters defaulted to the values as defined in during the
    pipeline definition.

Executing the pipeline
~~~~~~~~~~~~~~~~~~~~~~

Once the pipeline is installed as a CLI, it can be executed using the following
command::

   continuum_subtraction_pipeline\
     --input /path/to/xradio/processing/set.ps\
     --output /path/to/store/outputs

`continuum_subtraction_pipeline \-\-help`::

    usage: continuum_subtraction_pipeline [-h] [--input INPUT] \
      [--config [CONFIG_PATH]] [--output [OUTPUT_PATH]] \
      [--stages [STAGES ...]] [--dask-scheduler DASK_SCHEDULER] [--verbose]

    options:
      -h, --help            show this help message and exit
      --input INPUT         Input visibility path
      --config [CONFIG_PATH]
                            Path to the pipeline configuration yaml file
      --output [OUTPUT_PATH]
                            Path to store pipeline outputs
      --stages [STAGES ...]
                            Pipleline stages to be executed
      --dask-scheduler DASK_SCHEDULER
                            Optional dask scheduler address to which to submit
                            jobs. If specified, any eligible pipeline step will
                            be distributed on the associated Dask cluster.
      --verbose, -v         Increase pipeline verbosity to debug level.


Toggeling pipeline stages
~~~~~~~~~~~~~~~~~~~~~~~~~

The stages defined above can be toggled off during the pipeline execution by
one two following approaches.

1. Using the `\-\-stages` option
    Pass only the stages which needs to be executed.
2. Using the pipeline section in config
    Toggle to false the stages which need not be run.

"""
