##################
Pipeline Framework
##################

A complete installable data analysis pipeline can be defined with the help of
the `ska-sdp-pipeline` module.

A pipeline consisting of the stages, and their default configurations can be
defined by decorating a standard python function using
:py:class:`ConfigurableStage`,
and creating a pipeline object specifying the order of the stage executions.

The :py:class:`ConfigurableStage` serves two purposes

1. Defining a stage with default configurations
2. Extracting the default configuration as a YAML file when needed.

The framework by default reads an xradio processing set as the first step
and makes it available to all the stages defined in the pipeline during
execution. The user can override the data read function, to support custom
data formats.

The scheduling and execution is managed by the framework. All the stages are
scheduled using :py:func:`dask.delayed`, and hence the pipeline defininition should
not include a call to :py:func:`dask.compute()` as it will execute the scheduled
dask graph till that point, and can lead to unforseen errors. The framework
based approach removes the need to manage dask all together from the pipeline
definition.

A custom defined pipeline can be installed as an executable using the
:command:`sdp-pipeline install` command. The installation process creates an executable
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

********
Examples
********

===========================
Process Visibility Pipeline
===========================

This is an example to demonstarate how to define a pipeline which performs some operations on visibility data present in a processing set. The pipeline consists of three stages

- :py:func:`select_field`: Select the appropriate field from the processing set
- :py:func:`process_vis`: Multiplies visibility with a multiplier
- :py:func:`export_vis`: Stores the processed visibilities into a zarr file

----------------------------
Defining configurable stages
----------------------------

A configurable stage is defined by decorating a standard python function with 
:py:class:`ConfigurableStage``. The wrapper allows for the definition of a stage
having a name and the configurable parameters which are made available to the
function as function arguments. 

The first function argument is reserved for the pipeline metadata, 
which is a dictionary consisting of following keys:

* :py:attr:`input_data` : the input processing set
* :py:attr:`output`: the output from the upstream stage 
* :py:attr:`output_dir`: the output path for the pipeline which can be used for writing out data from within the stages

:py:func:`select_field` stage
  Configurable parameters
    * intent (str): default - None
    * field_id (int): default - 0
    * ddi (int): default - 0

>>> @ConfigurableStage(
...     "select_field",
...     configuration=Configuration(
...         intent=ConfigParam(str, None),
...         field_id=ConfigParam(int, 0),
...         ddi=ConfigParam(int, 0),
...     ),
... )
... def select_field_from_ps(pipeline_data, intent, field_id, ddi):
...     ps = pipeline_data["input_data"]
...     psname = list(ps.keys())[0].split(".ps")[0]
...     sel = f"{psname}.ps_ddi_{ddi}_intent_{intent}_field_id_{field_id}"
...     return {"ps": ps[sel].unify_chunks()}


:py:func:`process_vis` stage
  Configurable parameters
    * multiplier (float): default - 1.0

>>> @ConfigurableStage(
...     "process_vis",
...     configuration=Configuration(
...         multiplier=ConfigParam(float, 1.0)
...     ),
... )
... def process_vis(pipeline_data, multiplier):
...     ps = pipeline_data["output"]["ps"]
...     processed_vis = multiplier * ps.VISIBILITY
...     return {"processed_vis": processed_vis}

:py:func:`export_vis` stage:

Note that we are using the `output_dir` key from pipeline metadata to store the 
output zarr file.

  Configurable parameters
    * output_vis_name (str): default - "output_vis.zarr"

>>> @ConfigurableStage(
...     "export_vis"
... )
... def export_processed_vis(pipeline_data):
...     vis = pipeline_data["output"]["processed_vis"]
...     output_path = os.path.join(pipeline_data["output_dir"], "output_vis.zarr")
...     vis.to_zarr(store=output_path)


Once the stages are defined, a pipeline object is defined, which takes a name,
which serves as the name of the pipeline, and eventually the executable
generated post installation of the pipeline. The order of execution of the
stages is provided to the pipeline throught the `stages` argument. The pipeline
executes the stages in the order as provided.


>>> Pipeline(
...     "process_vis_pipeline",
...     stages=[
...         select_field_from_ps,
...         process_vis,
...         export_processed_vis
...     ]
... )

--------------------------
Entire Pipeline Definition 
--------------------------

>>> # process_vis_pipeline.py
... 
... import astropy.io.fits as fits
... import numpy as np
... import xarray as xr
... import os
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
...     "select_field",
...     configuration=Configuration(
...         intent=ConfigParam(str, None),
...         field_id=ConfigParam(int, 0),
...         ddi=ConfigParam(int, 0),
...     ),
... )
... def select_field_from_ps(pipeline_data, intent, field_id, ddi):
...     ps = pipeline_data["input_data"]
...     psname = list(ps.keys())[0].split(".ps")[0]
...     sel = f"{psname}.ps_ddi_{ddi}_intent_{intent}_field_id_{field_id}"
...     return {"ps": ps[sel].unify_chunks()}
... 
... @ConfigurableStage(
...     "process_vis",
...     configuration=Configuration(
...         multiplier=ConfigParam(float, 1.0)
...     ),
... )
... def process_vis(pipeline_data, multiplier):
...     ps = pipeline_data["output"]["ps"]
...     processed_vis = multiplier * ps.VISIBILITY
...     return {"processed_vis": processed_vis}
... 
... @ConfigurableStage(
...     "export_vis"
... )
... def export_processed_vis(pipeline_data):
...     vis = pipeline_data["output"]["processed_vis"]
...     output_path = os.path.join(pipeline_data["output_dir"], "output_vis.zarr")
... 
...     vis.to_zarr(store=output_path)
... 
... Pipeline(
...     "process_vis_pipeline",
...     stages=[
...         select_field_from_ps,
...         process_vis,
...         export_processed_vis
...     ]
... )

-----------------------
Installing the Pipeline
-----------------------

A python file containing the above definition of the pipeline can be installed
with the help of the :command:`sdp-pipelines` command.

.. code-block:: bash

  sdp-pipelines install \
  /path/to/process_vis_pipeline.py \
  --config-install-path=/path/to/save/default/config

If the ``--config-install-path`` is not provided, the default config will be
generated at the location of the pipeline definition file.

---------------------------
Pipeline Configuration File
---------------------------

The default configuration is saved as YAML file during the installation
process.

.. code-block:: yaml

  parameters:
    export_vis: {}
    process_vis:
        multiplier: 1.0
    select_field:
        ddi: 0
        field_id: 0
        intent: null
  pipeline:
    export_vis: true
    process_vis: true
    select_field: true

The generated configuration consists of two sections

1. Pipeline Section
    This section indicates which all stages would be run during the pipeline
    execution, and contains the list stages along with a boolean value 
    defaulted to `true`.
2. Parameters Section
    This section contains the list of stages and their corresponding
    configurable parameters defaulted to the values as defined in during the
    pipeline definition.

----------------------
Executing the pipeline
----------------------

Once the pipeline is installed as a CLI, it can be executed using the following
command::

   process_vis_pipeline \
     --input /path/to/processing_set.ps \
     --output /path/to/store/output

You can run :command:`process_vis_pipeline --help`, which will show 
the following help message::

    usage: process_vis_pipeline [-h] [--input INPUT] \
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

-------------------------
Toggeling pipeline stages
-------------------------

The stages defined above can be toggled off during the pipeline execution by
one two following approaches.

1. Using the ``--stages`` option
    Pass only the names of the stages (space seperated) which need to be executed.
2. Using the pipeline section in config
    Toggle the stages which need not be run to false.
