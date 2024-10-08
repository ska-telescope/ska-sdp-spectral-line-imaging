#####
Piper
#####

A complete installable data analysis pipeline can be defined with the help of
the `ska-sdp-piper` module.

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
:command:`piper install` command. The installation process creates an executable
data processing pipeline using the pipeline name, and also extracts the
default configuration to a path provided by the user, or in the same
directory as the pipeline script if the path is not provided. Additionaly a package manager like `poetry` can be used install a production
ready pipeline using the framework.

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

The stage has the function signature :py:attr:`stage_name(upstream_output, confugiration_parameters..., pipeline_metadata...)`

The first argument is reserved for the upstream output, followed by the configurations, and the optional pipeline metadata.
The upstream output is set to the output of the previous stages in the order of execution. It is set to `None` for the first stage.
The arguments for the configuration parameters are mandatory. The pipeline metadata arguments can be used on need basis.

The framework provides the following metadata arguments

* :py:attr:`_cli_args_`: a dictionary containing the cli arguments used for running the pipeline
* :py:attr:`_global_parameters_`: the pipeline level global configurations.
* :py:attr:`_input_data_` : the input processing set
* :py:attr:`_output_dir_`: the output path for the pipeline which can be used for writing out data from within the stages
  
:py:func:`select_field` stage
  Configurable parameters
    * field_id (int): default - 0
    * ddi (int): default - 0

>>> @ConfigurableStage(
...     "select_field",
...     configuration=Configuration(
...         field_id=ConfigParam(int, 0),
...         ddi=ConfigParam(int, 0),
...     ),
... )
... def select_field_from_ps(output, field_id, ddi, _input_data_):
...     ps = _input_data_
...     psname = list(ps.keys())[0].split(".ps")[0]
...     sel = f"{psname}.ps_ddi_{ddi}_intent_None_field_id_{field_id}"
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
... def process_vis(output, multiplier):
...     ps = output["ps"]
...     processed_vis = multiplier * ps.VISIBILITY
...     return {"processed_vis": processed_vis}

:py:func:`export_vis` stage:

Note that we are using the :py:attr:`_output_dir_`  to store the 
output zarr file.

  Configurable parameters
    * N/A

>>> @ConfigurableStage(
...     "export_vis"
... )
... def export_processed_vis(upstream_output, _output_dir_):
...     vis = upstream_output["processed_vis"]
...     output_path = os.path.join(_output_dir_, "output_vis.zarr")
...     vis.to_zarr(store=output_path)


Once the stages are defined, a pipeline object is defined, which takes a name,
which serves as the name of the pipeline, and eventually the executable
generated post installation of the pipeline. The order of execution of the
stages is provided to the pipeline throught the `stages` argument. The pipeline
executes the stages in the order as provided.


>>> Pipeline(
...     "process-vis-pipeline",
...     stages=[
...         select_field_from_ps,
...         process_vis,
...         export_processed_vis
...     ]
... )

---------------------------------------------------
Additional Runtime parameters and global parameters
---------------------------------------------------

Additional CLI arguments and global configurations for the pipeline can be provide during the pipeline definition, which are accessible
through the :py:attr:`_cli_args_` and :py:attr:`_global_parameters_` metadata argument.

:py:func:`select_field` stage
  Configurable parameters
    * field_id (int): default - 0
    * ddi (int): default - 0
  CLI argument
    * intent (str): default - None

>>> @ConfigurableStage(
...     "select_field",
...     configuration=Configuration(
...         field_id=ConfigParam(int, 0),
...         ddi=ConfigParam(int, 0),
...     ),
... )
... def select_field_from_ps(output, field_id, ddi, _input_data_, _cli_args_):
...     ps = _input_data_
...     intent = _cli_args_["intent"]
...     psname = list(ps.keys())[0].split(".ps")[0]
...     sel = f"{psname}.ps_ddi_{ddi}_intent_{intent}_field_id_{field_id}"
...     return {"ps": ps[sel].unify_chunks()}
... 
... pipeline = Pipeline(
...     "process-vis-pipeline",
...     stages=[
...         select_field_from_ps,
...         process_vis,
...         export_processed_vis
...     ],
...     cli_args=[
...        CLIArgument(
...            "--intent",
...            type=str,
...            dest="intent",
...            default=None,
...            help="XRADIO intent variable"
...        )
...     ],
...     global_config=Configuration(
...        processed_vis=ConfigParam(str, "processed_vis")
...     )
... )


------------------
Custom subcommands
------------------

Piper exposes additional APIs to add sub parsers and bind them to functions during
the pipeline definition. Sub commands can be added using the :py:func:`Pipeline.sub_command` decorator which takes the name of the subparser,
along with the callback function and a list of CLI arguments

>>> @pipeline.sub_command(
...     "clean", [CLIArgument(
...         "--output-path",
...         type=str,
...         dest="output_path",
...         required=True,
...         help="Path to cleanup"
...     )],
...     help="Clean output artefacts"
... )
... def cleanup(args):
...     output_path = args.output_path
...     folder_contents = os.listdir(output_path)
...    
...     for content in folder_contents:
...         timestamped_path = f"{output_path}/{content}"
...         if (
...               pipeline.name in content
...               and os.path.isdir(timestamped_path)
...         ):
...             shutil.rmtree(timestamped_path)


--------------------------
Entire Pipeline Definition 
--------------------------

>>> # process_vis_pipeline.py
... 
... import astropy.io.fits as fits
... import numpy as np
... import os
... import shutil
... import xarray as xr
... 
... from ska_sdp_piper.piper.command import CLIArgument
... from ska_sdp_piper.piper.configurations import (
...     ConfigParam,
...     Configuration,
... )
... from ska_sdp_piper.stage import ConfigurableStage
... from ska_sdp_piper.piper.pipeline import Pipeline
... 
... @ConfigurableStage(
...     "select_field",
...     configuration=Configuration(
...         field_id=ConfigParam(int, 0),
...         ddi=ConfigParam(int, 0),
...     ),
... )
... def select_field_from_ps(output, field_id, ddi, _input_data_, _cli_args_):
...     ps = _input_data_
...     intent = _cli_args_["intent"]
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
... def process_vis(output, multiplier, _global_parameters_):
...     ps = output["ps"]
...     p_vis_key = _global_parameters_["processed_vis"]
...     processed_vis = multiplier * ps.VISIBILITY
...     return {p_vis_key: processed_vis}
... 
... @ConfigurableStage(
...     "export_vis"
... )
... def export_processed_vis(upstream_output, _output_dir_, _global_parameters_):
...     processed_vis = _global_parameters_["processed_vis"]
...     vis = upstream_output[processed_vis]
...     output_path = os.path.join(_output_dir_, "output_vis.zarr")
...     vis.to_zarr(store=output_path)
... 
... pipeline = Pipeline(
...     "process-vis-pipeline",
...     stages=[
...         select_field_from_ps,
...         process_vis,
...         export_processed_vis
...     ],
...     cli_args=[
...        CLIArgument(
...            "--intent",
...            type=str,
...            dest="intent",
...            default=None,
...            help="XRADIO intent variable"
...        )
...     ],
...     global_config=Configuration(
...        processed_vis=ConfigParam(str, "processed_visibility")
...     )
... )
... 
... @pipeline.sub_command(
...     "clean", [CLIArgument(
...         "--output-path",
...         type=str,
...         dest="output_path",
...         required=True,
...         help="Path to cleanup"
...     )],
...     help="Clean up output artefacts"
... )
... def cleanup(args):
...     output_path = args.output_path
...     folder_contents = os.listdir(output_path)
...    
...     for content in folder_contents:
...         timestamped_path = f"{output_path}/{content}"
...         if (
...               pipeline.name in content
...               and os.path.isdir(timestamped_path)
...         ):
...             shutil.rmtree(timestamped_path)


--------------------------------------
Installing the Pipeline through poetry
--------------------------------------

If the pipeline definition is part of a bigger python module, poetry can be used to manage
the dependency and generate and install the executable pipeline. 

Add the following section in the :file:`pyproject.toml` file.

.. code-block:: toml

    [tool.poetry.scripts]
    process-vis-pipeline = "complete.import.path.to.process_vis_pipeline:pipeline"



--------------------------------
Installing a standalone Pipeline
--------------------------------

A python file containing the above definition of the pipeline can be installed
with the help of the :command:`piper` command.

.. code-block:: bash

  piper install process-vis-pipeline \
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

  global_parameters:
    processed_vis: processed_visibility
  parameters:
    export_vis: {}
    process_vis:
      multiplier: 1.0
    select_field:
      ddi: 0
      field_id: 0
  pipeline:
    export_vis: true
    process_vis: true
    select_field: true

The generated configuration consists of three sections

1. Pipeline Section
    This section indicates which all stages would be run during the pipeline
    execution, and contains the list stages along with a boolean value 
    defaulted to `true`.
2. Parameters Section
    This section contains the list of stages and their corresponding
    configurable parameters defaulted to the values as defined in during the
    pipeline definition.
3. Global Parameters Section
    This section contains the list of global configurable parameters which are
    available to all the stages through the metadata argument :py:attr:`_global_parameters_`

----------------------
Executing the pipeline
----------------------

The installed CLI application provides three sub-commands

1. :command:`run` (default provided with along with the framework)
2. :command:`install-config` (default provided with along with the framework)
3. :command:`clean` 


You can run :command:`process-vis-pipeline --help`, which will show 
the following help message

.. code-block:: bash

    usage: process-vis-pipeline [-h] {run,install-config,clean} ...
    
    positional arguments:
      {run,install-config,clean}
        run                 Run the pipeline
        install-config      Installs the default config at --config-install-path
        clean               Clean up output artefacts
    
    options:
      -h, --help            show this help message and exit

The pipeline can be executed using the following command

.. code-block:: bash

   process_vis_pipeline run\
     --input /path/to/processing_set.ps \
     --output /path/to/store/output

Default sub-command :command:`process-vis-pipeline run --help`

.. code-block:: bash

    usage: process-vis-pipeline run [-h]\
      --input INPUT [--config [CONFIG_PATH]]\
      [--output [OUTPUT_PATH]] [--stages [STAGES ...]]\
      [--dask-scheduler DASK_SCHEDULER] [--verbose]\
      [--intent INTENT]

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
                            Optional dask scheduler address to which to submit jobs.
                            If specified, any eligible pipeline step will be distributed
                            on the associated Dask cluster.
      --verbose, -v         Increase pipeline verbosity to debug level.
      --intent INTENT       XRADIO intent variable

Default  sub-command :command:`process-vis-pipeline install-config --help`

.. code-block:: bash

    usage: process-vis-pipeline install-config [-h] --config-install-path CONFIG_INSTALL_PATH

    options:
      -h, --help            show this help message and exit
      --config-install-path CONFIG_INSTALL_PATH
                            Path to place the default config.
      --set path value      Overrides for default config

Custom  sub-command :command:`process-vis-pipeline clean --help`

.. code-block:: bash

    usage: process-vis-pipeline clean [-h] --output-path OUTPUT_PATH

    options:
      -h, --help            show this help message and exit
      --output-path OUTPUT_PATH
                            Path to cleanup
          

-------------------------
Toggeling pipeline stages
-------------------------

The stages defined above can be toggled off during the pipeline execution by
one two following approaches.

1. Using the ``--stages`` option
    Pass only the names of the stages (space seperated) which need to be executed.
2. Using the pipeline section in config
    Toggle the stages which need not be run to false.
