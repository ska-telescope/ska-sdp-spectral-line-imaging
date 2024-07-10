**********************
Pipeline usage
**********************

=====================
Running the pipeline
=====================

To run the pipeline you can use the following command,

.. code-block:: bash

    spectral_line_pipeline --input /path/to/zarr \
        --stages [STAGES] \
        --config /path/to/config-file \
        --dask-scheduler DASK-SCHEDULER

==============================================
Pipeline parameters / command line arguments
==============================================

**-input**

    This parameter takes path to the visibilities/processing set/measurement set.

**-config**

    This parameter takes custom YAML configuration file for the pipeline. (While installing the pipeline, default configuration file is automatically created)

**-stages**

    This parameter takes the stages that the user wants to run specifically. (stages to be run can be set it in config YAML file, stages given here would be overriding the config file)

**-dask-scheduler**

    This parameter provides the address of a Dask Scheduler that is connected to several Dask Workers. Providing this parameter enables distribution for spectral line imaging. Its operations then run using multiple processes and/or nodes. The Dask Scheduler and Workers are required to be running before the pipeline is started. If this argument is not provided, the pipeline runs all processes sequentially, without using Dask.
