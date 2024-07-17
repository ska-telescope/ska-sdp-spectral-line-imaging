**********************
Pipeline usage
**********************

=====================
Running the pipeline
=====================

To run the pipeline you can use the following command,

.. code-block:: bash

    spectral_line_pipeline --input /path/to/zarr \
        --config /path/to/config-file \
        --stages [STAGES] \
        --dask-scheduler DASK-SCHEDULER

==============================================
Pipeline parameters / command line arguments
==============================================

**-input**

    This parameter takes path to the visibilities / processing set

**-config**

    This parameter takes custom YAML configuration file for the pipeline. While installing the pipeline, default configuration file is automatically created.

**-stages**

    This parameter takes a a list of stages (space seperated) that the user wants to run. This will override the stages defined in the config file.

**-dask-scheduler**

    This parameter provides the address of a dask scheduler that is connected to dask workers. Providing this parameter enables distributed processing of spectral line imaging pipeline, such that all the operations are run using multiple processes and/or nodes. The dask scheduler and workers should be up and running before the pipeline is started (see :doc:`launching_dask`). If this argument is not provided, the pipeline runs all the operations sequentially, without using Dask.
