Launching a Dask Cluster
=========================

The pipeline can leverage a dask cluster with multiple workers to run operations in distributed manner. For this, you need an active dask cluster before starting the pipeline.

Local machine
-------------

You can run dask locally on your machine, in which case the dask scheduler will distribute the operations across multiple processes.

In one terminal window, activate the same ``conda`` environment as the pipeline. Then run:

.. code-block:: bash

    dask scheduler

This starts the dask scheduler on (default) ``port 8786``. 

In another terminal window, activate the same python environment as the pipeline. Then run:

.. code-block:: bash

    dask worker localhost:8786 --resources subprocess_slots=1 --nworkers <NUM_WORKERS>

You may adjust other parameters as desired, such as the number of threads per worker; by default, all the available threads are split evenly between workers.
