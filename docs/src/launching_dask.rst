Launching a Dask Cluster
=========================

The pipeline can leverage a dask cluster with multiple nodes to distribute the spectral line imaging stage. In order to use that feature, you most likely will have to spin up said dask cluster before starting the pipeline.

Testing on a local machine
-----------------------------

You may want to do some development or testing work on your own desktop computer, in which case launching a dask cluster manually is the straightforward method.

In one terminal window, activate the same ``conda`` environment as the pipeline. Then launch:

.. code-block:: bash

    dask scheduler

This starts the dask scheduler on ``port 8786``. Now onto launching the workers. In another terminal window, activate the same python environment as the pipeline. Then launch:

.. code-block:: bash

    dask worker localhost:8786 --resources subprocess_slots=1 --nworkers <NUM_WORKERS>

You may adjust other parameters as desired, such as the number of threads per worker; by default, all the available threads are split evenly between workers.
