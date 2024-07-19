***************
Installation
***************

===============
Pipeline Setup
===============
It is recommended to use a virtual environment for the developer setup. This document explains the developer setup using ``conda``.

Please make sure to clone the submodules also using

.. code-block:: bash

    git clone --recurse-submodules url://git-repository

Also make sure to update submodules at every pull.

Setup and activate environment
-----------------------------------

The `conda` based approach, sets up an environment with ``python 3.10``, ``pip 24.0`` and ``poetry 1.8``

.. code-block:: bash

    conda env create -f environment.yml

    conda activate spec_line

Install dependencies
----------------------

`poetry` is used for dependency management.

.. code-block:: bash

    poetry install


Install spectral line pipeline
-------------------------------

This command install spectral line pipeline and generates default config YAML file.

.. code-block:: bash

    sdp-pipeline install src/ska_sdp_spectral_line_imaging/pipeline.py
        [--config-install-path]=/path/to/dir


Run the pipeline
----------------

.. code-block:: bash

    spectral_line_imaging_pipeline --input /path/to/processing_set \
        --config /path/to/config \
        --output /path/to/output 
        --dask-scheduler /url/of/the/dask/scheduler

Run `spectral_line_imaging_pipeline --help` for more information.

==============
Dev Setup
==============

Git hooks
-----------

The pre-commit hook is defined for the main branch and is present in the ``.githooks`` folder. To enable ``git-hooks`` for the current repository please link the ``.githooks`` folder to the ``core.hooksPath`` variable of the ``git`` configuration

.. code-block:: bash

    git config --local core.hooksPath .githooks/

GPG signing the commits
--------------------------

Enable GPG signing for commits by setting the ``commit.gpgsign`` config variable to ``true``

.. code-block:: bash

    git config commit.gpgsign true

To use the ssh-key to sign the commits set ``gpg.format`` to ssh, and update ``user.signingkey`` to the path of the ssh public key 

.. code-block:: bash

    git config gpg.format ssh

    git config user.signingkey ~/.ssh/id_rsa.pub

The current pre-commit hook runs the following 

1. Tests on ``src`` folder
2. ``pylint`` set to fail on warnings. **[To be enabled once code is added]**
3. Coverage test to not fall below 80%  **[To be enabled once code is added]**
