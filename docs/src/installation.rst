************
Installation
************

========================
Pipeline Setup (non-dev)
========================

Currently, only way to install the pipeline is using poetry. First, clone the repo using this command.

.. code-block:: bash

    git clone https://gitlab.com/ska-telescope/sdp/ska-sdp-spectral-line-imaging.git

Install the framework
----------------------

`poetry` is used for dependency management and installation.

.. code-block:: bash

    poetry install --only main


Install spectral line pipeline
-------------------------------

This command installs spectral line pipeline and generates default config YAML file.

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

Run ``spectral_line_imaging_pipeline --help`` for more information.

=========
Dev Setup
=========

Cloning the repository
----------------------

Please make sure to clone the submodules also using

.. code-block:: bash

    git clone --recurse-submodules https://gitlab.com/ska-telescope/sdp/ska-sdp-spectral-line-imaging.git

Also make sure to update submodules at every pull.

**Updating submodule post clone**

.. code-block:: bash

    git submodule update --init


Setting up conda and installing pipeline
----------------------------------------

It is recommended to use a virtual environment for the developer setup. This document explains the developer setup using ``conda``.

The ``conda`` based approach, sets up an environment with packages defined in ``environment.yaml``. This makes sure that the python and poetry versions used with this repo does not conflict with any previous poetry installed on your machine.

You can run the scripts present in ``/scripts`` directory to create a conda environment and install the pipeline with its dependencies.

.. code-block:: bash

    ./scripts/recreate-conda-env.sh

    conda activate spec_line

    scripts/install-pipeline.sh


Git hooks
-----------

To enable ``git-hooks`` for the current repository please link the ``.githooks`` folder to the ``core.hooksPath`` variable of the ``git`` configuration

.. code-block:: bash

    git config --local core.hooksPath .githooks/

The pre-commit hook is defined for the main branch and is present in the ``.githooks`` folder.
The current pre-commit hook runs the following

#. ``pylint`` set to fail on warnings.
#. Tests on ``src`` folder, with coverage not to fall below 80%.
#. Build documentation
#. Help prepare commit message as per agreed format

GPG signing the commits
--------------------------

Enable GPG signing for commits by setting the ``commit.gpgsign`` config variable to ``true``

.. code-block:: bash

    git config commit.gpgsign true

To use the ssh-key to sign the commits set ``gpg.format`` to ssh, and update ``user.signingkey`` to the path of the ssh public key 

.. code-block:: bash

    git config gpg.format ssh

    git config user.signingkey ~/.ssh/id_rsa.pub
