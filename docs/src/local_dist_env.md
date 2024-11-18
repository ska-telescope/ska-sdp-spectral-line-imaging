# Running on a Local Dask Cluster

The pipeline can leverage a dask cluster with multiple workers to run operations in distributed manner.

There are 2 ways in which you can setup and run the pipeline

1. Docker containerized way (recommended)
1. Using dask cli

## Containerized Local Cluster

Contianerized run makes sure that all the dask cluster components (scheduler, workers and client) run in exactly same environment, completely isolated from each other.

The repository contains a [docker-compose.yml](https://gitlab.com/ska-telescope/sdp/science-pipeline-workflows/ska-sdp-spectral-line-imaging/-/blob/main/docker-compose.yml) which can run the pipeline inside a containerized environment.
An example script for docker compose is:

```bash
MOUNT_FLAGS=:z # only for SELinux Users
IMAGE=artefact.skao.int/ska-sdp-spectral-line-imaging:0.6.1 \
DATA=/path/to/local/dir \
REPLICAS=4 \
INPUT=processing_set.ps \
CONFIG=config.yml \
OUTPUT_DIR=output \
docker compose -f docker-compose.yml up --abort-on-container-exit
```

Above command will spin up scheduler, 4 workers, and a client as docker containers, run the pipeline, and store output of the pipeline in `OUTPUT_DIR` inside `DATA` directory in your host file system. Above command expects that all the necessary data required (processing set, config, model images etc.) are present in the `/data` inside each container, which is mounted to `DATA`.

> If pipeline is unable to read model images, then inside the config, you may need to prefix the model path parameter `image` with `/data`.

The scheduler also starts a dask dashboard at `localhost:8787`, which is exposed to the host network.

You can modify the `entrypoint` in the `client` service inside docker compose as per you requirement, for example if you wish to add more options while running the pipeline.

## Using Dask CLI

Once you [install the pipeline](./README.md#installation-with-pip) and its dependencies in your local python environment, you can use dask CLI to run a local cluster.

> Make sure that you run scheduler and workers using the same python environment where the pipeline is installed.

Start a dask scheduler as:

```bash
dask scheduler
```

This starts the dask scheduler on (default) `port 8786`.

In another terminal window, start workers as:

```bash
dask worker localhost:8786 --resources subprocess_slots=1 --nworkers <NUM_WORKERS>
```

> If you want fine control over workers, you can start seperate workers in seperate terminal windows.

Then run the pipeline whlie specifying the scheduler IP as:

```bash
spectral-line-imaging-pipeline run \
--input /path/to/processing_set.ps \
--config config.yml
--dask-scheduler localhost:8786
```
