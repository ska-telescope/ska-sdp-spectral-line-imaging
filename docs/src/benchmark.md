# Benchmarking Pipeline

We have integrated various ways into this pipeline which can be used to benchmark it.

## Using dask performance report

You can generate detailed reports of dask's run using [dask diagnostics](https://docs.dask.org/en/stable/diagnostics-distributed.html#diagnostics-distributed).

When running the pipeline in distributed mode, you can pass `--with-report` option which store the dask report in `dask_report.html` file inside the specified output directory.

```bash
spectral-line-imaging-pipeline run --input input.ps --config config.yml --output-path output/benchmark --dask-scheduler localhost:8786 --with-report
```

**Note** that pipeline must be run with a dask scheduler for reports to be generated.

Also, since dask performance report captures detailed metrics per task (so per process), this might affect the computation times.

## Using Dool on local machine

We have integrated [`dool`](https://github.com/scottchiefbaker/dool/tree/master) into `piper benchmark` command, which can be used to capture performance metrics (CPU, Memory, IO).

Run `piper benchmark --help` to see all the options available for the commmand.

For example, use following command to run pipeline and capture metrics:

```bash
piper benchmark --setup --command "spectral-line-imaging-pipeline run --input input.ps --config config.yml" --output-path output/benchmark
```

This will run the spectral line imaging pipeline as usual, and also store the captured metrics in a CSV formatted file in `output/benchmark` directory.
These values can then be used for plotting graphs (see "Dool Visualizer" in [System Resource Tracing](https://confluence.skatelescope.org/display/SE/System%27s+resource+tracing+with+dool) confluence page).

## Using dool on distributed setup

In a distributed setup, we need to make sure that we start `dool` seperately on each of the worker machines. The `piper benchmark` command can be used for this purpose.

We have an example [docker-compose-benchmark.yml](https://gitlab.com/ska-telescope/sdp/science-pipeline-workflows/ska-sdp-spectral-line-imaging/-/blob/main/examples/docker-compose-benchmark.yml) which can run pipeline in distributed manner (in seperate docker containers), and will also capture metrics for each worker, storing them in seperate CSV files in `DATA/benchmark` directory.
The command to run can be something like this:

```bash
MOUNT_FLAGS=:z # only for SELinux Users
IMAGE=artefact.skao.int/ska-sdp-spectral-line-imaging:0.6.1 \
DATA=/path/to/local/dir \
REPLICAS=4 \
INPUT=processing_set.ps \
CONFIG=config.yml \
OUTPUT_DIR=output \
docker compose -f docker-compose-benchmark.yml up --abort-on-container-exit
```
