#!/usr/bin/env bash

# install-pipeline.sh

# Installs the pipeline using poetry from the conda environment

set -e

# Extract the environment name from environment.yml
ENV_NAME=$(grep -m 1 '^name:' environment.yml | awk '{print $2}')

# checking if poetry from environment is being used
if [[ $(which poetry) != *"envs/${ENV_NAME}/bin/poetry" ]]; then 
    echo "Incorrect poetry executable. The poetry from $ENV_NAME environment is not in PATH. Check if $ENV_NAME environment is activated"
else
    # Run poetry lock
    echo "Updating poetry lock..."
    poetry lock --no-update

    # Run poetry install
    echo "Running poetry install..."
    poetry install --all-extras
fi