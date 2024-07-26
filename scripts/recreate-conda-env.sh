#!/usr/bin/env bash

# recreate-conda-env.sh

# (Re)installs the conda environment as defined in environment.yml

set -e

# Extract the environment name from environment.yml
ENV_NAME=$(grep -m 1 '^name:' environment.yml | awk '{print $2}')

# Check if the environment exists and remove it if it does
if conda info --envs | grep -q "$ENV_NAME"; then
    echo "Removing existing $ENV_NAME environment..."
    conda env remove -n "$ENV_NAME"
fi

# Create a new environment from environment.yml
echo "Creating new $ENV_NAME environment...";
conda env create -f environment.yml

echo -e \
"\nThe $ENV_NAME environment successfully created.\n\n\
Next steps:\n\
\t1.Activate the environment 'conda activate $ENV_NAME'\n\
\t2.Install the pipeline by running './scripts/install-pipeline.sh'\n"

