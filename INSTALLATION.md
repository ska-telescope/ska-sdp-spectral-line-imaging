# Installation

## Pipeline Setup (non-dev)

Currently, only way to install the pipeline is using [poetry](https://python-poetry.org/). First, clone the repo with:

```bash
git clone https://gitlab.com/ska-telescope/sdp/ska-sdp-spectral-line-imaging.git
```

### Install the framework

The poetry is used for dependency management and installation. Following command will install the pipeline framework as a command line executable.

```bash
poetry install --only main
```

### Install spectral line pipeline

Following command installs spectral line pipeline and generates default
config YAML file.

```bash
sdp-pipeline install src/ska_sdp_spectral_line_imaging/pipeline.py
    [--config-install-path]=/path/to/dir
```

### Run the pipeline

```bash
spectral_line_imaging_pipeline --input /path/to/processing_set \
    --config /path/to/config \
    --output /path/to/output 
    --dask-scheduler /url/of/the/dask/scheduler
```

Run `spectral_line_imaging_pipeline --help` for more information.

## Developer Setup

### Cloning the repository

Please make sure to clone the submodules with:

```bash
git clone --recurse-submodules git@gitlab.com:ska-telescope/sdp/ska-sdp-spectral-line-imaging.git
```

Also make sure to update submodules after every pull with:

```bash
git submodule update --init
```

### Setting up conda and installing pipeline

It is recommended to use a virtual environment for the developer setup. The `conda` based approach, sets up an environment with packages defined in `environment.yaml`. This makes sure that the python and poetry
versions used with this repo does not conflict with any previous poetry installed on your machine.

You can run the scripts present in `/scripts` directory to create a conda environment and install the pipeline with its dependencies.

```bash
./scripts/recreate-conda-env.sh

conda activate spec_line

scripts/install-pipeline.sh
```

### Git hooks

To enable `git-hooks` for the current repository please link the `.githooks` folder to the `core.hooksPath` variable of the `git` configuration.

```bash
git config --local core.hooksPath .githooks/
```

The `pre-commit` hook is defined for the main branch and is present in the `.githooks` folder. The current `pre-commit` hook runs the following

1. If there is a change in either `environment.yml` or `pyproject.toml`, prompt the user to ask whether the local environment has been updated.
1. Run `pylint`, which is set to fail on warnings.
1. Run `pytest`, with coverage not to fall below 80%.
1. Build documentation

A `prepare-commit-msg` hook runs after `pre-commit` hook, which helps to prepare a commit message as per agreed format.

> Note: Due to interactiveness of these githooks, it is recommended to run all git commands using a terminal. GUI based git applications might throw errors.

### GPG signing the commits

First, set the git username and email for the your local repository.

```bash
git config user.name "username"
git config user.email "email address"
```

> The git `user.email` must match your gitlab account's email address.

Now, enable signing for commits by setting the `commit.gpgsign` config variable to `true`

```bash
git config commit.gpgsign true
```

#### Signing with SSH key

To use the ssh-key to sign the commits, set `gpg.format` to ssh, and update `user.signingkey` to the path of the ssh public key.

```bash
PUB_KEY="path to ssh public key" # set appropriate path

git config gpg.format ssh
git config user.signingkey $PUB_KEY

# Optionally, add your ssh key added into the "allowedSignersFile"
# gloablly in your home/.config, so that git can trust your ssh key
mkdir -p ~/.config/git
EMAIL=$(git config --get user.email)
echo "$EMAIL $(cat $PUB_KEY)" >> ~/.config/git/allowed-signers
git config --global gpg.ssh.allowedSignersFile ~/.config/git/allowed-signers
```

#### Signing with GPG key

To use gpg keys to sign the commits

```bash
git config gpg.format openpgp
git config user.signingkey "GPG KEY" #set GPG key value
```

### Useful commands for developers

This repo contains [SKA ci-cd makefile](https://gitlab.com/ska-telescope/sdi/ska-cicd-makefile) repository as a submodule, which provides us with some standard commands out of the box.

It is **recommended** to use these instead of their simpler counterparts. For example, use `make python-test` instead of `pytest`.

Run `make help` to get list of all supported commands. Some of the mostly used commands are listed below:

``` bash
# Formatting the code
make python-format

# Linting checks
make python-lint

# Running tests
make python-test

# Generating source files for docs
make -C docs/ create-doc

# Building html documentation
make docs-build html
```
