# Developer Guide

## Cloning the repository

Please make sure to clone the submodules with:

```bash
git clone --recurse-submodules git@gitlab.com:ska-telescope/sdp/ska-sdp-spectral-line-imaging.git
```

Also make sure to update submodules after every pull with:

```bash
git submodule update --init
```

## Setting up conda and installing pipeline

It is recommended to use a virtual environment for the developer setup. The `conda` based approach, sets up an environment with packages defined in `environment.yaml`. This makes sure that the python and poetry
versions used with this repo does not conflict with any previous poetry installed on your machine.

You can run the scripts present in `/scripts` directory to create a conda environment and install the pipeline with its dependencies.

```bash
./scripts/recreate-conda-env.sh

conda activate spec_line

scripts/install-pipeline.sh
```

## Git hooks

To enable `git-hooks` for the current repository, run

```bash
make dev-git-hooks
```

The `pre-commit` hook is defined for the main branch and is present in the `.githooks` folder. The current `pre-commit` hook runs the following

1. If there is a change in either `environment.yml` or `pyproject.toml`, prompt the user to ask whether the local environment has been updated.
1. Run `pylint`, which is set to fail on warnings.
1. Run `pytest`, with coverage not to fall below 80%.
1. Build documentation

A `prepare-commit-msg` hook runs after `pre-commit` hook, which helps to prepare a commit message as per agreed format.

A `pre-push` hook checks the gitlab-ci pipeline status, and warns user if the status is not "success".

> Note: Due to interactiveness of these githooks, it is recommended to run all git commands using a terminal. GUI based git applications might throw errors.

## GPG signing the commits

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

### Signing with SSH key

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

### Signing with GPG key

To use gpg keys to sign the commits

```bash
git config gpg.format openpgp
git config user.signingkey "GPG KEY" #set GPG key value
```

## Useful commands for developers

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

# Building oci images
make oci-build-all
```

## Making a release

We follow the guidelines for a release given [on this page](https://developer.skatelescope.org/en/latest/tutorial/release-management/automate-release-process.html#how-to-make-a-release).

Following steps are simplified, and specific for this repository:

1. Make sure that all the changes are commited, and the local git working area is clean. Since we follow [trunk based development](https://developer.skao.int/en/latest/explanation/branching-policy.html#trunk-based-development), the active branch should be the `main` branch.

1. Check the current version using `make show-version`.

1. To bump up the release, run either one of the following commands

    ```bash
    make bump-patch-release
    make bump-minor-release
    make bump-major-release
    ```

    Above commands automtically should update the version information in following files:

    - .release
    - pyproject.toml
    - docs/src/conf.py ('release' and 'version' variable)

    If it doesn't happen automtically, please make mannual changes.

    The `make show-version` command should now show the next version. Use this version for all the later changes.

1. Apart from above files, we need to update `__version__` variable in following modules:

    - `src/ska_sdp_piper/__init__.py`
    - `src/ska_sdp_spectral_line_imaging/__init__.py`

1. Add a new `H2 (##)` heading in [CHANGELOG.md](./CHANGELOG.md), and add release notes under that heading. The heading should be the new version number.

1. Update the `docker pull` command example under the "Containerized usage" under "Getting Started" section in [README](./README.md). The docker tag needs to be updated to new version.

1. Using the new version, create a new issue on the [Release Management](https://jira.skatelescope.org/projects/REL/summary) Jira Project with a summary of your release, and set it to "IN PROGRESS".

1. Stage all the changes, create a new commit with:

    1. The JIRA Ticket ID = ID of the release issue created in previous step.

    1. The commit message title = "Bump version to V.V.V"

1. Push the changes to the `main` branch. Make sure that the pipeline is green.

1. Create a git-tag for the new version using `make create-git-tag` command.

1. Run `make push-git-tag` to push the tag to main branch.
