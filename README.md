# SKA SDP Spectral Line Imaging

## Description

This repository consists of the spectral line imaging pipeline.

## Pipeline Installation

Please visit [docs](https://developer.skao.int/projects/ska-sdp-spectral-line-imaging/en/latest/installation.html) for instructions on how to setup, install and run the spectral line imaging pipeline.

## Developer Setup

Please make sure to clone the submodules using

```bash
git clone --recurse-submodules url://git-repository
```

Also make sure to update submodules at every pull.

**Updating submodule post clone**

```bash
git submodule update --init
```

**Setting up conda and installing pipeline**

It is recommended to use a virtual environment for the developer setup. This document explains the developer setup using `conda`.

The `conda` based approach, sets up an environment with packages defined in ``environment.yaml``. This approach makes sure that the poetry and python versions used with this repo do not conflict with any previously installed versions on your machine.

You can run the scripts present in ``/scripts`` directory to create a conda environment and install pipeline.

```bash
./scripts/recreate-conda-env.sh

conda activate spec_line

scripts/install-pipeline.sh
```

**Formatting and Linting your code**

```
make python-format
make python-lint
```

**Running the existing tests**

```
make python-test
```

**Updating documentation of API**

```
make -C docs/ create-doc
```

**Git hooks**

To enable `git-hooks` for the current repository, please link the `.githooks` folder to the `core.hooksPath` variable of the `git` configuration.

```bash
git config --local core.hooksPath .githooks/
```

The pre-commit hook is defined for the main branch and is present in the `.githooks` folder.
The current pre-commit hook runs the following

1. `pylint` set to fail on warnings.
1. Tests on `src` folder
    1. Coverage not to fall below 80%.
1. Build documentation
1. Help prepare commit message as per agreed format

**GPG signing the commits**

First, make sure that your email address matches the email address of your remote repository (github/gitlab) account. Only then the commits will be shown as "verified".

To set the username and email for the your local repository only:

```bash
git config user.name <username>
git config user.email <email address>
```

Now, enable signing for commits by setting the `commit.gpgsign` config variable to `true`

```bash
git config commit.gpgsign true
```

To use the ssh-key to sign the commits, set `gpg.format` to ssh, and update `user.signingkey` to the path of the ssh public key.

```bash
PUB_KEY=<path to ssh public key>
EMAIL=$(git config --get user.email)
git config gpg.format ssh
git config user.signingkey $PUB_KEY

# Optionally, add your ssh key added into the "allowedSignersFile"
# gloablly in your home/.config, so that git can trust your ssh key
mkdir -p ~/.config/git
echo "$EMAIL $(cat $PUB_KEY)" >> ~/.config/git/allowed-signers
git config --global gpg.ssh.allowedSignersFile ~/.config/git/allowed-signers
```

To use gpg keys to sign the commits

```bash
git config gpg.format openpgp
git config user.signingkey <GPG KEY>
```
