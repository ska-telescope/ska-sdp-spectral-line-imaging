SKA SDP Spectral Line Imaging
--

## Description
This repository consists of the spectral line imaging pipeline.

## Developer Setup

It is recommended to use a virtual environment for the developer setup. This document explains the developer setup using `conda`

**Setup and activate environment**

The `conda` based approach, sets up an environment with `python 3.10`, `pip 24.0` and `poetry 1.8`

```
$> conda env create -f environment.yml
$> conda activate spec_line
```

**Install dependencies**

`poetry` is used for dependency management.

```
$> poetry install
```

**Git hooks**

The pre-commit hook is defined for the main branch and is present in the `.githooks` folder. To enable `git-hooks` for the current repository please link the `.githooks` folder to the `core.hooksPath` variable of the `git` configuration

```
$> git config --local core.hooksPath .githooks/
```

The current pre-commit hook runs the following 
1. Tests on `src` folder
2. `pylint` set to fail on warnings. **[To be enabled once code is added]**
3. Coverage test to not fall below 80%  **[To be enabled once code is added]**

## To Be updated
 - Installation 
 - Usage 
 - Support 
