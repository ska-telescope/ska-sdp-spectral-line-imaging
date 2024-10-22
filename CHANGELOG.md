# Changelog

## Latest

- The `read_model` stage can accept a FITS spectral cube as a model image.
- The `read_model` and `imaging` stage parameters have changed. Please refer the "Stage Configs" section of the documentation.

## 0.5.0

- Update documentation
- Add support for radler deconvolver
- Add script to install config and run pipeline

## 0.4.0

- Implement clean algorithm using ducc gridder and hogbom deconvolver
- Allow pipeline to write spectral cube in FITS format
- Add functionality to estimate cell size and image size for imaging
- Add option `--set` to sub-command `install-config` to override config
- Fix vulnerabilities in docker image
- Performance improvements in the diagnostic stage

## v0.3.0

- Add Singularity Image
- Update Documentation
- Update default pipeline configuration

## v0.2.0

- Documentation updates
- Enable dask bench marking
- CSD3 example script
- Move linting dependencies to dev section in pyproject
- Streamline dev environment setup

## v0.1.0

Initial release of SKA SDP Spectral Line Imaging Pipeline
