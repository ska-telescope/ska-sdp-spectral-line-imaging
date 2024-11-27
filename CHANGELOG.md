# Changelog

## 0.6.2

- Add support for power law scaling in `read_model` stage
- Add support for configurable spectral line imaging flaggin strategy
- Output FITS images are now written to seperate file per polarisation

## 0.6.1

- Add `flagging_stage`.
- The `read_model` stage can accept a FITS spectral cube as a model image.
- The `read_model` and `imaging` stage parameters have changed. Please refer the "Stage Configs" section of the documentation.
- The `cont_sub` stage reports the peak visibility in subtracted visibilities and the corresponding channel
- The `cont_sub` stage can optionally report extent of polynomial fit, based on input configuration
- Use `subtract_visibility` and `convert_polarization` from `ska-sdp-func-python`
- Remove `input_polarisation_frame` from `vis_stokes_conversion` stage
- Rename `output_polarisation_frame` to `output_polarizations`, this option will consume an array of string for converting polarization frames
- Add dask worker plugin to configure logger
- Introduce delayed logger, used to log values which need computations
- Remove all export stages. The export happens from the respective stages where the data is generated.
- Move configs related to exported image name and image format to imaging stage, with the addition of export flags for image, psf, model, and residual.
- Move read processing set logic from piper framework to spectral line imaging pipeline
- Delay reading of FITS model images till execution of graph
- Add support for power law scaling of continuum model images
- Add `benchmark` subcommand in `piper` which can be used to benchmark pipeline using `dool`
- Update xradio version to 0.0.40
- Add auto-complete script for bash and zsh

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

## 0.3.0

- Add Singularity Image
- Update Documentation
- Update default pipeline configuration

## 0.2.0

- Documentation updates
- Enable dask bench marking
- CSD3 example script
- Move linting dependencies to dev section in pyproject
- Streamline dev environment setup

## 0.1.0

Initial release of SKA SDP Spectral Line Imaging Pipeline
