# Changelog

## Latest
- Add `flagging_stage`.
- Add `benchmark-pipeline` command to run benchmarks.
- The `read_model` stage can accept a FITS spectral cube as a model image.
- The `read_model` and `imaging` stage parameters have changed. Please refer the "Stage Configs" section of the documentation.
- The `cont_sub` stage reports the peak channel, and is controlled via the configuration parameter `report_peak_channel` of type boolean. 
- Added auto-complete script for bash and zsh
- Use `subtract_visibility` and `convert_polarization` from `ska-sdp-func-python`
- Remove `input_polarisation_frame` from `vis_stokes_conversion` stage
- Rename `output_polarisation_frame` to `output_polarizations`, this option will consume an array of string for converting polarization frames
- Add dask worker plugin to configure logger. Introduce delayed logger
- Move export image name and format to imaging stage, with the addition of export flags for image, psf, model, and residual.
- Remove all export stages. The export happens from the respective stages where the data is generated.
- Move read processing set logic from piper framework to spectral line imaging pipeline
- Delay read model fits reads and add power law scaling for modell
- Update xradio version to 0.0.40

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
