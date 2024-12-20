Stages and configurations
#########################

.. This page is generated using docs/generate_config.py

The descriptions of each stage are copied from the docstrings of stages.
Refer to the `API page for stages <api/ska_sdp_spectral_line_imaging.stages.html>`_

Each stage has parameters, which are defined in the YAML config file passed to the pipeline.


load_data
*********

    Reads processing set, selects one partition, and returns xarray dataset
    to be used by further stage.
    The proceessing set path is passed through ``--input`` option from cli.

Parameters
==========

..  table::
    :width: 100%
    :widths: 15, 10, 10, 45, 10, 10

    +---------+--------+-----------+------------------------------------------------------+------------+------------------+
    | Param   | Type   | Default   | Description                                          | Nullable   | Allowed values   |
    +=========+========+===========+======================================================+============+==================+
    | obs_id  | int    | 0         | The index of the partition present in processing set | True       |                  |
    +---------+--------+-----------+------------------------------------------------------+------------+------------------+


vis_stokes_conversion
*********************

    Converts visibilities to expected output polarizations.
    The visibilities are taken from processing set in the
    upstream_output.

Parameters
==========

..  table::
    :width: 100%
    :widths: 15, 10, 10, 45, 10, 10

    +----------------------+--------+------------+---------------------------------------------------------------------------------+------------+------------------+
    | Param                | Type   | Default    | Description                                                                     | Nullable   | Allowed values   |
    +======================+========+============+=================================================================================+============+==================+
    | output_polarizations | list   | ['I', 'Q'] | List of desired polarization codes, in the order they will appear in the output | True       |                  |
    |                      |        |            | dataset polarization axis                                                       |            |                  |
    +----------------------+--------+------------+---------------------------------------------------------------------------------+------------+------------------+


read_model
**********

    Read model image(s) from FITS file(s).
    Supports reading from continuum or spectral FITS images.

    Please refer to "Getting Started" in the documentation
    (or "README.md" in the repository)
    to understand the requirements of the model image.

    If `do_power_law_scaling` is True, this function can scale model image
    across channels. This is only applicable for images with single
    frequency channel. The formula for power law scaling is as follows

    .. math::

        scaled\_channel = current\_channel *
            (\frac{channel\_frequency}
            {reference\_frequency})^{-\alpha}

    Where:

        - :math:`{channel\_frequency}`: Frequency of the current channel
        - :math:`{reference\_frequency}`: Reference frequency
        - :math:`{\alpha}`: Spectral index

Parameters
==========

..  table::
    :width: 100%
    :widths: 15, 10, 10, 45, 10, 10

    +----------------------+--------+--------------------------------+----------------------------------------------------------------------------------+------------+------------------+
    | Param                | Type   | Default                        | Description                                                                      | Nullable   | Allowed values   |
    +======================+========+================================+==================================================================================+============+==================+
    | image                | str    | /path/to/wsclean-%s-image.fits | Path to the image file. The value must have a             `%s`                   | False      |                  |
    |                      |        |                                | placeholder to fill-in polarization values.              The polarization values |            |                  |
    |                      |        |                                | are taken from the polarization             coordinate present in the processing |            |                  |
    |                      |        |                                | set in upstream_output.              For example, if polarization coordinates    |            |                  |
    |                      |        |                                | are ['I', 'Q'],             and `image` param is `/data/wsclean-%s-image.fits`,  |            |                  |
    |                      |        |                                | then the             read_model stage will try to read                           |            |                  |
    |                      |        |                                | `/data/wsclean-I-image.fits` and             `/data/wsclean-Q-image.fits`        |            |                  |
    |                      |        |                                | images.              Please refer             `README <README.html#regarding-    |            |                  |
    |                      |        |                                | the-model-visibilities>`_             to understand the requirements of the      |            |                  |
    |                      |        |                                | model image.                                                                     |            |                  |
    +----------------------+--------+--------------------------------+----------------------------------------------------------------------------------+------------+------------------+
    | do_power_law_scaling | bool   | False                          | Whether to perform power law scaling to scale             model                  | True       |                  |
    |                      |        |                                | image across channels. Only applicable for             continuum images.         |            |                  |
    +----------------------+--------+--------------------------------+----------------------------------------------------------------------------------+------------+------------------+
    | spectral_index       | float  | 0.75                           | Spectral index (alpha) to perform power law scaling.                             | True       |                  |
    |                      |        |                                | Note: The ratio of frequencies is raised to `-spectral_index`             Please |            |                  |
    |                      |        |                                | refer `read_model stage                                                          |            |                  |
    |                      |        |                                | <api/ska_sdp_spectral_line_imaging.stages.model.html>`_ for                      |            |                  |
    |                      |        |                                | information on the formula.                                                      |            |                  |
    +----------------------+--------+--------------------------------+----------------------------------------------------------------------------------+------------+------------------+


predict_stage
*************

    Predicts visibilities from model image data using ducc0.wgridder.

Parameters
==========

..  table::
    :width: 100%
    :widths: 15, 10, 10, 45, 10, 10

    +--------------+--------+-----------+------------------------------------------+------------+------------------+
    | Param        | Type   | Default   | Description                              | Nullable   | Allowed values   |
    +==============+========+===========+==========================================+============+==================+
    | cell_size    | float  | 60.0      | Cell size in arcsecond                   | True       |                  |
    +--------------+--------+-----------+------------------------------------------+------------+------------------+
    | epsilon      | float  | 0.0001    | Floating point accuracy for ducc gridder | True       |                  |
    +--------------+--------+-----------+------------------------------------------+------------+------------------+
    | export_model | bool   | False     | Export the predicted model               | True       |                  |
    +--------------+--------+-----------+------------------------------------------+------------+------------------+
    | psout_name   | str    | vis_model | Output path of model data                | False      |                  |
    +--------------+--------+-----------+------------------------------------------+------------+------------------+


continuum_subtraction
*********************

    Perform subtraction of visibilities.
    The "VISIBILITY" and "VISIBILITY_MODEL" are taken
    from upstream_output.

Parameters
==========

..  table::
    :width: 100%
    :widths: 15, 10, 10, 45, 10, 10

    +-----------------+--------+--------------+--------------------------------------------------------------------------------+------------+------------------+
    | Param           | Type   | Default      | Description                                                                    | Nullable   | Allowed values   |
    +=================+========+==============+================================================================================+============+==================+
    | export_residual | bool   | False        | Export the residual visibilities                                               | True       |                  |
    +-----------------+--------+--------------+--------------------------------------------------------------------------------+------------+------------------+
    | psout_name      | str    | vis_residual | Output file name prefix of residual data                                       | False      |                  |
    +-----------------+--------+--------------+--------------------------------------------------------------------------------+------------+------------------+
    | report_poly_fit | bool   | False        | Whether to report extent of continuum subtraction by fitting polynomial across | True       |                  |
    |                 |        |              | channels                                                                       |            |                  |
    +-----------------+--------+--------------+--------------------------------------------------------------------------------+------------+------------------+


flagging
********

    Perfoms flagging on visibilities using strategies and existing flags.

Parameters
==========

..  table::
    :width: 100%
    :widths: 15, 10, 10, 45, 10, 10

    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | Param                                        | Type   | Default   | Description                                                                     | Nullable   | Allowed values   |
    +==============================================+========+===========+=================================================================================+============+==================+
    | strategy_file                                | str    | None      | Path to the flagging strategy file (.lua). If null, a default strategy will be  | True       |                  |
    |                                              |        |           | built using strategy_configs.                                                   |            |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.base_threshold              | float  | 2.0       | Flagging sensitivity threshold. Lower means more sensitive detection            | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.iteration_count             | int    | 3         | Number of flagging iterations                                                   | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.threshold_factor_step       | float  | 4.0       | How much to increase the sensitivity each iteration                             | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.transient_threshold_factor  | float  | 5.0       | Transient RFI threshold. Decreasing this value makes detection of transient RFI | False      |                  |
    |                                              |        |           | more aggressive                                                                 |            |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.threshold_timestep_rms      | float  | 3.0       | RMS sigma threshold for time domain                                             | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.threshold_channel_rms       | float  | 99.0      | RMS sigma threshold for frequency domain                                        | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.keep_outliers               | bool   | True      | Keep frequency outliers during channel rms threshold.                           | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.keep_original_flags         | bool   | True      | Consider the original flags while applying strategy                             | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.low_pass_filter.do_low_pass | bool   | False     | Do low pass filtering                                                           | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.low_pass_filter.window_size | list   | [11, 21]  | Kernel size for low pass filtering                                              | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.low_pass_filter.time_sigma  | float  | 6.0       | Sigma threshold for time domain                                                 | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | strategy_configs.low_pass_filter.freq_sigma  | float  | 7.0       | Sigma threshold for frequency domain                                            | False      |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | export_flags                                 | bool   | False     | Export the Flags                                                                | True       |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+
    | psout_name                                   | str    | flags     | Output path of flags                                                            | True       |                  |
    +----------------------------------------------+--------+-----------+---------------------------------------------------------------------------------+------------+------------------+


imaging
*******

    Performs clean algorithm on the visibilities present in
    processing set. Processing set is present in from the upstream_output.

    For detailed parameter info, please refer to
    "Stages and configurations" section in the documentation.

Parameters
==========

..  table::
    :width: 100%
    :widths: 15, 10, 10, 45, 10, 10

    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | Param                                     | Type   | Default                             | Description                                                             | Nullable   | Allowed values                                                            |
    +===========================================+========+=====================================+=========================================================================+============+===========================================================================+
    | gridding_params.cell_size                 | float  | None                                | Cell Size for gridding in arcseconds. Will be calculated if None.       | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | gridding_params.scaling_factor            | float  | 3.0                                 | Scalling parameter for gridding                                         | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | gridding_params.epsilon                   | float  | 0.0001                              | Epsilon                                                                 | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | gridding_params.image_size                | int    | 256                                 | Image Size for gridding. Will be calculated if None                     | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | deconvolution_params.algorithm            | str    | generic_clean                       | Deconvolution algorithm. Note that 'hogbom' and 'msclean'               | False      | ['multiscale', 'iuwt', 'more_sane', 'generic_clean', 'hogbom', 'msclean'] |
    |                                           |        |                                     | are only allowed when radler is not used.                               |            |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | deconvolution_params.gain                 | float  | 0.7                                 | Gain                                                                    | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | deconvolution_params.threshold            | float  | 0.0                                 | Threshold                                                               | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | deconvolution_params.fractional_threshold | float  | 0.01                                | Fractional Threshold                                                    | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | deconvolution_params.scales               | list   | [0, 3, 10, 30]                      | Scalling Value for multiscale                                           | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | deconvolution_params.niter                | int    | 100                                 | Minor cycle iterations.                                                 | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | deconvolution_params.use_radler           | bool   | True                                | Flag for radler                                                         | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | n_iter_major                              | int    | 1                                   | Number of major cycle iterations.  If 0, only dirty image is generated. | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | psf_image_path                            | str    | None                                | Path to PSF FITS image. If None, the pipeline generates the psf image.  | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | beam_info                                 | dict   | {'bmaj': None, 'bmin': None, 'bpa': | Clean beam information, each value is in degrees                        | True       |                                                                           |
    |                                           |        | None}                               |                                                                         |            |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | image_name                                | str    | spectral_cube                       | Output path of the spectral cube                                        | False      |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | export_format                             | str    | fits                                | Data format for the image. Allowed values: fits|zarr                    | True       | ['fits', 'zarr']                                                          |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | export_model_image                        | bool   | False                               | Whether to export the model image generated as part of clean.           | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | export_psf_image                          | bool   | False                               | Whether to export the psf image.                                        | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+
    | export_residual_image                     | bool   | False                               | Whether to export the residual image generated as part of clean.        | True       |                                                                           |
    +-------------------------------------------+--------+-------------------------------------+-------------------------------------------------------------------------+------------+---------------------------------------------------------------------------+


