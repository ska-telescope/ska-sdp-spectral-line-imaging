Stage Configs
=============

.. This file is referenced by "imaging" stage docstring by a relative reference
.. to the generated html page.

load_data
**********

..  table::
    :width: 100%
    :widths: 25, 10, 20, 45

    +---------+--------+-----------+------------------------------------------------------+
    | Param   | Type   | Default   | Description                                          |
    +=========+========+===========+======================================================+
    | obs_id  | int    | 0         | The index of the partition present in processing set |
    +---------+--------+-----------+------------------------------------------------------+



vis_stokes_conversion
*********************

..  table::
    :width: 100%
    :widths: 25, 10, 20, 45

    +----------------------+--------+------------+---------------------------------------------------------------------------------+
    | Param                | Type   | Default    | Description                                                                     |
    +======================+========+============+=================================================================================+
    | output_polarizations | list   | ['I', 'Q'] | List of desired polarization codes, in the order they will appear in the output |
    |                      |        |            | dataset polarization axis                                                       |
    +----------------------+--------+------------+---------------------------------------------------------------------------------+

read_model
**********

..  table::
    :width: 100%
    :widths: 25, 10, 20, 45

    +------------+--------+--------------------------------+--------------------------------------------------------------------------------+
    | Param      | Type   | Default                        | Description                                                                    |
    +============+========+================================+================================================================================+
    | image      | str    | /path/to/wsclean-%s-image.fits | Path to the image file. The value must have a              `%s`                |
    |            |        |                                | placeholder to fill-in polarization values.               The polarization     |
    |            |        |                                | values are taken from the polarization              coordinate present in the  |
    |            |        |                                | processing set in upstream_output.              For example, if polarization   |
    |            |        |                                | coordinates are ['I', 'Q'],              and `image` param is                  |
    |            |        |                                | `/data/wsclean-%s-image.fits`, then the              read_model stage will try |
    |            |        |                                | to read              `/data/wsclean-I-image.fits` and                          |
    |            |        |                                | `/data/wsclean-Q-image.fits` images.              Please refer                 |
    |            |        |                                | `README <README.html#regarding-the-model-visibilities>`_              to       |
    |            |        |                                | understand the requirements of the model image.                                |
    +------------+--------+--------------------------------+--------------------------------------------------------------------------------+
    | image_type | str    | continuum                      | Type of the input images. Available options are 'spectral' or 'continuum'      |
    +------------+--------+--------------------------------+--------------------------------------------------------------------------------+


predict_stage
*************

..  table::
    :width: 100%
    :widths: 25, 10, 20, 45

    +--------------+--------+-----------+------------------------------------------+
    | Param        | Type   | Default   | Description                              |
    +==============+========+===========+==========================================+
    | cell_size    | float  | 60        | Cell size in arcsecond                   |
    +--------------+--------+-----------+------------------------------------------+
    | epsilon      | float  | 0.0001    | Floating point accuracy for ducc gridder |
    +--------------+--------+-----------+------------------------------------------+
    | export_model | bool   | False     | Whether to export the predicted model    |
    +--------------+--------+-----------+------------------------------------------+
    | psout_name   | str    | vis_model | Output path of model data                |
    +--------------+--------+-----------+------------------------------------------+


continuum_subtraction
*********************

..  table::
    :width: 100%
    :widths: 25, 10, 20, 45

    +---------------------+--------+-----------+----------------------------------------------+
    | Param               | Type   | Default   | Description                                  |
    +=====================+========+===========+==============================================+
    | report_peak_channel | bool   | True      | Report channel with peak emission/absorption |
    +---------------------+--------+-----------+----------------------------------------------+
    | export_residual     | bool   | False     | Whether to export the residual               |
    +---------------------+--------+-----------+----------------------------------------------+
    | psout_name          | str    | vis_model | Output path of residual data                 |
    +---------------------+--------+-----------+----------------------------------------------+



imaging
*******

..  table::
    :width: 100%
    :widths: 25, 10, 20, 45

    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | Param                                     | Type   | Default        | Description                                                                     |
    +===========================================+========+================+=================================================================================+
    | n_iter_major                              | int    | 0              | Number of major cycle iterations                                                |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | do_clean                                  | bool   | False          | Whether to run clean algorithm. If False, only the dirty image is generated. If |
    |                                           |        |                | True, the restored image is generated.                                          |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | psf_image_path                            | str    | None           | Path to PSF FITS image. If None, the pipeline generates the psf image.          |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | gridding_params.cell_size                 | float  | None           | Cell size of the image in arcseconds                                            |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | gridding_params.scaling_factor            | float  | 3.0            | Scaling factor used for esimation of cell size.                                 |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | gridding_params.epsilon                   | float  | 0.0001         | Floating point accuracy for ducc gridder                                        |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | gridding_params.image_size                | int    | 256            | Spatial size (nx and ny) of the spectral cube                                   |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | deconvolution_params.algorithm            | str    | multiscale     | Algorithm for deconvolution. If use_radler is False, then options are           |
    |                                           |        |                | 'hogbom'|'msclean'. With radler, the options are                                |
    |                                           |        |                | 'multiscale'|'iuwt'|'more_sane'|'generic_clean'                                 |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | deconvolution_params.gain                 | float  | 0.7            | Loop gain                                                                       |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | deconvolution_params.threshold            | float  | 0.0            | Clean threshold                                                                 |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | deconvolution_params.fractional_threshold | float  | 0.01           | Fractional threshold                                                            |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | deconvolution_params.scales               | list   | [0, 3, 10, 30] | Scales in pixels for multiscale                                                 |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | deconvolution_params.niter                | int    | 100            | Maximum number of minor cycle iterations                                        |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | deconvolution_params.use_radler           | bool   | True           | Whether to use radler or not                                                    |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | beam_info.bmaj                            | float  | None           | Beam major axis in radian                                                       |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | beam_info.bmin                            | float  | None           | Beam minor axis in radian                                                       |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | beam_info.bpa                             | float  | None           | Beam position angle in radian                                                   |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | image_name                                | str    | spectral_cube  | Output path of the spectral cube                                                |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | export_format                             | str    | fits          | Output format of the spectral cube                                               |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | export_image                              | bool   | False          | Whether to export the restored/dirty image                                      |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | export_model_image                        | bool   | False          | Whether to export the model image                                               |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | export_psf_image                          | bool   | False          | Whether to export the psf image                                                 |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+
    | export_residual_image                     | bool   | False          | Whether to export the residual image                                            |
    +-------------------------------------------+--------+----------------+---------------------------------------------------------------------------------+

