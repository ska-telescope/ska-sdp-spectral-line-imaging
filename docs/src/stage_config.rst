Stage Configs
=============

select_vis
**********


+---------+--------+-----------+------------------------------------------------------+
| Param   | Type   | Default   | Description                                          |
+=========+========+===========+======================================================+
| obs_id  | int    | 0         | The index of the partition present in processing set |
+---------+--------+-----------+------------------------------------------------------+



vis_stokes_conversion
*********************

+---------------------------+--------+------------+-------------------------------------------------------------+
| Param                     | Type   | Default    | Description                                                 |
+===========================+========+============+=============================================================+
| input_polarisation_frame  | str    | linear     | Polarization frame of the input visibility. Supported       |
|                           |        |            | options are: 'circular','circularnp', 'linear', 'linearnp', |
|                           |        |            | 'stokesIQUV', 'stokesIV', 'stokesIQ', 'stokesI'.            |
+---------------------------+--------+------------+-------------------------------------------------------------+
| output_polarisation_frame | str    | stokesIQUV | Polarization frame of the output visibilities. Supported    |
|                           |        |            | options are same as output_polarisation_frame               |
+---------------------------+--------+------------+-------------------------------------------------------------+


read_model
**********


+------------+--------+----------------------+--------------------------------------------------------------+
| Param      | Type   | Default              | Description                                                  |
+============+========+======================+==============================================================+
| image_name | str    | wsclean              | Prefix path of the image(s) which contain model data. Please |
|            |        |                      | refer `README <README.html#regarding-the-model-              |
|            |        |                      | visibilities>`_ to understand the pre-requisites of the      |
|            |        |                      | pipeline.                                                    |
+------------+--------+----------------------+--------------------------------------------------------------+
| pols       | list   | ['I', 'Q', 'U', 'V'] | Polarizations of the model images                            |
+------------+--------+----------------------+--------------------------------------------------------------+


predict_stage
*************

+-----------+--------+-----------+------------------------------------------+
| Param     | Type   | Default   | Description                              |
+===========+========+===========+==========================================+
| cell_size | float  | 60        | Cell size in arcsecond                   |
+-----------+--------+-----------+------------------------------------------+
| epsilon   | float  | 0.0001    | Floating point accuracy for ducc gridder |
+-----------+--------+-----------+------------------------------------------+


export_model
************

+------------+--------+-----------+---------------------------+
| Param      | Type   | Default   | Description               |
+============+========+===========+===========================+
| psout_name | str    | vis_model | Output path of model data |
+------------+--------+-----------+---------------------------+

continuum_subtraction
*********************

No parameters


imaging
*******

+----------------------+--------+--------------------------------+-------------------------------------------------------------+
| Param                | Type   | Default                        | Description                                                 |
+======================+========+================================+=============================================================+
| gridding_params      | dict   | {'cell_size': None,            | Gridding parameters                                         |
|                      |        | 'scaling_factor': 3.0,         |                                                             |
|                      |        | 'epsilon': 0.0001,             |                                                             |
|                      |        | 'image_size': 256}             |                                                             |
+----------------------+--------+--------------------------------+-------------------------------------------------------------+
| deconvolution_params | dict   | {'algorithm': 'multiscale',    | Deconvolution parameters                                    |
|                      |        | 'gain': 0.7, 'threshold': 0.0, |                                                             |
|                      |        | 'fractional_threshold': 0.01,  |                                                             |
|                      |        | 'scales': [0, 3, 10, 30],      |                                                             |
|                      |        | 'niter': 100, 'use_radler':    |                                                             |
|                      |        | True}                          |                                                             |
+----------------------+--------+--------------------------------+-------------------------------------------------------------+
| n_iter_major         | int    | 0                              | Number of major cycle iterations                            |
+----------------------+--------+--------------------------------+-------------------------------------------------------------+
| do_clean             | bool   | False                          | Whether to run clean algorithm. If False, only the dirty    |
|                      |        |                                | image is generated. If True, the restored image is          |
|                      |        |                                | generated.                                                  |
+----------------------+--------+--------------------------------+-------------------------------------------------------------+
| psf_image_path       | str    |                                | Path to PSF FITS image. If None, the pipeline generates the |
|                      |        |                                | psf image.                                                  |
+----------------------+--------+--------------------------------+-------------------------------------------------------------+
| beam_info            | dict   | {'bmaj': None, 'bmin': None,   | Beam information. If any value is None, pipeline calculates |
|                      |        | 'bpa': None}                   | beam information using psf image.                           |
+----------------------+--------+--------------------------------+-------------------------------------------------------------+


export_residual
***************

+------------+--------+--------------+------------------------------+
| Param      | Type   | Default      | Description                  |
+============+========+==============+==============================+
| psout_name | str    | vis_residual | Output path of residual data |
+------------+--------+--------------+------------------------------+

export_image
************

+------------+--------+---------------+----------------------------------+
| Param      | Type   | Default       | Description                      |
+============+========+===============+==================================+
| image_name | str    | spectral_cube | Output path of the spectral cube |
+------------+--------+---------------+----------------------------------+

