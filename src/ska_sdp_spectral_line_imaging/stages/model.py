# pylint: disable=no-member,import-error
import logging
import os
from typing import Tuple

import dask
import dask.array
import dask.delayed
import numpy as np
import xarray as xr
from astropy.io import fits
from astropy.wcs import WCS
from ska_sdp_func_python.xradio.visibility.operations import (
    subtract_visibility,
)
from ska_sdp_func_python.xradio.visibility.polarization import (
    convert_polarization,
)

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage
from ska_sdp_piper.piper.utils import delayed_log

from ..upstream_output import UpstreamOutput
from ..util import export_to_zarr

logger = logging.getLogger()

fits_codes_to_pol_names = {
    1: "I",
    2: "Q",
    3: "U",
    4: "V",
    -1: "RR",
    -2: "LL",
    -3: "RL",
    -4: "LR",
    -5: "XX",
    -6: "YY",
    -7: "XY",
    -8: "YX",
}

fits_axis_to_image_dims = {
    "RA": "x",
    "DEC": "y",
    "FREQ": "frequency",
    "STOKES": "polarization",
}


@dask.delayed
def read_fits_memmapped_delayed(image_path, hduid=0):
    with fits.open(
        image_path, mode="denywrite", memmap=True, lazy_load_hdus=True
    ) as hdul:
        hdu = hdul[hduid]
        data = hdu.data

    return data


def get_dask_array_from_fits(
    image_path: str,
    hduid: int,
    shape: Tuple,
    dtype: type,
    blocksize: Tuple = None,
):
    if blocksize:
        raise NotImplementedError(
            "Chunking of FITS image is not yet supported"
        )

    data = dask.array.from_delayed(
        read_fits_memmapped_delayed(image_path, hduid),
        shape=shape,
        dtype=dtype,
    )

    return data


def get_dataarray_from_fits(image_path, hduid=0, chunksizes={}):
    """
    Reads FITS image and returns an xarray dataarray with
    dimensions ["polarization", "frequency", "y", "x"] or
    only ["y", "x"] if data is 2 dimensionsional.

    Function can also read coordinte values for dimensions "polarization"
    and "frequency". Spatial coordinates "y" and "x" are linear, and
    the their coordinate values are not populatedin output dataarray.
    If needed, those can be populated later.
    Refer ska_sdp_datamodels.image.Image.constructor.

    The image data is read as a dask array using delayed read calls to
    astropy.fits.open.

    Parameters
    ----------
    image_path: str
        Path to FITS image

    hduid: int
        The HDU number in the HDUList read from FITS image.

    chunksizes: dict
        A dictionary mapping a image dimension to its chunk size.
        Most like these chunksizes will come from the one of the
        visibility xarray dataarray.
        This will be used to read FITS image using slices.
        **This feature is not implemented yet**.

    Returns
    -------
        xarray.DataArray

    Raises
    ------
        NotImplementedError
            If chunksizes are passed as parameter
    """
    # opening image only to get metadata
    with fits.open(image_path, memmap=True) as hdul:
        hdu = hdul[hduid]
        shape = hdu.data.shape
        dtype = hdu.data.dtype

    wcs = WCS(image_path)

    dimensions = [
        fits_axis_to_image_dims[axis] for axis in reversed(wcs.axis_type_names)
    ]

    coordinates = {}
    if "frequency" in dimensions:
        spectral_wcs = wcs.sub(["spectral"])
        frequency_range = spectral_wcs.wcs_pix2world(
            range(spectral_wcs.pixel_shape[0]), 0
        )[0]
        coordinates["frequency"] = frequency_range
    if "polarization" in dimensions:
        pol_wcs = wcs.sub(["stokes"])
        pol_codes = pol_wcs.wcs_pix2world(range(pol_wcs.pixel_shape[0]), 0)[0]
        pol_names = [fits_codes_to_pol_names[code] for code in pol_codes]
        coordinates["polarization"] = pol_names

    if chunksizes:
        raise NotImplementedError("Chunks for FITS image is not yet supported")

    data = get_dask_array_from_fits(image_path, hduid, shape, dtype)

    return xr.DataArray(
        data,
        dims=dimensions,
        coords=coordinates,
        name="fits_image_arr",
    )


def apply_power_law_scaling(
    image: xr.DataArray,
    frequency_range: np.ndarray,
    reference_frequency: float = None,
    spectral_index: float = 0.75,
):
    """
    Apply power law scaling on a continuum image and return a scaled cube.

    The "image" must be a xarray dataarray.
    If "frequency" dimension of size 1 is present in image,
    then that value is used as reference frequency for scaling.
    Else, user can pass any reference frequency
    as "reference_frequency" argument, which takes preference.

    If "data" attribute of image is a dask array, then this will return
    a dataarray which wraps a new dask array containing operations
    for power law scaling. This means, the values in returned dataarray
    are not computed eagerly.

    The formula for power law scaling is given as:

    ..math

        S2 = S1 * ( \\nu2 / \\nu1 ) ^ {-\\alpha}

    Parameters
    ----------
        image: xr.DataArray
            Image data array. The "data" attribute of dataarray can either
            be a numpy array or a dask array.
        frequency_range: numpy.ndarray | dask.array
            Frequency range in hertz over which to scale the data.
        reference_frequency: float, optional
            Refernce frequency in hertz. If not passed, function expects that
            a frequency coordinate is present in the image. If passed, this
            takes priority over the frequency cordinates of image.
        spectral_index: float, optional
            Spectral index (alpha) used in power law scaling.
            Defaults to 0.75.

    Returns
    -------
        xr.DataArray
            A 3-dimensional scaled spectral cube
    """
    _ref_freq = None
    if "frequency" in image.dims:
        if image.frequency.size != 1:
            logger.warn(
                "Can not apply power law scaling on a spectral cube."
                "Ignoring passed argument and continuing pipeline"
            )
            return image

        _ref_freq = image.frequency.data
        # Need to remove frequency dimension
        # for broadcasting to work later
        image = image.squeeze(dim="frequency", drop=True)

    if reference_frequency:
        _ref_freq = reference_frequency

    if _ref_freq is None:
        raise Exception(
            "reference_frequency is not passed, and "
            "'frequency' dimension is not present in the input image. "
            "Can not proceed with power law scaling."
        )

    channel_multipliers = np.power(
        (frequency_range / _ref_freq), -spectral_index
    )
    channel_multipliers_da = xr.DataArray(
        channel_multipliers,
        dims=["frequency"],
        coords={"frequency": frequency_range},
    )

    scaled_cube = image * channel_multipliers_da
    return scaled_cube


@ConfigurableStage(
    "read_model",
    configuration=Configuration(
        image=ConfigParam(
            str,
            "/path/to/wsclean-%s-image.fits",
            description="""
            Path to the image file. The value must have a
            `%s` placeholder to fill-in polarization values.

            The polarization values are taken from the polarization
            coordinate present in the processing set in upstream_output.

            For example, if polarization coordinates are ['I', 'Q'],
            and `image` param is `/data/wsclean-%s-image.fits`, then the
            read_model stage will try to read
            `/data/wsclean-I-image.fits` and
            `/data/wsclean-Q-image.fits` images.

            Please refer
            `README <README.html#regarding-the-model-visibilities>`_
            to understand the requirements of the model image.
            """,
            nullable=False,
        ),
        do_power_law_scaling=ConfigParam(
            bool,
            False,
            description="Perform power law scaling to scale model "
            "image across channels. Only applicable for continuum images.",
        ),
        spectral_index=ConfigParam(
            float,
            0.75,
            description="Spectral index to perform power law scaling",
        ),
    ),
)
def read_model(
    upstream_output: UpstreamOutput,
    image: str,
    do_power_law_scaling: bool,
    spectral_index: float,
) -> UpstreamOutput:
    """
    Read model image(s) from FITS file(s).
    Supports reading from continuum or spectral FITS images.

    Please refer `README <../README.html#regarding-the-model-visibilities>`_
    to understand the requirements of the model image.

    Parameters
    ----------
        upstream_output: UpstreamOutput
            Output from the upstream stage

        image: str
            Path to the image file. The path must have a
            `%s` placeholder to fill-in polarization values at runtime.
            The polarization values are taken from the polarization
            coordinate present in the processing set in upstream_output.

            For example, if polarization coordinates are `['I', 'Q']`,
            and `image` is `/data/wsclean-%s-image.fits`, then the
            **read_model** stage will try to read
            `/data/wsclean-I-image.fits` and
            `/data/wsclean-Q-image.fits` images.

            If the corresponding image is not available in filesystem,
            this stage will raise an exception.

        image_type: str
            Whether all the images being read are "continuum"
            or "spectral"

    Returns
    -------
        UpstreamOutput
    """
    ps = upstream_output.ps
    pols = ps.polarization.values

    images = []
    for pol in pols:
        image_path = image % pol
        image_xr = get_dataarray_from_fits(image_path)
        images.append(image_xr)

    if "polarization" in images[0].dims:
        model_image = xr.concat(images, dim="polarization")
    else:
        # stack the images creating new polarization axis
        # assuming that all images have same dims and coords
        model_image_data = dask.array.stack(images, axis=0)
        model_image = xr.DataArray(
            model_image_data,
            dims=["polarization", *images[0].dims],
            name="model_image",
        )
        model_image = model_image.assign_coords(images[0].coords)
        model_image = model_image.assign_coords({"polarization": pols})

    if do_power_law_scaling:
        model_image = apply_power_law_scaling(
            model_image,
            ps.frequency.data,
            spectral_index=spectral_index,
        )

    if "frequency" in model_image.dims and model_image.frequency.size == 1:
        # continuum image with extra dimension on frequency
        model_image = model_image.squeeze(dim="frequency", drop=True)

    model_image = model_image.chunk(
        dict(
            polarization=-1,
        )
    )

    upstream_output["model_image"] = model_image

    return upstream_output


@ConfigurableStage(
    "vis_stokes_conversion",
    configuration=Configuration(
        output_polarizations=ConfigParam(
            list,
            ["I", "Q"],
            description="List of desired polarization codes, in the order "
            "they will appear in the output dataset polarization axis",
        ),
    ),
)
def vis_stokes_conversion(upstream_output, output_polarizations):
    """
    Visibility to stokes conversion

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        output_polarizations: list
            List of desired polarization codes, in the order they will appear
            in the output dataset polarization axis

    Returns
    -------
        dict
    """
    ps = upstream_output.ps

    upstream_output["ps"] = convert_polarization(ps, output_polarizations)

    return upstream_output


def _fit_polynomial_on_visibility(data):
    """
    Perform polynomial fit across frequency axis.

    Parameters
    ----------
    data: xarray.DataArray
        A DataArray with dimensions ["time", "baseline_id", "polarization",
        "frequencies"] in any sequence.

    Returns
    -------
    dask.delayed.Delayed
        A dask delayed call to numpy polynomial polyfit function
    """
    mean_vis = data.mean(
        dim=["time", "baseline_id", "polarization"], skipna=True
    )
    weights = np.isfinite(mean_vis)
    mean_vis_finite = xr.where(weights, mean_vis, 0.0)
    xaxis = dask.array.arange(mean_vis_finite.size)

    return dask.delayed(np.polynomial.polynomial.polyfit)(
        xaxis, mean_vis_finite, w=weights, deg=1
    )


@ConfigurableStage(
    "continuum_subtraction",
    configuration=Configuration(
        export_residual=ConfigParam(
            bool, False, description="Export the residual visibilities"
        ),
        psout_name=ConfigParam(
            str,
            "vis_residual",
            description="Output file name prefix of residual data",
            nullable=False,
        ),
        report_poly_fit=ConfigParam(
            bool,
            False,
            description="Whether to report extent of continuum subtraction "
            "by fitting polynomial across channels",
        ),
    ),
)
def cont_sub(
    upstream_output,
    export_residual,
    psout_name,
    report_poly_fit,
    _output_dir_,
):
    """
    Perform continuum subtraction

    Parameters
    ----------
        upstream_output: UpstreamOutput
            Output from the upstream stage
        export_residual: bool
            Export the residual visibilities
        psout_name: str
            Output file name prefix of residual data

    Returns
    -------
        UpstreamOutput
    """

    ps = upstream_output.ps

    model = ps.assign({"VISIBILITY": ps.VISIBILITY_MODEL})
    cont_sub_ps = subtract_visibility(ps, model)

    if export_residual:
        output_path = os.path.join(_output_dir_, psout_name)
        upstream_output.add_compute_tasks(
            export_to_zarr(
                cont_sub_ps.VISIBILITY, output_path, clear_attrs=True
            )
        )

    # TODO: This has to be in ska-sdp-func-python's function
    cont_sub_ps = cont_sub_ps.assign(
        {
            "VISIBILITY": cont_sub_ps.VISIBILITY.assign_attrs(
                ps.VISIBILITY.attrs
            )
        }
    )
    # Sending a copy to avoid issues where attributes get cleared
    upstream_output["ps"] = cont_sub_ps.copy()

    # Report peak visibility and corresponding channel
    abs_visibility = np.abs(cont_sub_ps.VISIBILITY)
    max_freq_axis = abs_visibility.max(
        dim=["time", "baseline_id", "polarization"]
    )
    peak_channel = max_freq_axis.argmax()
    peak_frequency = max_freq_axis.idxmax()
    max_visibility = abs_visibility.max()
    unit = cont_sub_ps.frequency.units[0]

    upstream_output.add_compute_tasks(
        delayed_log(
            logger.info,
            "Peak visibility Channel: {peak_channel}."
            " Frequency: {peak_frequency} {unit}."
            " Peak Visibility: {max_visibility}",
            peak_channel=peak_channel,
            peak_frequency=peak_frequency,
            max_visibility=max_visibility,
            unit=unit,
        )
    )

    # Report extent of continuum subtraction
    if report_poly_fit:
        pols = ps.polarization.values
        valid_sets = [{"XX", "YY"}, {"RR", "LL"}]
        if not set(pols) in valid_sets:
            logger.warning("Cannot report extent of continuum subtraction.")
        else:
            cont_sub_vis = cont_sub_ps.VISIBILITY.where(
                np.logical_not(cont_sub_ps.FLAG)
            )
            fit_real = _fit_polynomial_on_visibility(cont_sub_vis.real)
            fit_imag = _fit_polynomial_on_visibility(cont_sub_vis.imag)
            upstream_output.add_compute_tasks(
                delayed_log(
                    logger.info,
                    "Slope of fit on real part {fit}",
                    fit=fit_real[1],
                )
            )
            upstream_output.add_compute_tasks(
                delayed_log(
                    logger.info,
                    "Slope of fit on imag part {fit}",
                    fit=fit_imag[1],
                )
            )

    return upstream_output
