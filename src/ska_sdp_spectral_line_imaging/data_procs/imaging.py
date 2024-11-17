# pylint: disable=import-error,no-name-in-module,no-member

import logging

import dask.array
import ducc0.wgridder as wgridder
import numpy as np
import xarray as xr
from ska_sdp_datamodels.image import Image, import_image_from_fits
from ska_sdp_func_python.xradio.visibility.operations import (
    subtract_visibility,
)

from ..constants import SPEED_OF_LIGHT
from ..util import (
    estimate_cell_size_in_arcsec,
    estimate_image_size,
    get_polarization_frame_from_observation,
    get_wcs_from_observation,
)
from .deconvolution import deconvolve, restore_cube
from .predict import predict_for_channels

logger = logging.getLogger()


def image_ducc(
    weight,
    flag,
    uvw,
    freq,
    vis,
    cell_size,
    nx,
    ny,
    epsilon,
    nchan,
    ntime,
    nbaseline,
):
    """
    Perform imaging using ducc0.gridder

    Parameters
    ----------
        weight: numpy.array
            Weights array
        flag: numpy.array
            Flag array
        uvw: numpy.array
            Polarization array
        freq: numpy.array
            Frequency array
        vis: numpy.array
            Visibility array
        cell_size: float
            Cell size in arcsecond
        nx: int
            Size of image X
        ny: int
            Size of image y
        epsilon: float
            Epsilon
        nchan: int
            Number of channel dimension
        ntime: int
            Number of time dimension
        nbaseline: int
            Number of baseline dimension

    Returns
    -------
        xarray.DataArray
    """

    # Note: There is a conversion to float 32 here
    vis_grid = vis.reshape(ntime * nbaseline, nchan).astype(np.complex64)
    uvw_grid = uvw.reshape(ntime * nbaseline, 3)
    weight_grid = weight.reshape(ntime * nbaseline, nchan).astype(np.float32)
    freq_grid = freq.reshape(nchan)

    dirty = wgridder.ms2dirty(
        uvw_grid,
        freq_grid,
        vis_grid,
        weight_grid,
        nx,
        ny,
        cell_size,
        cell_size,
        0,
        0,
        epsilon,
        nthreads=1
        #         mask=flag_xx
    )

    return dirty


def chunked_imaging(ps, cell_size, nx, ny, epsilon=1e-4):
    """
    Perform imaging on individual chunks

    Parameters
    ----------
        ps: xarray.Dataset
            Observation
        cell_size: float
            Cell size in radian
        nx: int
            Image size X
        ny: int
            Image size Y
        epsilon: float
            Epsilon

    Returns
    -------
        xarray.DataArray
    """

    image_cube = xr.apply_ufunc(
        image_ducc,
        ps.WEIGHT,
        ps.FLAG,
        ps.UVW,
        ps.frequency,
        ps.VISIBILITY,
        input_core_dims=[
            ["time", "baseline_id"],
            ["time", "baseline_id"],
            ["time", "baseline_id", "uvw_label"],
            [],
            ["time", "baseline_id"],
        ],
        output_core_dims=[["y", "x"]],
        vectorize=True,
        keep_attrs=True,
        dask="parallelized",
        # TODO: parameterize dtype
        output_dtypes=[np.float32],
        dask_gufunc_kwargs={
            "output_sizes": {"y": ny, "x": nx},
        },
        kwargs=dict(
            nchan=1,
            ntime=ps.time.size,
            nbaseline=ps.baseline_id.size,
            cell_size=cell_size,
            epsilon=epsilon,
            nx=nx,
            ny=ny,
        ),
    )

    # not considering flags for now
    norm_vect = ps.WEIGHT.sum(dim=["time", "baseline_id"])

    image_cube = image_cube / norm_vect

    return image_cube


def cube_imaging(ps, cell_size, nx, ny, epsilon, wcs, polarization_frame):
    """
    Creates an Image object from a xarray dataset

    Parameters
    ----------
        ps: xarray.Dataset
            Observation
        cell_size: float
            Cell size in arcsecond
        nx: int
            Image size X
        ny: int
            Image size Y
        epsilon: float
            Epsilon
        wcs: WCS
            WCS Information
        polarization_frame: PolarizationFrame
            Polarization information

    Returns
    -------
        ska_sdp_datamodels.image.image_model.Image
    """
    cell_size_radian = np.deg2rad(cell_size / 3600)

    cube_data = chunked_imaging(
        ps,
        nx=int(nx),
        ny=int(ny),
        epsilon=epsilon,
        cell_size=float(cell_size_radian),
    )

    return Image.constructor(
        data=cube_data.data,
        polarisation_frame=polarization_frame,
        wcs=wcs,
    )


def generate_psf_image(
    ps, cell_size, nx, ny, epsilon, wcs, polarization_frame
):
    """
    Creates a PSF Image object from a xarray dataset

    Parameters
    ----------
        ps: xarray.Dataset
            Observation
        cell_size: float
            Cell size in arcsecond
        nx: int
            Image size X
        ny: int
            Image size Y
        epsilon: float
            Epsilon
        wcs: WCS
            WCS Information
        polarization_frame: PolarizationFrame
            Polarization information

    Returns
    -------
        ska_sdp_datamodels.image.image_model.Image
    """

    psf_ps = ps.assign(
        {
            "VISIBILITY": xr.DataArray(
                dask.array.ones_like(ps.VISIBILITY.data),
                attrs=ps.VISIBILITY.attrs,
                coords=ps.VISIBILITY.coords,
            )
        }
    )

    psf_image = cube_imaging(
        psf_ps, cell_size, nx, ny, epsilon, wcs, polarization_frame
    )

    # TODO: Do we have to make sure that peak of the psf_image is 1.0?
    # assert np.isclose(psf_image.max() , 1.0)

    return psf_image


def clean_cube(
    ps,
    psf_image_path,
    n_iter_major,
    gridding_params,
    deconvolution_params,
    beam_info,
):
    """
    Perform cube clean on an xarray dataset

    Parameters
    ----------
        ps: xarray.Dataset
            Observation
        psf_image_path: str
            File path to psf image stored in FITS format
        n_iter_major: int
            Number of major iterations
        gridding_params: dict
            Prameters to perform gridding.
        deconvolution_params: dict
            Deconvolution parameters
        beam_info:
            Clean beam e.g. {"bmaj":0.1, "bmin":0.05, "bpa":-60.0}.
            Units are deg, deg, deg

    Returns
    -------
        clean products: dict(
           str -> ska_sdp_datamodels.image.image_model.Image
        )
    """
    epsilon = gridding_params.get("epsilon")
    cell_size = gridding_params.get("cell_size")
    nx = gridding_params.get("nx")
    ny = gridding_params.get("ny")

    polarization_frame = get_polarization_frame_from_observation(ps)
    wcs = get_wcs_from_observation(ps, cell_size, nx, ny)

    dirty_image = cube_imaging(
        ps,
        cell_size,
        nx,
        ny,
        epsilon,
        wcs,
        polarization_frame,
    )

    imaging_products = {"dirty": dirty_image}

    if n_iter_major > 0:
        if psf_image_path is None:
            psf_image = generate_psf_image(
                ps, cell_size, nx, ny, epsilon, wcs, polarization_frame
            )
        else:
            logger.warning(
                f"Will load FITS psf image from {psf_image_path} "
                "into the memory of the client node. "
                "This may slow down the computations."
            )
            # TODO: Replace this with "get_dataarray_from_fits"
            psf_image = import_image_from_fits(psf_image_path, fixpol=True)

            # TODO: Remove once coordinate issue is fixed
            # The frequency coords have floating point precision issue
            psf_image = psf_image.assign_coords(dirty_image.coords)

        model_image = Image.constructor(
            data=dask.array.zeros_like(dirty_image.pixels.data),
            polarisation_frame=polarization_frame,
            wcs=wcs,
        )

        residual_image = dirty_image
        residual_ps = ps.copy(deep=False)

        for _ in range(n_iter_major):

            model_image_iter, _ = deconvolve(
                residual_image,
                psf_image,
                **gridding_params,
                **deconvolution_params,
            )

            model_image = model_image.assign(
                {"pixels": model_image.pixels + model_image_iter.pixels}
            )

            # TODO: Remove once data models are standardized
            if "polarisation" in model_image.coords:  # pragma: no cover
                model_image = model_image.rename(
                    {"polarisation": "polarization"}
                )

            model_visibility = predict_for_channels(
                residual_ps,
                model_image.pixels,
                epsilon,
                cell_size,
            )
            model_visibility = model_visibility.assign_attrs(
                residual_ps.VISIBILITY.attrs
            )

            # TODO: Remove once data models are standardized
            if "polarization" in model_image.coords:  # pragma: no cover
                model_image = model_image.rename(
                    {"polarization": "polarisation"}
                )

            model_ps = residual_ps.assign({"VISIBILITY": model_visibility})

            # TODO: Attrs are skipped in v0.5.1 ska-sdp-func-python
            residual_ps = subtract_visibility(ps, model_ps)

            residual_image = cube_imaging(
                residual_ps,
                cell_size,
                nx,
                ny,
                epsilon,
                wcs,
                polarization_frame,
            )

        restored_image = restore_cube(
            model_image,
            psf_image,
            residual_image,
            beam_info,
        )

        return {
            "model": model_image,
            "psf": psf_image,
            "residual": residual_image,
            "restored": restored_image,
        }

    return imaging_products


def get_cell_size_from_obs(observation, scaling_factor):
    """
    A helper function which reads UVW and other metadata from
    xradio observation dataset,
    and estimates cell size to be used for imaging.

    The function is dask compatible, i.e. input dask arrays are
    not eagerly computed. Consumer of this function must call `compute()`
    on the returned object to get the actual values.

    Parameters
    ----------
        observation: xarray.Dataset
            Xradio observation
        scaling_factor: float
            Scaling factor for estimation of cell size

    Returns
    -------
        xarray.Dataarray
            Dataarray which wraps a dask array of size 1, representing
            cell size value.
    """
    umax, vmax, _ = np.abs(observation.UVW).max(dim=["time", "baseline_id"])
    # TODO: handle units properly. eg. Hz, MHz etc.
    #  Assumption, current unit is Hz.
    maximum_frequency = observation.frequency.max()
    minimum_wavelength = SPEED_OF_LIGHT / maximum_frequency

    # Taking maximum of u and v baselines, rounded
    max_baseline = np.maximum(umax, vmax).round(2)

    return estimate_cell_size_in_arcsec(
        max_baseline, minimum_wavelength, scaling_factor
    )


def get_image_size_from_obs(observation, cell_size):
    """
    A helper function which reads antenna information and other metadata from
    xradio observation dataset,
    and estimates image size to be used for imaging.

    The function is dask compatible, i.e. input dask arrays are
    not eagerly computed. Consumer of this function must call `compute()`
    on the returned object to get the actual values.

    Parameters
    ----------
        observation: xarray.Dataset
            Xradio observation
        cell_size: float
            Cell size in arcsecond.

    Returns
    -------
        xarray.Dataarray
            Dataarray which wraps a dask array of size 1, representing
            image size value.
    """
    maximum_wavelength = SPEED_OF_LIGHT / observation.frequency.min()
    # rounded to 2 decimals
    min_antenna_diameter = observation.antenna_xds.DISH_DIAMETER.min().round(2)

    return estimate_image_size(
        maximum_wavelength, min_antenna_diameter, cell_size
    )
