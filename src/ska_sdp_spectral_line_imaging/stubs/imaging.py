# pylint: disable=import-error,no-name-in-module,no-member

import logging

import dask
import dask.array
import ducc0.wgridder as wgridder
import numpy as np
import xarray as xr
from ska_sdp_datamodels.image import Image, import_image_from_fits
from ska_sdp_func_python.image.deconvolution import fit_psf

from .deconvolution import deconvolve, restore_cube
from .model import subtract_visibility
from .predict import predict_for_channels


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
        # TODO: this shouuld be parameterized
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
                dask.array.ones_like(ps.VISIBILITY),
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
    dirty_image,
    n_iter_major,
    gridding_params,
    deconvolution_params,
    polarization_frame,
    wcs,
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
        dirty_image: ska_sdp_datamodels.image.image_model.Image
            The dirty image after continuum subtraction
        n_iter_major: int
            Number of major iterations
        gridding_params: dict
            Prameters to perform gridding.
        deconvolution_params: dict
            Deconvolution parameters
        polarization_frame: PolarisationFrame
            Polarisation information
        wcs: WCS
            WCS information
        beam_info:
            Beam information

    Returns
    -------
        restored_image: ska_sdp_datamodels.image.image_model.Image
        residual_image: ska_sdp_datamodels.image.image_model.Image
    """
    # TODO: Gridding parameters should have units documented somewhere
    epsilon = gridding_params.get("epsilon")
    cell_size = gridding_params.get("cell_size")
    nx = gridding_params.get("nx")
    ny = gridding_params.get("ny")
    residual_ps = ps

    logger = logging.getLogger()

    if psf_image_path is None:
        psf_image = generate_psf_image(
            ps, cell_size, nx, ny, epsilon, wcs, polarization_frame
        )
    else:
        # TODO: This returns image but frequency axis is not aligned
        psf_image = import_image_from_fits(psf_image_path, fixpol=True)

        # TODO: Will be removed once coordinate issue is fixed
        # The frequency coords have floating point precision issue
        psf_image = psf_image.assign_coords(dirty_image.coords)

    model_image = Image.constructor(
        data=dask.array.zeros_like(dirty_image.pixels.data),
        polarisation_frame=dirty_image.image_acc.polarisation_frame,
        wcs=dirty_image.image_acc.wcs,
    )

    if any(value is None for value in beam_info.values()):
        # TODO: Make fit_psf dask compliant to remove psf_image.load()
        logger.warning(
            "Calculating beam info. "
            "This causes entire psf_image to LOAD INTO MEMORY. "
            "This will slow down the computation."
        )
        psf_image.load()
        beam_info = fit_psf(psf_image)

    logger.info(f"Beam information: {beam_info}")

    for _ in range(n_iter_major):

        model_image_iter, _ = deconvolve(
            dirty_image, psf_image, **gridding_params, **deconvolution_params
        )

        model_image = model_image.assign(
            {"pixels": model_image.pixels + model_image_iter.pixels}
        )

        # TODO: Remove once data models are standardized
        if "polarisation" in model_image.coords:  # pragma: no cover
            model_image = model_image.rename({"polarisation": "polarization"})

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
            model_image = model_image.rename({"polarization": "polarisation"})

        model_ps = residual_ps.assign({"VISIBILITY": model_visibility})

        residual_ps = subtract_visibility(ps, model_ps)

        dirty_image = cube_imaging(
            residual_ps, cell_size, nx, ny, epsilon, wcs, polarization_frame
        )

    model_image_last, residual_image = deconvolve(
        dirty_image, psf_image, **gridding_params, **deconvolution_params
    )

    model_image = model_image.assign(
        {"pixels": model_image.pixels + model_image_last.pixels}
    )

    restored_image = restore_cube(model_image, beam_info, residual_image)

    return restored_image, residual_image
