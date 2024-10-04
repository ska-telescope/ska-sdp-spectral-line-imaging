# pylint: disable=import-error,no-name-in-module,no-member
import ducc0.wgridder as wgridder
import numpy as np
import xarray as xr
from astropy import units as au
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS
from ska_sdp_datamodels.image.image_model import Image
from ska_sdp_datamodels.science_data_model.polarisation_model import (
    PolarisationFrame,
)
from ska_sdp_func_python.image import deconvolve_cube

from .model import subtract_visibility
from .predict import predict_for_channels

polarization_lookup = {
    "_".join(value): key
    for key, value in PolarisationFrame.polarisation_frames.items()
}

# TODO: get_wcs is untested temporary function.
# Once stubbed imager is replaced by a proper imager
# we expect that the imager will give the image class instance
# with wcs information already populated.


def get_wcs(ps, cell_size, nx, ny):
    """
    Creates WCS from processing set

    Parameters
    ----------
        ps: xarray.Dataset
            Observation
        cell_size: float
            Cell size in arcseconds.
        nx: int
            Image size X
        ny: int
            Image size Y

    Returns
    -------
        WCS object
    """

    assert (
        ps.field_and_source_xds.FIELD_PHASE_CENTER.units[0] == "rad"
    ), "Phase field center value is not defined in radian."
    assert (
        ps.field_and_source_xds.FIELD_PHASE_CENTER.units[1] == "rad"
    ), "Phase field center value is not defined in radian."

    cell_size_degree = cell_size / 3600
    freq_channel_width = ps.frequency.channel_width["data"]
    ref_freq = ps.frequency.reference_frequency["data"]

    fp_frame = ps.field_and_source_xds.FIELD_PHASE_CENTER.frame.lower()
    fp_center = ps.field_and_source_xds.FIELD_PHASE_CENTER.to_numpy()

    coord = SkyCoord(
        ra=fp_center[0] * au.rad, dec=fp_center[1] * au.rad, frame=fp_frame
    )

    new_wcs = WCS(naxis=4)

    new_wcs.wcs.crpix = [nx // 2, ny // 2, 1, 1]
    new_wcs.wcs.cunit = ["deg", "deg", ps.frequency.units[0], ""]
    new_wcs.wcs.cdelt = np.array(
        [-cell_size_degree, cell_size_degree, freq_channel_width, 1]
    )
    new_wcs.wcs.crval = [coord.ra.deg, coord.dec.deg, ref_freq, 1]
    new_wcs.wcs.ctype = ["RA---SIN", "DEC--SIN", "FREQ", "STOKES"]
    new_wcs.wcs.radesys = coord.frame.name.upper()
    new_wcs.wcs.equinox = coord.frame.equinox.jyear
    new_wcs.wcs.specsys = ps.frequency.frame

    return new_wcs


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

    return xr.DataArray(dirty, dims=["ra", "dec"])


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

    image_vec = xr.apply_ufunc(
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
        output_core_dims=[["ra", "dec"]],
        vectorize=True,
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

    return xr.DataArray(
        image_vec.data, dims=["frequency", "polarization", "ra", "dec"]
    )


def cube_imaging(ps, cell_size, nx, ny, epsilon):
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

    Returns
    -------
        ska_sdp_datamodels.image.image_model.Image
    """

    template_core_dims = ["frequency", "polarization", "ra", "dec"]
    template_chunk_sizes = {
        k: v for k, v in ps.chunksizes.items() if k in template_core_dims
    }
    output_xr = xr.DataArray(
        np.empty(
            (
                ps.sizes["frequency"],
                ps.sizes["polarization"],
                int(nx),
                int(ny),
            )
        ),
        dims=template_core_dims,
    ).chunk(template_chunk_sizes)

    cell_size_radian = np.deg2rad(cell_size / 3600)

    cube_data = xr.map_blocks(
        chunked_imaging,
        ps,
        template=output_xr,
        kwargs=dict(
            nx=int(nx),
            ny=int(ny),
            epsilon=epsilon,
            cell_size=float(cell_size_radian),
        ),
    )

    polarization_frame = PolarisationFrame(
        polarization_lookup["_".join(ps.polarization.data)]
    )

    wcs = get_wcs(ps, cell_size, nx, ny)

    return Image.constructor(
        data=cube_data.data,
        polarisation_frame=polarization_frame,
        wcs=wcs,
    )


def clean_cube(
    ps, psf_image, n_iter_major, gridding_params, deconvolution_params
):
    epsilon = gridding_params.get("epsilon", 1e-4)
    cell_size = gridding_params.get("cell_size", None)
    nx = gridding_params.get("nx", 256)
    ny = gridding_params.get("ny", 256)

    image = cube_imaging(ps, cell_size, nx, ny, epsilon)
    residual_ps = ps

    def image_restoration(model, residual):
        # Restoration of cube image
        return model

    for _iter in range(n_iter_major):
        model_image, residual_image = deconvolve_cube(
            image, psf_image, **deconvolution_params
        )

        if _iter == n_iter_major - 1:
            image = image_restoration(model_image, residual_image)
            break

        model_vis = residual_ps.assign(
            {
                "VISIBILITY": predict_for_channels(
                    residual_ps, model_image, epsilon, cell_size
                )
            }
        )

        residual_ps = subtract_visibility(ps, model_vis)
        image = cube_imaging(residual_ps, cell_size, nx, ny, epsilon)

    return image
