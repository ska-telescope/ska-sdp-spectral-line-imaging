# This pipeline additionally depends on ska_sdp_datamodels
# and ska_sdp_func_python
#
# Image the line free channels for the continuum model
# wsclean --size 256 256 --scale 60arcsec --pol IQUV <input.ms>
#
# Installing the pipeline
#
# sdp-pipeline install pipeline.py
#
# Running the pipline
#
# spectral_line_imaging_pipeline --input <input.ms>
#
# With config overridden
# spectral_line_imaging_pipeline --input <input.ms> \
# --config spectral_line_imaging_pipeline.yaml
#
# pylint: disable=no-member,import-error

import os

import astropy.io.fits as fits
import astropy.units as au
import numpy as np
import xarray as xr
from ska_sdp_datamodels.science_data_model.polarisation_functions import (
    convert_pol_frame,
)
from ska_sdp_datamodels.science_data_model.polarisation_model import (
    PolarisationFrame,
)

from ska_sdp_pipelines.framework.configurable_stage import ConfigurableStage
from ska_sdp_pipelines.framework.configuration import (
    ConfigParam,
    Configuration,
)
from ska_sdp_pipelines.framework.pipeline import Pipeline
from ska_sdp_spectral_line_imaging.stages.predict_stage import predict_stage
from ska_sdp_spectral_line_imaging.stubs.imaging import cube_imaging


@ConfigurableStage(
    "select_vis",
    configuration=Configuration(
        intent=ConfigParam(str, None),
        field_id=ConfigParam(int, 0),
        ddi=ConfigParam(int, 0),
    ),
)
def select_field(upstream_output, intent, field_id, ddi, _input_data_):
    """
    Selects the field from processing set
    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
        intent: str
            Name of the intent field
        field_id: int
            ID of the field in the processing set
        ddi: int
            Data description ID
        _input_data_: ProcessingSet
            Input processing set
    Returns
    -------
        Dictionary
    """

    ps = _input_data_
    # TODO: This is a hack to get the psname
    psname = list(ps.keys())[0].split(".ps")[0]

    sel = f"{psname}.ps_ddi_{ddi}_intent_{intent}_field_id_{field_id}"

    # TODO: There is an issue in either xradio/xarray/dask that causes chunk
    # sizes to be different for coordinate variables
    return {"ps": ps[sel].unify_chunks()}


@ConfigurableStage(
    "read_model",
    configuration=Configuration(
        image_name=ConfigParam(str, "wsclean"),
        pols=ConfigParam(list, ["I", "Q", "U", "V"]),
    ),
)
def read_model(upstream_output, image_name, pols):
    """
    Read model from the image
    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
        image_name: str
            Name of the image to be read
        pos: list(str)
            Polarizations to be included
    Returns
    -------
        Dictionary
    """

    ps = upstream_output["ps"]
    images = []

    for pol in pols:
        with fits.open(f"{image_name}-{pol}-image.fits") as f:
            images.append(f[0].data.squeeze())

    image_stack = xr.DataArray(
        np.stack(images), dims=["polarization", "ra", "dec"]
    )

    return {"ps": ps, "model_image": image_stack}


@ConfigurableStage("continuum_subtraction")
def cont_sub(upstream_output):
    """
    Perform continuum subtraction
    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
    Returns
    -------
        Dictionary
    """

    ps = upstream_output["ps"]
    model = upstream_output["model_vis"]

    return {"ps": ps.assign({"VISIBILITY": ps.VISIBILITY - model})}


@ConfigurableStage(
    "imaging",
    configuration=Configuration(
        epsilon=ConfigParam(float, 1e-4),
        cell_size=ConfigParam(
            float, 15.0, description="Cell size in arcsecond"
        ),
        nx=ConfigParam(int, 256, description="Image size x"),
        ny=ConfigParam(int, 256, description="Image size y"),
    ),
)
def imaging_stage(upstream_output, epsilon, cell_size, nx, ny):
    """
    Creates a dirty image using ducc0.gridder
    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
        epsilon: float
            Epsilon
        cell_size: float
            Cell size in arcsecond
        nx: int
            Image size x
        ny: int
            Image size y
    Returns
    -------
        Dictionary
    """

    ps = upstream_output["ps"]

    template_core_dims = ["frequency", "polarization", "ra", "dec"]
    template_chunk_sizes = {
        k: v for k, v in ps.chunksizes.items() if k in template_core_dims
    }
    output_xr = xr.DataArray(
        np.empty(
            (
                ps.sizes["frequency"],
                ps.sizes["polarization"],
                nx,
                ny,
            )
        ),
        dims=template_core_dims,
    ).chunk(template_chunk_sizes)

    cell_size_radian = (cell_size * au.arcsecond).to(au.rad).value

    image_cube = xr.map_blocks(
        cube_imaging,
        ps,
        template=output_xr,
        kwargs=dict(
            nx=nx,
            ny=ny,
            epsilon=epsilon,
            cell_size=cell_size_radian,
        ),
    )

    return {"ps": ps, "cubes": image_cube}


@ConfigurableStage("vis_stokes_conversion")
def vis_stokes_conversion(upstream_output):
    """
    Visibility to stokes conversion
    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
    Returns
    -------
        Dictionary
    """

    ps = upstream_output["ps"]

    converted_vis = xr.apply_ufunc(
        convert_pol_frame,
        ps.VISIBILITY,
        kwargs=dict(
            ipf=PolarisationFrame("linear"),
            opf=PolarisationFrame("stokesIQUV"),
            polaxis=3,
        ),
        dask="allowed",
    )

    return {"ps": ps.assign(dict(VISIBILITY=converted_vis))}


@ConfigurableStage(
    "export_residual",
    Configuration(
        psout_name=ConfigParam(str, "residual.zarr"),
    ),
)
def export_residual(upstream_output, psout_name, _output_dir_):
    """
    Export continuum subtracted residual
    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
        psout_name: str
            Output file name
        _output_dir_: str
            Output directory created for the run
    Returns
    -------
        upstream_output
    """

    ps = upstream_output["ps"]
    output_path = os.path.abspath(os.path.join(_output_dir_, psout_name))
    ps.VISIBILITY.to_zarr(store=output_path)
    return upstream_output


@ConfigurableStage(
    "export_zarr",
    Configuration(
        image_name=ConfigParam(str, "output_image.zarr"),
    ),
)
def export_image(upstream_output, image_name, _output_dir_):
    """
    Export the generated cube image
    Parameters
    ----------
        upstream_output: Any
            Output from the upstream stage
        image_name: str
            Output file name
        _output_dir_: str
            Output directory created for the run
    Returns
    -------
        upstream_output
    """
    cubes = upstream_output["cubes"]
    output_path = os.path.join(_output_dir_, image_name)

    cubes.to_zarr(store=output_path)
    return upstream_output


spectral_line_imaging_pipeline = Pipeline(
    "spectral_line_imaging_pipeline",
    stages=[
        select_field,
        vis_stokes_conversion,
        read_model,
        predict_stage,
        cont_sub,
        imaging_stage,
        export_residual,
        export_image,
    ],
)
