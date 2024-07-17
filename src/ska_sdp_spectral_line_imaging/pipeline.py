# This prototype additionally depends on ska_sdp_datamodels
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
# spectral_line_imaging_prototype --input <input.ms>
#
# With config overridden
# spectral_line_imaging_prototype --input <input.ms> \
# --config spectral_line_imaging_prototype.yaml
#
# pylint: disable=no-member,import-error
import astropy.io.fits as fits
import astropy.units as au
import ducc0.wgridder as wgridder
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


@ConfigurableStage(
    "select_vis",
    configuration=Configuration(
        intent=ConfigParam(str, None),
        field_id=ConfigParam(int, 0),
        ddi=ConfigParam(int, 0),
    ),
)
def select_field(pipeline_data, intent, field_id, ddi):
    ps = pipeline_data["input_data"]
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
def read_model(pipeline_data, image_name, pols):
    ps = pipeline_data["output"]["ps"]
    images = []

    for pol in pols:
        with fits.open(f"{image_name}-{pol}-image.fits") as f:
            images.append(f[0].data.squeeze())

    image_stack = xr.DataArray(
        np.stack(images), dims=["polarization", "ra", "dec"]
    )

    return {"ps": ps, "model_image": image_stack}


@ConfigurableStage("continuum_subtraction")
def cont_sub(pipeline_data):
    ps = pipeline_data["output"]["ps"]
    model = pipeline_data["output"]["model_vis"]

    return {"ps": ps.assign({"VISIBILITY": ps.VISIBILITY - model})}


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


def cube_imaging(ps, cell_size, nx, ny, epsilon=1e-4):
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
        image_vec.data,
        dims=["frequency", "polarization", "ra", "dec"],
    )


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
def imaging_stage(pipeline_data, epsilon, cell_size, nx, ny):
    ps = pipeline_data["output"]["ps"]

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

    return {"cubes": image_cube}


@ConfigurableStage("vis_stokes_conversion")
def vis_stokes_conversion(pipeline_data):
    ps = pipeline_data["output"]["ps"]

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
    "export_zarr",
    Configuration(
        image_name=ConfigParam(str, "output_image.zarr"),
    ),
)
def export_image(pipeline_data, image_name):
    cubes = pipeline_data["output"]["cubes"]
    cubes.to_zarr(store=image_name)


pipeline_1 = Pipeline(
    "spectral_line_imaging_prototype",
    stages=[
        select_field,
        vis_stokes_conversion,
        read_model,
        predict_stage,
        cont_sub,
        imaging_stage,
        export_image,
    ],
)
