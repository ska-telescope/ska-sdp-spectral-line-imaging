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

from ska_sdp_pipelines.framework.configurable_stage import ConfigurableStage
from ska_sdp_pipelines.framework.configuration import (
    ConfigParam,
    Configuration,
)
from ska_sdp_pipelines.framework.pipeline import Pipeline
from ska_sdp_spectral_line_imaging.stages.data_export import (
    export_image,
    export_residual,
)
from ska_sdp_spectral_line_imaging.stages.imaging import imaging_stage
from ska_sdp_spectral_line_imaging.stages.model import (
    cont_sub,
    read_model,
    vis_stokes_conversion,
)
from ska_sdp_spectral_line_imaging.stages.predict import predict_stage


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
        dict
    """

    ps = _input_data_
    # TODO: This is a hack to get the psname
    psname = list(ps.keys())[0].split(".ps")[0]

    sel = f"{psname}.ps_ddi_{ddi}_intent_{intent}_field_id_{field_id}"

    # TODO: There is an issue in either xradio/xarray/dask that causes chunk
    # sizes to be different for coordinate variables
    return {"ps": ps[sel].unify_chunks()}


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
