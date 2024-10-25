import os

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..stubs.predict import predict_for_channels
from ..util import export_data_as


@ConfigurableStage(
    "predict_stage",
    configuration=Configuration(
        cell_size=ConfigParam(float, 60.0, "Cell size in arcsecond"),
        epsilon=ConfigParam(
            float, 1e-4, "Floating point accuracy for ducc gridder"
        ),
        export_model=ConfigParam(bool, False, "Export the predicted model"),
        psout_name=ConfigParam(str, "vis_model", "Output path of model data"),
    ),
)
def predict_stage(
    upstream_output, epsilon, cell_size, export_model, psout_name, _output_dir_
):
    """
    Perform model prediction

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        epsilon: float
            Floating point accuracy for ducc gridder
        cell_size: float
            Cell size in arcsecond

    Returns
    -------
        dict
    """

    ps = upstream_output.ps
    model_image = upstream_output.model_image

    # TODO: If VISIBILITY_MODEL already exists in ps, do we want to copy the
    # attributes of existing VISIBILITY_MODEL to new VISIBILITY_MODEL?

    ps = ps.assign(
        {
            "VISIBILITY_MODEL": predict_for_channels(
                ps, model_image, epsilon, cell_size
            )
        }
    )

    if export_model:
        output_path = os.path.join(_output_dir_, psout_name)
        upstream_output.add_compute_tasks(
            export_data_as(
                ps.VISIBILITY_MODEL, output_path, export_format="zarr"
            )
        )

    upstream_output["ps"] = ps

    return upstream_output
