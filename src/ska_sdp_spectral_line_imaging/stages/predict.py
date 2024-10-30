import logging
import os

import numpy as np

from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage
from ska_sdp_piper.piper.utils.log_util import delayed_log

from ..stubs.predict import predict_for_channels
from ..util import export_data_as

logger = logging.getLogger()


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
        export_model: bool
            If True, export model.

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

    peak_flux = np.abs(
        ps.VISIBILITY_MODEL.mean(dim=["time", "baseline_id"]).max(
            dim="frequency"
        )
    )

    peak_amp = np.abs(
        ps.VISIBILITY.mean(dim=["time", "baseline_id"])
        .isel(polarization=0)
        .max()
    )

    upstream_output.add_compute_tasks(
        delayed_log(
            logger.info,
            "Peak flux in Model: {peak_flux}."
            " Peak amplitude of visibilities: {peak_amp}",
            peak_flux=peak_flux,
            peak_amp=peak_amp,
        ),
    )

    upstream_output["ps"] = ps

    return upstream_output
