from ska_sdp_piper.piper.configurations import ConfigParam, Configuration
from ska_sdp_piper.piper.stage import ConfigurableStage

from ..stubs.predict import predict_for_channels


@ConfigurableStage(
    "predict_stage",
    configuration=Configuration(
        cell_size=ConfigParam(
            float, 60.0, description="Cell size in arcsecond"
        ),
        epsilon=ConfigParam(float, 1e-4),
    ),
)
def predict_stage(upstream_output, epsilon, cell_size):
    """
    Perform model prediction

    Parameters
    ----------
        upstream_output: dict
            Output from the upstream stage
        epsilon: float
            Epsilon
        cell_size: float
            Cell size in arcsecond

    Returns
    -------
        dict
    """

    ps = upstream_output["ps"]
    model_image = upstream_output["model_image"]

    ps = ps.assign(
        {
            "VISIBILITY_MODEL": predict_for_channels(
                ps, model_image, epsilon, cell_size
            )
        }
    )

    return {"ps": ps}
