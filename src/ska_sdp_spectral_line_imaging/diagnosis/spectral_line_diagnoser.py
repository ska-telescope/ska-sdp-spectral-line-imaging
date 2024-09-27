import logging

import numpy as np
import xarray as xr

from ska_sdp_piper.piper.executors import ExecutorFactory
from ska_sdp_piper.piper.scheduler import DefaultScheduler
from ska_sdp_piper.piper.utils import read_dataset, read_yml

from ..stages.select_vis import select_field
from .utils import amp_vs_channel_plot, create_plot, store_spectral_csv

logger = logging.getLogger()


class SpectralLineDiagnoser:
    """
    Spectral line imaging pipeline diagnoser class.

    Attributes
    ----------
      input_path: Path
          path of pipeline run output.
      output_dir: Path
          Location where to store diagnose output.
      pipeline_args: dict
          Dictionary of cli arguments pipeline ran with.
      pipeline_config: dict
          Dictionary of pipeline configuration.
      input_ps: Xarray.Dataset
          Input processing provided to pipeline.
      residual: Xarray.Dataset
          Residual visibilities generated by the pipeline.
          Visibilities after continuum subtraction generated by the pipeline.
      model: Xarray.DataArray
          Model used in continuum subtraction stage in pipeline.
    """

    def __init__(self, input_path, output_path, channel, dask_scheduler=None):
        """
        Initialise the Diagnoser object
        """
        self.input_path = input_path
        self.output_dir = output_path
        self.channel = channel

        cli_yaml_path = next(input_path.glob("*.cli.yml"))
        self.pipeline_args = read_yml(cli_yaml_path)

        config_yaml_path = next(self.input_path.glob("*.config.yml"))
        self.pipeline_config = read_yml(config_yaml_path)

        self.input_ps = None
        self.residual = None
        self.model = None

        self.scheduler = DefaultScheduler()

        self.executor = ExecutorFactory.get_executor(
            self.output_dir, dask_scheduler
        )

        self.__read_input_data()

    def diagnose(self):
        """
        Main method that runs the diagnosis steps.
        """

        logger.info("Creating plots...")

        flagged_vis = xr.where(
            self.input_ps.FLAG, None, self.input_ps.VISIBILITY
        )
        flagged_residual = (
            xr.where(self.input_ps.FLAG, None, self.residual.VISIBILITY)
            if self.residual is not None
            else None
        )
        flagged_model = (
            xr.where(self.input_ps.FLAG, None, self.model)
            if self.model is not None
            else None
        )

        delayed_tasks = self.__plot_uv_distance(
            flagged_vis, flagged_residual, flagged_model
        )

        self.scheduler.extend(delayed_tasks)

        input_pol = self.input_ps.polarization.values

        self.scheduler.extend(
            self.__plot_visibility(
                flagged_vis,
                "Input Visibilities",
                "input-vis",
                input_pol,
            )
        )

        if flagged_residual is not None:
            residual_pol = self.residual.polarization.values
            self.scheduler.extend(
                self.__plot_visibility(
                    flagged_residual,
                    "Residual Visibilities",
                    "residual-vis",
                    residual_pol,
                )
            )
            self.scheduler.append(self.__export_residual_csv(flagged_residual))

        self.executor.execute(self.scheduler.tasks)

        logger.info("=========== DIAGNOSE COMPLETED ===========")

    def __export_residual_csv(self, residual_vis):
        averaged_vis = (
            residual_vis.sel(polarization=self.input_ps.polarization[0])
            .mean(dim=["time", "baseline_id"])
            .values
        )
        return store_spectral_csv(
            self.residual.frequency.values,
            averaged_vis,
            self.output_dir / "residual.csv",
        )

    def __plot_visibility(
        self,
        visibilities,
        plot_title_postfix,
        file_postfix,
        label,
    ):
        delayed_tasks = []

        logger.info(f"Creating {plot_title_postfix}")
        poloarizations = self.input_ps.polarization

        delayed_tasks.append(
            amp_vs_channel_plot(
                visibilities.sel(polarization=poloarizations[0]),
                title=f"Amp Vs Channel on {plot_title_postfix}",
                path=self.output_dir
                / f"single-pol-i-amp-vs-channel-{file_postfix}.png",
                label=label[0:1],
            )
        )

        delayed_tasks.append(
            amp_vs_channel_plot(
                visibilities,
                title=f"Amp Vs Channel on {plot_title_postfix}",
                path=self.output_dir
                / f"all-pol-amp-vs-channel-{file_postfix}.png",
                label=label,
            )
        )

        return delayed_tasks

    def __plot_uv_distance(self, flagged_vis, flagged_residual, flagged_model):
        delayed_tasks = []

        uv_distance = get_uv_dist(self.input_ps.UVW)
        polarizations = self.input_ps.polarization

        input_vis = flagged_vis.sel(polarization=polarizations[0]).isel(
            frequency=self.channel
        )

        delayed_tasks.append(
            create_plot(
                np.abs(uv_distance),
                np.abs(input_vis),
                xlabel="uv distance",
                ylabel="amp",
                title="Amp vs UV Distance before Continnum Subtraction",
                path=self.output_dir
                / "amp-vs-uv-distance-before-cont-sub.png",
                label=None,
            )
        )

        if flagged_residual is not None:
            residual_vis = flagged_residual.sel(
                polarization=polarizations[0]
            ).isel(frequency=self.channel)

            delayed_tasks.append(
                create_plot(
                    np.abs(uv_distance),
                    np.abs(residual_vis),
                    xlabel="uv distance",
                    ylabel="amp",
                    title="Amp vs UV Distance after Continnum Subtraction",
                    path=self.output_dir
                    / "amp-vs-uv-distance-after-cont-sub.png",
                    label=None,
                )
            )

        if flagged_model is not None:
            model_vis = flagged_model.sel(polarization=polarizations[0]).isel(
                frequency=self.channel
            )

            delayed_tasks.append(
                create_plot(
                    np.abs(uv_distance),
                    np.abs(model_vis),
                    xlabel="uv distance",
                    ylabel="amp",
                    title="Amp vs UV Distance model",
                    path=self.output_dir / "amp-vs-uv-distance-model.png",
                    label=None,
                )
            )

        return delayed_tasks

    def __read_input_data(self):
        """
        Read
            - input processing set
            - residual
            - model data

        required for diagnosis.
        """

        pipeline_parameter = self.pipeline_config["parameters"]
        select_vis_config = pipeline_parameter["select_vis"]
        pipeline_run_config = self.pipeline_config["pipeline"]
        input_ps = read_dataset(self.pipeline_args["input"])
        logger.info("Reading input visibility")
        self.input_ps = select_field.stage_definition(
            None,
            **select_vis_config,
            _input_data_=input_ps,
        )["ps"]

        if pipeline_run_config.get("export_model"):
            logger.info("Reading model data")
            ps_out = pipeline_parameter["export_model"]["psout_name"] + ".zarr"
            self.model = xr.open_zarr(self.input_path / ps_out)[
                "VISIBILITY_MODEL"
            ]
        else:
            logger.info("Export model stage not run.")

        if pipeline_run_config.get("export_residual"):
            logger.info("Reading residual data")
            ps_out = (
                pipeline_parameter["export_residual"]["psout_name"] + ".zarr"
            )
            self.residual = xr.open_zarr(self.input_path / ps_out)
        else:
            logger.info("Export residual stage not run.")


def get_uv_dist(uvw):
    uvw_t = uvw.transpose("uvw_label", "time", "baseline_id")
    return (uvw_t[0] ** 2 + uvw_t[1] ** 2) ** 0.5
