import xarray


def subtract_visibility(
    target_observation: xarray.Dataset, src_observation: xarray.Dataset
) -> xarray.Dataset:
    """
    Subtract model visibility from visibility,
    returning dataset with new visibility.

    :param target_observation: The target observation dataset from which
        visibility needs to be subtracted
    :param src_observation: The observtion dataset providing the visibilities
        to be subtracted
    :return: New observation with subtracted visibilities
    """

    subtracted_visibility = (
        target_observation.VISIBILITY - src_observation.VISIBILITY
    )

    subtracted_visibility = subtracted_visibility.assign_attrs(
        target_observation.VISIBILITY.attrs
    )

    return target_observation.assign({"VISIBILITY": subtracted_visibility})
