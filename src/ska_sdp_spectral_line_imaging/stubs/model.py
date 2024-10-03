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
    :return: New processing set with subtracted visibilities
    """

    return target_observation.assign(
        {
            "VISIBILITY": target_observation.VISIBILITY
            - src_observation.VISIBILITY
        }
    )
