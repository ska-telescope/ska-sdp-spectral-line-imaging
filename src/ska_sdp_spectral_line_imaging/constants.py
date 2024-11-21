SPEED_OF_LIGHT = 299792458

FITS_CODE_TO_POL_NAME = {
    1: "I",
    2: "Q",
    3: "U",
    4: "V",
    -1: "RR",
    -2: "LL",
    -3: "RL",
    -4: "LR",
    -5: "XX",
    -6: "YY",
    -7: "XY",
    -8: "YX",
}

POL_NAME_TO_FITS_CODE = {v: k for k, v in FITS_CODE_TO_POL_NAME.items()}

FITS_AXIS_TO_IMAGE_DIM = {
    "RA": "x",
    "DEC": "y",
    "FREQ": "frequency",
    "STOKES": "polarization",
}
