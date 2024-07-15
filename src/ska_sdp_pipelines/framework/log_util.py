import logging
from datetime import datetime


def additional_log_config(base_name):
    timestamp = datetime.now()
    log_file = f"{base_name}_{timestamp.strftime('%Y-%m-%dT%H:%M:%S')}.log"
    return {
        "handlers": {
            "file": {
                "()": logging.FileHandler,
                "formatter": "default",
                "filename": log_file,
            }
        },
        "root": {
            "handlers": ["console", "file"],
        },
    }
