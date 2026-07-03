import logging


def configure_logging(log_level: str = "INFO", layer: str = "common", entity: str = "logging"):
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(f"{layer}.{entity}")
    return logger