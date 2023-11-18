import logging


def get_logger(module: str) -> logging.Logger:
    logger = logging.getLogger(name=module)
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler.setFormatter(fmt=formatter)

    logger.addHandler(hdlr=handler)

    return logger
