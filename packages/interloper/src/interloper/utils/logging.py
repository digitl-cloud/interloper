import logging


def basic_logging(level: int = logging.DEBUG) -> None:
    # Configure all loggers that start with "interloper"
    interloper_logger = logging.getLogger("interloper")
    interloper_logger.setLevel(level)
    interloper_logger.propagate = False  # Prevent logs from bubbling up

    # Create a console handler
    handler = logging.StreamHandler()
    handler.setLevel(level)

    # Set a formatter
    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)-32s | %(message)s")
    handler.setFormatter(formatter)

    # Remove existing handlers to avoid duplicate logs
    interloper_logger.handlers.clear()
    interloper_logger.addHandler(handler)

    # Also configure all other `interloper_*` loggers dynamically
    logging.captureWarnings(True)  # Capture warnings as logs
    for name in logging.root.manager.loggerDict.keys():
        if name.startswith("interloper"):
            logging.getLogger(name).setLevel(level)
            logging.getLogger(name).propagate = False  # Avoid duplicate logs
            logging.getLogger(name).handlers.clear()
            logging.getLogger(name).addHandler(handler)
