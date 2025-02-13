import logging

from termcolor import colored


# Configure logging
class ColoredFormatter(logging.Formatter):
    COLORS = {"SERVER": "cyan", "CLIENT": "yellow", "INFO": "green"}

    def format(self, record):
        if hasattr(record, "color_group"):
            prefix = colored(f"[{record.color_group}]", self.COLORS[record.color_group])
            return f"{prefix} {super().format(record)}"
        return super().format(record)


# Setup loggers
def setup_logger(name, color_group):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredFormatter("%(message)s"))
    logger.addHandler(handler)
    logger.color_group = color_group
    return logger
