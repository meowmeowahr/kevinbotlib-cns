import logging

from loguru import logger


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        level = logger.level(record.levelname).name

        frame, depth = logging.currentframe(), 1
        while frame.f_code.co_filename in [logging.__file__, __file__]:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def init_logging():
    loggers = (
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
        if name.startswith("uvicorn.")
    )
    for uvicorn_logger in loggers:
        uvicorn_logger.handlers = []

    intercept_handler = InterceptHandler()
    logging.getLogger("uvicorn").handlers = [intercept_handler]
