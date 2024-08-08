import logging


class OnlyDebugInfoFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= logging.INFO


class RemoveSrcFromLoggerNameFilter(logging.Filter):
    remove_str = "importer.src."

    def filter(self, record: logging.LogRecord) -> logging.LogRecord:
        if record.name.startswith(self.remove_str):
            return_name_from = len(self.remove_str)
            record.name = record.name[return_name_from:]
        return record
