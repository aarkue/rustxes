from typing import Optional
from .rustxes import import_xes_rs
import polars


def import_xes(path: str, date_format: Optional[str] = None, print_debug: Optional[bool] = None) -> tuple[polars.DataFrame, str]:
    """
     Import an XES event log

     Returns a tuple of a Polars [DataFrame] for the event data and a json-encoding of  all log attributes/extensions/classifiers

     * `path` - The filepath of the .xes or .xes.gz file to import
     * `date_format` - Optional date format to use for parsing `<date>` tags (See https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
     * `print_debug` - Optional flag to enable debug print outputs

    """
    return import_xes_rs(path, date_format, print_debug)
