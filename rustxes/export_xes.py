from typing import Optional
from .rustxes import export_xes_rs
import polars


def export_xes(df: polars.DataFrame, path: str):
    """
     Export an XES event log

     * `df` - The Polars DataFrame representation of the event log to export
     * `path` - The filepath where the .xes or .xes.gz file should be saved 

    """
    return export_xes_rs(df,path)
