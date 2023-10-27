from typing import Optional
from .rustxes import import_xes_rs


def import_xes(path: str, date_format: Optional[str] = None):
    return import_xes_rs(path, date_format)
