from .rustxes import import_xes_rs


def import_xes(path: str):
    return import_xes_rs(path)
