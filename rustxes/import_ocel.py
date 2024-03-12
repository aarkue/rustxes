from .rustxes import import_ocel_xml_rs, import_ocel_json_rs
import polars


def rs_ocel_to_pm4py(ocel_rs: dict[str, polars.DataFrame]):
    """
     Convert a dictionary of polars.DataFrame returned from rust OCEL import to PM4PY representation

     Note, that you might instead use one of the automatically converting import functions ending with `_pm4py` (e.g., `import_ocel_xml_pm4py`)

     * `path` - The filepath of the .xml file to import

    """
    import pm4py
    ocel_pm4py = pm4py.ocel.OCEL(
        events=ocel_rs["events"].to_pandas().convert_dtypes(),
        objects=ocel_rs["objects"].to_pandas().convert_dtypes(),
        relations=ocel_rs["relations"].to_pandas().convert_dtypes(),
        object_changes=ocel_rs["object_changes"].to_pandas().convert_dtypes(),
        globals={},
        o2o=ocel_rs["o2o"].to_pandas().convert_dtypes(),
        parameters={
            pm4py.objects.ocel.obj.Parameters.EVENT_ID: "ocel:eid",
            pm4py.objects.ocel.obj.Parameters.EVENT_ACTIVITY: "ocel:activity",
            pm4py.objects.ocel.obj.Parameters.EVENT_TIMESTAMP: "ocel:timestamp",
            pm4py.objects.ocel.obj.Parameters.OBJECT_ID: "ocel:oid",
            pm4py.objects.ocel.obj.Parameters.OBJECT_TYPE: "ocel:type",
            pm4py.objects.ocel.obj.Parameters.QUALIFIER: "ocel:qualifier",
            pm4py.objects.ocel.obj.Parameters.CHANGED_FIELD: "ocel:field",
        },
    )

    return ocel_pm4py


def import_ocel_xml(path: str) -> dict[str, polars.DataFrame]:
    """
     Import an OCEL2 XML event log

     Returns an dict with the polars DataFrames for the following keys: 'objects', 'events', 'o2o', 'object_changes', 'relations'

     * `path` - The filepath of the .xml file to import

    """
    return import_ocel_xml_rs(path)


def import_ocel_xml_pm4py(path: str):
    """
     Import an OCEL2 XML event log and convert it to PM4PY version

     * `path` - The filepath of the .xml file to import

    """
    ocel_rs = import_ocel_xml(path)
    return rs_ocel_to_pm4py(ocel_rs)




def import_ocel_json(path: str) -> dict[str, polars.DataFrame]:
    """
     Import an OCEL2 JSON event log

     Returns an dict with the polars DataFrames for the following keys: 'objects', 'events', 'o2o', 'object_changes', 'relations'

     * `path` - The filepath of the .json file to import

    """
    return import_ocel_json_rs(path)


def import_ocel_json_pm4py(path: str):
    """
     Import an OCEL2 JSON event log and convert it to PM4PY version

     * `path` - The filepath of the .json file to import

    """
    ocel_rs = import_ocel_json(path)
    return rs_ocel_to_pm4py(ocel_rs)
