import rustxes
import pm4py
import sys
IGNORE_OBJECT_CHANGE_DATES = True


def ocel_to_df_dict(ocel: pm4py.OCEL):
    return {
        "objects": ocel.objects,
        "events": ocel.events,
        "relations": ocel.relations,
        "o2o": ocel.o2o,
        "object_changes": ocel.object_changes.reset_index().sort_values(["ocel:timestamp", "index"]).drop(["index", "ocel:timestamp"], axis=1) if IGNORE_OBJECT_CHANGE_DATES else ocel.object_changes,
    }


def sort_df_columns(df):
    return df.reindex(sorted(df.columns), axis=1)


if __name__ == "__main__":
    path = sys.argv[1]
    ocel_rs = rustxes.import_ocel_xml_pm4py(path) if path.endswith(
        ".xml") else rustxes.import_ocel_json_pm4py(path)
    ocel_py = pm4py.read_ocel2_xml(path) if path.endswith(
        ".xml") else pm4py.read_ocel2_json(path)

    obj_ids_rs = set(ocel_rs.objects["ocel:oid"].unique())
    obj_ids_py = set(ocel_py.objects["ocel:oid"].unique())
    diff = obj_ids_rs - obj_ids_py
    if len(diff) == 0:
        print("Object IDs match ✅")
    else:
        print("Object id difference: " + str(diff))
        if obj_ids_rs.issuperset(obj_ids_rs):
            print("Rust object IDs are superset!")


    if IGNORE_OBJECT_CHANGE_DATES:
        print("Ignoring change datetime in object_change df!")
        
    dict_rs = ocel_to_df_dict(ocel_rs)
    dict_py = ocel_to_df_dict(ocel_py)
    for key in dict_rs:
        df_rs = sort_df_columns(
            dict_rs[key].convert_dtypes().reset_index(drop=True))
        df_py = sort_df_columns(
            dict_py[key].convert_dtypes().reset_index(drop=True))
        if df_rs.shape != df_py.shape:
            print(f"[{key}] Shapes do not match: {df_rs.shape} != {df_py.shape}")
        else:
            print(f"[{key}] Shapes match ({df_rs.shape}) ✅")
        if df_rs.equals(df_py):
            print(f"[{key}] DFs are equal ✅")
        else:
            print(f"[{key}] DFs are not equal:")
            print(df_rs)
            print(df_py)
