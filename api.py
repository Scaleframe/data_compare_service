
import json
from fastapi import FastAPI, Response, status
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, select, schema, func
from sqlalchemy.engine import reflection

from typing import Union, Dict, List

class DiffInput(BaseModel):

    conn_1: str
    conn_2: str
    table_1: str
    table_2: str



app = FastAPI()


async def connect_database(connection_string):
    try:
        engine = create_engine(connection_string)
    except Exception:
        return None
    return engine

async def table_name_exists(engine, table_name):

    try:
        inspector = inspect(engine)
    except Exception:
        return False

    return table_name in inspector.get_table_names()

async def get_table_columns(engine, table_name):

    metadata = schema.MetaData()
    metadata.reflect(bind=engine)

    table = metadata.tables[table_name]  # metadata.tables = {"table_name": "reflected_table_object"}

    return table, table.columns

async def get_table_metric(engine, mean_stddev_pairs, table_name):
    # 
    select_query = [
        element
        for pair in mean_stddev_pairs
        for element in pair
    ]

    statement = select(select_query) 

    connection = engine.connect()
    
    
    try:
        mean_stddev_result = dict(next(connection.execute(statement)))
    except Exception:
        mean_stddev_result = {}

    mean_stddev_result = {
        key: round(float(value), 2) for key, value in mean_stddev_result.items()
    }

    try:
        row_count = next(connection.execute(f"select count(*) from {table_name}"))[0]
    except Exception:
        row_count = 0

    formatted_metrics = {"row_count": row_count}

    for column_agg, value in mean_stddev_result.items():
        column_name, _, agg = column_agg.rpartition("_") # "column_1_mean" -> ("column_1", "_", "mean")
        if agg in formatted_metrics:
            formatted_metrics[agg][column_name] = value
        else:
            formatted_metrics[agg] = {}
            formatted_metrics[agg][column_name] = value

    return {"metrics": formatted_metrics, "original_metrics": mean_stddev_result}


async def get_all_columns_info(table_columns):

    col_name_type = {}  # {"col_1": "text", "col_2": "float"}
    numeric_cols = set()

    for col_name, meta in table_columns.items():
        col_type = str(meta.type).partition("(")[0]  # "varchar(10)".partition("(") -> ("varchar", "(", "10)")[0] -> "varchar"
        
        col_type = col_type.casefold()
        col_name_type[col_name] = col_type
        if "float" in col_type or "integer" in col_type or "precision" in col_type:
            numeric_cols.add(col_name)

    return {
        "columns_type_map": col_name_type,
        "numeric_columns": list(numeric_cols),
    }


async def get_columns_data(table_1_cols, table_2_cols):

    common_cols = set()

    for col_name, meta in table_1_cols.items():
        try:
            table_2_cols[col_name]
        except Exception:
            pass
        else:
            common_cols.add(col_name)

    table_1_uncommon_cols = set(table_1_cols.keys()) - common_cols
    table_2_uncommon_cols = set(table_2_cols.keys()) - common_cols

    column_out = {
        "table_1_uncommon_columns": list(table_1_uncommon_cols),
        "table_2_uncommon_columns": list(table_2_uncommon_cols),
        "common_columns": list(common_cols),
        "common_columns_same_type": [],
        "common_columns_different_type": [],
    }

    for col in common_cols:
        table_1_col_type = str(table_1_cols[col].type).partition("(")[0]
        table_2_col_type = str(table_2_cols[col].type).partition("(")[0]
        if table_1_col_type == table_2_col_type:
            column_out["common_columns_same_type"].append({col: table_1_col_type})
        else:
            column_out["common_columns_different_type"].append(
                {
                    col: {
                        "table_1": table_1_col_type, "table_2": table_2_col_type
                    }
                }
            )

    return column_out


@app.get("/api/getAvailableMetrics")
async def get_available_metrics():

    return ["mean", "stddev"]


class AvailableColumnsInput(BaseModel):

    conn_1: str
    table_1: str
    conn_2: str = ""
    table_2: str = ""

@app.post("/api/getAvailableColumns")
async def get_available_columns(
    payload: AvailableColumnsInput,
    response: Response,
):
    conn_1 = payload.conn_1
    conn_2 = payload.conn_2
    table_1 = payload.table_1
    table_2 = payload.table_2

    # Whether to process table_2
    engine_2 = True

    if not table_2:
        # If no conn_2 given, table_2 is not processed
        if not conn_2:
            engine_2 = False
        # if con_2 is given, table_2's name should be the same as table_1
        else:
            table_2 = table_1
    else:
        # If con_2 is not given, it should have the same value as conn_1
        if not conn_2:
            conn_2 = conn_1

    # Don't process table_2 if the connection strings are the same and 
    # table names are the same
    if (conn_1 == conn_2) and (table_1 == table_2):
        engine_2 = False

    engine_1 = await connect_database(conn_1)
    if not engine_1:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Could not connect to DB with connection string {conn_1}"}

    if not await table_name_exists(engine_1, table_1):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Table name {table_1} does not exist on {conn_1}"}
    
    if engine_2:
        engine_2 = await connect_database(conn_2)
        if not engine_2:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"error": f"Could not connect to DB with connection string {conn_2}"}
  
        if not await table_name_exists(engine_2, table_2):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"error": f"Table name {table_2} does not exist on {conn_2}"}

    if engine_2:
        output = {
            "table_1": {}, "table_2": {},
        }
    else:
        output = {
            "table": {}
        }

    _, table_1_cols = await get_table_columns(engine_1, table_1)

    table_1_info = await get_all_columns_info(table_1_cols)
    if not engine_2:
        output["table"] = table_1_info
        return output

    output["table_1"] = table_1_info

    _, table_2_cols = await get_table_columns(engine_2, table_2) 
    table_2_info = await get_all_columns_info(table_2_cols)
    output["table_2"] = table_2_info

    return output


# # get table diff
@app.post("/api/getTableDiff/")
async def get_table_diff(
                payload: DiffInput, 
                response: Response
):
    
    conn_1 = payload.conn_1
    conn_2 = payload.conn_2
    table_1 = payload.table_1
    table_2 = payload.table_2

    engine_1 = await connect_database(conn_1)
    if not engine_1:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Could not connect to DB with connection string {conn_1}"}

    engine_2 = await connect_database(conn_2)
    if not engine_2:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Could not connect to DB with connection string {conn_2}"}

    if not await table_name_exists(engine_1, table_1):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Table name {table_1} does not exist"}
    
    if not await table_name_exists(engine_2, table_2):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Table name {table_2} does not exist"}

    table_1_obj, table_1_cols = await get_table_columns(engine_1, table_1)
    table_2_obj, table_2_cols = await get_table_columns(engine_2, table_2)


    columns_data = await get_columns_data(table_1_cols, table_2_cols)

    common_columns_same_type = columns_data["common_columns_same_type"]

    numeric_columns = set()
    text_columns = set()

    # check for alphanumeric columns, as we only want to analyze those.

    for column_data in common_columns_same_type:
        for column_name, column_type in column_data.items():
            column_type = column_type.casefold()
            if "float" in column_type or "integer" in column_type or (
                "double precision" in column_type and "[]" not in column_type
            ):
                numeric_columns.add(column_name)
            elif "char" in column_type or "text" in column_type:
                text_columns.add(column_name)

    columns_data.pop("common_columns_same_type", None)    

    final_output = dict(
        columns_data=columns_data,
        rows_data={},
        metrics_diff={"mean": {}, "stddev": {}}
    )
    
    cols_table_1 = [
        (
            func.avg(getattr(table_1_obj.columns, column)).label(f"{column}_mean"),
            func.stddev(getattr(table_1_obj.columns, column)).label(f"{column}_stddev"),
        )
        for column in numeric_columns
    ]

    table_1_metrics = await get_table_metric(
        engine_1, cols_table_1, table_1
    )

    final_output["rows_data"]["table_1"] = table_1_metrics

    cols_table_2 = [
        (
            func.avg(getattr(table_2_obj.columns, column)).label(f"{column}_mean"),
            func.stddev(getattr(table_2_obj.columns, column)).label(f"{column}_stddev"),
        )
        for column in numeric_columns
    ]

    table_2_metrics = await get_table_metric(
        engine_2, cols_table_2, table_2
    )

    final_output["rows_data"]["table_2"] = table_2_metrics

    table_1_mean_stddev_metrics = table_1_metrics.pop("original_metrics", {})
    table_2_mean_stddev_metrics = table_2_metrics.pop("original_metrics", {})

    for key, table_1_value in table_1_mean_stddev_metrics.items():
        
        table_2_value = table_2_mean_stddev_metrics[key]

        diff_value = round(
            ((table_1_value - table_2_value) * 100) / table_1_value,
            5
        )
        diff_value_str = f"{diff_value}%"


        column_name, sep, agg_operation=on = key.rpartition("_")  
    
        if agg_operation == "mean":
            final_output["metrics_diff"]["mean"][column_name] = diff_value_str
        elif agg_operation == "stddev":
            final_output["metrics_diff"]["stddev"][column_name] = diff_value_str

    final_output["metrics_diff"]["row_count_diff"] = (
        table_1_metrics["metrics"]["row_count"] -
        table_2_metrics["metrics"]["row_count"]
    )

    return final_output
        