
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

    return {"row_count": row_count, "metrics": mean_stddev_result}



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

    table_1_mean_stddev_metrics = table_1_metrics["metrics"]
    table_2_mean_stddev_metrics = table_2_metrics["metrics"]

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


    return final_output
        