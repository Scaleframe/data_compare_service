
import json
import os

from typing import Union, Dict, List, Tuple, Optional, Mapping, NamedTuple, Set

from fastapi import FastAPI, Response, status, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, select, schema, func, within_group
from sqlalchemy.engine import reflection
from dotenv import load_dotenv

load_dotenv()

try:
    API_TOKEN = os.environ["API_TOKEN"]
except KeyError:
    raise Exception("No API_TOKEN provided as environment variable") from None


class DiffInput(BaseModel):

    conn_1: str
    conn_2: str
    table_1: str
    table_2: str



app = FastAPI()


async def connect_database(connection_string: str) -> "sqlalchemy.Engine":
    try:
        engine = create_engine(connection_string, connect_args={"timeout": 60})
    except Exception:
        return None
    return engine

async def table_name_exists(engine: "Engine", table_name: str) -> bool:

    try:
        inspector = inspect(engine)
    except Exception:
        return False

    return table_name in inspector.get_table_names()

async def get_table_columns(
    engine: "sqlalchemy.Engine", table_name: str
) -> Tuple[Union["sqlalchemy.Table", List["sqlalchemy.Column"]]]:

    metadata = schema.MetaData()
    metadata.reflect(bind=engine)

    table = metadata.tables[table_name]  # metadata.tables = {"table_name": "reflected_table_object"}

    return table, table.columns

async def get_table_metrics(engine: "sqlalchemy.Engine", mean_stddev_pairs: List[Tuple], table_name: str) -> Dict[str, Dict[str, float]]:
    # 
    select_query = [
        element
        for pair in mean_stddev_pairs
        for element in pair
    ]

    statement = select(select_query) 

    connection = engine.connect()

    try:
        agg_result = dict(next(connection.execute(statement)))
    except Exception:
        agg_result = {}

    agg_result = {
        key: round(float(value), 2) for key, value in agg_result.items()
    }

    quartile_map = {}

    for key, value in agg_result.items():
        if key.endswith("quartile25") or key.endswith("quartile75"):
            column_name, sep, quartile = key.rpartition("_")
            # ("nr_floors", "_", "quartile25")

            quartile_map.setdefault(column_name, {}).update({quartile: value})

    
    for column_name, quartile_data in quartile_map.items():
        agg_result[f"{column_name}_IQR"] = quartile_data["quartile75"] - quartile_data["quartile25"]

    # agg_result_copy = agg_result.copy()

    try:
        row_count = next(connection.execute(f"select count(*) from {table_name}"))[0]
    except Exception:
        row_count = 0

    formatted_metrics = {"row_count": row_count}

    for column_agg, value in agg_result.items():
        column_name, _, agg = column_agg.rpartition("_") # "column_1_mean" -> ("column_1", "_", "mean")
        if agg in formatted_metrics:
            formatted_metrics[agg][column_name] = value
        else:
            formatted_metrics[agg] = {}
            formatted_metrics[agg][column_name] = value

    return {"metrics": formatted_metrics, "original_metrics": agg_result}


async def get_all_columns_info(
    table_columns: Mapping[str, "sqlalchemy.Column"]
) -> Dict[str, Union[List[str], Dict[str, "ColumnType"]]]:

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


async def get_columns_data(table_1_cols: Mapping[str, "sqlalchemy.Column"], table_2_cols: Mapping[str, "sqlalchemy.Column"]) -> Dict[str, List[str]]:

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
async def get_available_metrics(
    x_api_token: str = Header(""),
):

    if x_api_token != API_TOKEN:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"error": "Authentication failure"}
        )
    
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=["mean", "stddev", "quartiles", "row_count"]
    )


class AvailableColumnsInput(BaseModel):

    conn_1: str
    table_1: str
    conn_2: str = ""
    table_2: str = ""

async def _get_engines_conns_tables(
    conn_1: str, conn_2: str, table_1: str, table_2: str
) -> Tuple[str, bool]:
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

    return (engine_2, conn_1, conn_2, table_1, table_2)

@app.post("/api/getAvailableColumns")
async def get_available_columns(
    payload: AvailableColumnsInput,
    response: Response,
    x_api_token: str = Header(""), # X-API-TOKEN
):

    if x_api_token != API_TOKEN:
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"error": "Authentication failure"}

    conn_1 = payload.conn_1
    conn_2 = payload.conn_2
    table_1 = payload.table_1
    table_2 = payload.table_2

    engine_2, conn_1, conn_2, table_1, table_2 = await _get_engines_conns_tables(
        conn_1, conn_2, table_1, table_2
    )

    engine_1 = await connect_database(conn_1)
    if not engine_1:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Could not connect to DB with the provided connection string."}

    if not await table_name_exists(engine_1, table_1):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Table name {table_1} does not exist."}
    
    if engine_2:
        engine_2 = await connect_database(conn_2)
        if not engine_2:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"error": f"Could not connect to DB with the provided connection string"}
  
        if not await table_name_exists(engine_2, table_2):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"error": f"Table name {table_2} does not exist."}

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


class ColumnTypes(NamedTuple):

    text_columns: Set[str]
    numeric_columns: Set[str]

async def _get_numeric_text_cols(common_columns_same_type: List[Mapping[str, "ColumnType"]]) -> ColumnTypes:
    numeric_columns = set()
    text_columns = set()

    # check for alphanumeric columns, we only want to analyze those.
    for column_data in common_columns_same_type:
        for column_name, column_type in column_data.items():
            column_type = column_type.casefold()
            if "float" in column_type or "integer" in column_type or (
                "double precision" in column_type and "[]" not in column_type
            ):
                numeric_columns.add(column_name)
            elif "char" in column_type or "text" in column_type:
                text_columns.add(column_name)

    return ColumnTypes(text_columns=text_columns, numeric_columns=numeric_columns)


async def _get_table_metrics_wrapper(
    engine: "Engine", table_obj: "sqlalchemy.Table", numeric_columns: Set[str], table_name: str
) -> Dict[str, Dict[str, float]]:
    cols_table = [
        (
            func.avg(getattr(table_obj.columns, column)).label(f"{column}_mean"),
            func.stddev(getattr(table_obj.columns, column)).label(f"{column}_stddev"),
            func.percentile_cont(.25).within_group(getattr(table_obj.columns, column)).label(f"{column}_quartile25"),
            func.percentile_cont(.5).within_group(getattr(table_obj.columns, column)).label(f"{column}_quartile50"),         
            func.percentile_cont(.75).within_group(getattr(table_obj.columns, column)).label(f"{column}_quartile75"),
            func.percentile_cont(1).within_group(getattr(table_obj.columns, column)).label(f"{column}_quartile100"),
        )
        for column in numeric_columns
    ]

    table_metrics = await get_table_metrics(
        engine, cols_table, table_name
    )

    return table_metrics


async def _get_all_metrics_diff(
    table_1_agg_metrics: Dict[str, float],
    table_2_agg_metrics: Dict[str, float]
) -> Dict[str, Dict[str, Dict[str, Union[str, float]]]]:

    metrics_diff = {
        "mean_diff": {},
        "stddev_diff": {},
        "quartiles_diff": {
            "25": {},
            "50": {},
            "75": {},
            "100": {},
            "IQR": {},
        },
    }

    for key, table_1_value in table_1_agg_metrics.items():
        
        table_2_value = table_2_agg_metrics[key]

        try:
            diff_value_percent = round(
                ((table_1_value - table_2_value) * 100) / table_1_value,
                5
            )
        except ZeroDivisionError:
            diff_value_percent = "N/A"

        if diff_value_percent != "N/A":
            diff_value_percent_str = f"{diff_value_percent}%"
        else:
            diff_value_percent_str = diff_value_percent

        diff_value_raw = table_1_value - table_2_value
        diff_value_raw_str = str(round(diff_value_raw, 5))

        diff_value = {
            "raw": diff_value_raw_str,
            "percent": diff_value_percent_str,
        }

        column_name, sep, agg_operation=on = key.rpartition("_")  
    
        if agg_operation == "mean":
            metrics_diff["mean_diff"][column_name] = diff_value
        elif agg_operation == "stddev":
            metrics_diff["stddev_diff"][column_name] = diff_value
        elif agg_operation == "quartile25":
            metrics_diff["quartiles_diff"]["25"][column_name] = diff_value
        elif agg_operation == "quartile50":
            metrics_diff["quartiles_diff"]["50"][column_name] = diff_value
        elif agg_operation == "quartile75":
            metrics_diff["quartiles_diff"]["75"][column_name] = diff_value
        elif agg_operation == "quartile100":
            metrics_diff["quartiles_diff"]["100"][column_name] = diff_value
        elif agg_operation == "IQR":
            metrics_diff["quartiles_diff"]["IQR"][column_name] = diff_value

    return metrics_diff

# # get table diff
@app.post("/api/getTableDiff/")
async def get_table_diff(
                payload: DiffInput, 
                response: Response,
                x_api_token: str = Header("")
):
    
    if x_api_token != API_TOKEN:
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"error": "Authentication failure"}

    conn_1 = payload.conn_1
    conn_2 = payload.conn_2
    table_1 = payload.table_1
    table_2 = payload.table_2

    engine_1 = await connect_database(conn_1)
    if not engine_1:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Could not connect to DB with the provided connection string."}

    engine_2 = await connect_database(conn_2)
    if not engine_2:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Could not connect to DB with the provided connection string."}

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

    column_types = await _get_numeric_text_cols(common_columns_same_type)
    numeric_columns = column_types.numeric_columns
    text_columns = column_types.text_columns

    columns_data.pop("common_columns_same_type", None)    

    final_output = dict(
        columns_data=columns_data,
        rows_data={},
    )
    
    table_1_metrics = await _get_table_metrics_wrapper(
        engine=engine_1,
        table_obj=table_1_obj,
        numeric_columns=numeric_columns,
        table_name=table_1
    )
    final_output["rows_data"]["table_1"] = table_1_metrics

    table_2_metrics = await _get_table_metrics_wrapper(
        engine=engine_2,
        table_obj=table_2_obj,
        numeric_columns=numeric_columns,
        table_name=table_2
    )
    final_output["rows_data"]["table_2"] = table_2_metrics

    table_1_agg_metrics = table_1_metrics.pop("original_metrics", {})
    table_2_agg_metrics = table_2_metrics.pop("original_metrics", {})

    final_output["metrics_diff"] = await _get_all_metrics_diff(
        table_1_agg_metrics,
        table_2_agg_metrics,
    )

    final_output["metrics_diff"]["row_count_diff"] = (
        table_1_metrics["metrics"]["row_count"] -
        table_2_metrics["metrics"]["row_count"]
    )

    return final_output
        