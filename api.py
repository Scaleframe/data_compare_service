from fastapi import FastAPI, Response, status
from pydantic import BaseModel
from sqlalchemy import create_engine, select, schema
from sqlalchemy.engine import reflection

# routes for web service


class DiffInput(BaseModel):

    conn_1: str
    conn_2: str
    table_1: str
    table_2: str
    row_comparison: str = "fast"


app = FastAPI()


async def connect_database(connection_string):
    try:
        engine = create_engine(connection_string)
    except Exception:
        return None
    return engine

async def table_name_exists(engine, table_name):

    try:
        inspector = reflection.Inspector.from_engine(engine)
    except Exception:
        return False

    return table_name in inspector.get_table_names()

async def get_table_columns(engine, table_name):

    metadata = schema.MetaData()
    metadata.reflect(bind=engine)

    table = metadata.tables[table_name]  # metadata.tables = {"table_name": "reflected_table_object"}

    return table, table.columns

async def get_table_rows(engine, table, columns=None):
    if columns is None:
        columns = []

    statement = select(columns)

    connection = engine.connect()

    rows = connection.execute(statement).fetchall()

    return rows

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
            column_out["common_columns_same_type"].append(col)
        else:
            column_out["common_columns_different_type"].append(col)

    return column_out


# # get table diff
@app.post("/api/getTableDiff/")
async def get_table_diff(
                payload: DiffInput, 
                response: Response,
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

    return columns_data
    








        


       
