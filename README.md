####
Table Diff Service

This service takes in two tables and returns a diff between them. 


## Gettnig Started

To run this locally, startup your favorite virtual environemnt and install the dependencies from the `requirements.txt` file. 

`pip install -r requirements.txt`

Next you'll need an API token to run the service. 

Once you have that, create a file named `.env` in this project's root folder, and paste this in the file:

`API_TOKEN=PASTE_TOKEN_VALUE_HERE`

From there you should be good to get things running. 


## Endpoints

### POST `/api/getTableDiff/`

This endpoint takes in two connection strings and table paths, and return a diff between them.

#### Payload

```
{
    "conn_1": "db_connection_string_1",
    "table_1": "table from conn_1 to compare",
    "conn_2": "db_connection_string_2",
    "table_2": "table from conn_2 to compare"
}
```

##### Payload example:

```
{
    "conn_1": "postgresql://username:password@hostname:port/dbname", 
    "conn_2": "postgresql://username2:password2@hostname2:port/dbname", 
    "table_1": "table_name_1", 
    "table_2": "table_name_2"
}
```

#### Response

##### 400

For invalid connection strings or if the table does not exist.

###### Format:
`{"error": "description"}`

###### Example:
`{"error": "Table name table_1_name does not exist on conn_1"}`


##### 200
Success

###### Format:
```json
{
    "columns_data": {
        "table_1_uncommon_columns": [
            "uncommon_column_1_name",
            "uncommon_column_2_name"
        ],
        "table_2_uncommon_columns": [],
        "common_columns": [
            "common_column_1_name",
            "common_column_2_name"
        ],         
        "common_columns_different_type": [
            {
                "common_column_2_name": {
                    "table_1": "DOUBLE PRECISION",
                    "table_2": "INTEGER"
                }
            }
        ]
    },
    "rows_data": {
        "table_1": {
            "row_count": 3789615,
            "metrics": {
                "mean": {
                    "column_1_name": 6.71,
                    "column_2_name": 10.98
                },
                "stddev": {
                    "column_1_name": 6.71,
                    "column_2_name": 56.66
                }
            }
        },
        "table_2": {
            "row_count": 3773153,
            "metrics": {
                "mean": {
                    "column_1_name": 5.91,
                    "column_2_name": 9.56
                },
                "stddev": {
                    "column_1_name": 5.71,
                    "column_2_name": 45.66
                }
            }
        }
    },
    "metrics_diff": {
        "avg": {
            "column_1_name": "1.19225%"   
        },
        "stddev": {
            "column_1_name": "0.91776%"
        }
    }
}
```
###### Example:
```json
{
    "columns_data": {
        "table_1_uncommon_columns": [
            "owner_name",
            "land_use_code"
        ],
        "table_2_uncommon_columns": [],
        "common_columns": [
            "nr_buildings",
            "parcel_frontage"
        ],
        "common_columns_different_type": [
            {
                "gross_floor_area": {
                    "table_1": "DOUBLE PRECISION",
                    "table_2": "INTEGER"
                }
            }
        ]
    },
    "rows_data": {
        "table_1": {
            "row_count": 3789615,
            "metrics": {
                "mean": {
                    "nr_parking_units": 6.71
                },
                "stddev": {
                    "nr_parking_units": 3.12
                }
            }
        },
        "table_2": {
            "row_count": 3773153,
            "metrics": {
                "mean": {
                    "nr_parking_units": 5.71
                },
                "stddev": {
                    "nr_parking_units": 4.12
                }
            }
        }
    },
    "metrics_diff": {
        "avg": {
            "nr_parking_units": "1.19225%"  
        },
        "stddev": {
            "nr_parking_units": "0.91776%"
        }
    }
}
```

### POST `/api/getAvailableColumns/`

This endpoint takes a table name (optionally two) and returns available column names with their types, along with numeric columns.

If `conn_2` is not provided but `table_2` is given, the value `conn_2` is set to `conn_1` automatically.

If `table_2` is not provided but `conn_2` is provided, then the value of `table_2` is taken as `table_1`.

#### Payload

{
    "conn_1": "db_connection_string_1",
    "table_1": "table from conn_1 to compare",
    "conn_2": "db_connection_string_2", (Optional)
    "table_2": "table from conn_2 to compare" (Optional)
}

##### Payload example:

{
    "conn_1": "postgresql://username:password@hostname:port/dbname", "conn_2": "postgresql://username2:password2@hostname2:port/dbname", 
    "table_1": "table_name_1", 
    "table_2": "table_name_2"
}

#### Response

##### 400

For invalid connection strings or if the table does not exist.

###### Format:
{"error": "description"}

###### Example:
{"error": "Table name table_1_name does not exist on conn_1"}


##### 200
Success

###### Format:

For two tables:

```json
{
    "table_1": {
        "columns_type_map": {
            "column_1_name": "double precision",
            "column_2_name": "double precision",
            "column_3_name": "text"
        },
        "numeric_columns": [
            "column_1_name",
            "columns_2_name"
        ]
    },
    "table_2": {
        "columns_type_map": {
            "column_1_name": "double precision",
            "column_2_name": "double precision",
            "column_3_name": "text"
        },
        "numeric_columns": [
            "column_1_name",
            "columns_2_name"
        ]
    }
}
```

For one table the output object contains only one key -- `table`:

```json
{
    "table": {
        "columns_type_map": {
            "column_1_name": "double precision",
            "column_2_name": "double precision",
            "column_3_name": "text"
        },
        "numeric_columns": [
            "column_1_name",
            "columns_2_name"
        ]
    }
}
```

###### Example:

For two tables:

```json
{
    "table_1": {
        "columns_type_map": {
            "latitude": "double precision",
            "nr_floors": "double precision",
            "county_parcel_id": "text"
        },
        "numeric_columns": [
            "nr_floors",
            "latitude"
        ]
    },
    "table_2": {
        "columns_type_map": {
            "latitude": "double precision",
            "nr_floors": "double precision",
            "county_parcel_id": "text"
        },
        "numeric_columns": [
            "nr_floors",
            "latitude"
        ]
    }
}
```

For one table the output object contains only one key -- `table`:

```json
{
    "table": {
        "columns_type_map": {
            "latitude": "double precision",
            "nr_floors": "double precision",
            "county_parcel_id": "text"
        },
        "numeric_columns": [
            "nr_floors",
            "latitude"
        ]
    }
}
```


### GET `/api/getAvailableMetrics/`

This endpoint returns the available aggregation metrics. 

#### Response

##### 200

###### Format:

["aggregation_metric_1", "aggregation_metric_2"]

###### Example:

["mean", "stddev"]


