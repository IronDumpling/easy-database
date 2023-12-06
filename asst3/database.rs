/*
 * database.rs
 *
 * Implementation of EasyDB database internals
 *
 * University of Toronto
 * 2019
 */

/*************************************** Import and Global Vars *************************************/
// Packet
use packet::{Value, Command, Request, Response};
// Schema
use schema::Table;
use schema::Column;
// Standard
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Mutex, MutexGuard};
use std::convert::TryInto;

/* OP codes for the query command */
pub const OP_AL: i32 = 1;
pub const OP_EQ: i32 = 2;
pub const OP_NE: i32 = 3;
pub const OP_LT: i32 = 4;
pub const OP_GT: i32 = 5;
pub const OP_LE: i32 = 6;
pub const OP_GE: i32 = 7;

/*************************************** Data Structures *************************************/
// Level 1. Each Row of the Table
struct TableRow {
    version: i64,
    data: Vec<Value>,
}

// Level 2. Each Table inside Database
pub struct DatabaseTable {
    schema: Table,
    rows: Mutex<HashMap<i64, TableRow>>,
}

// Level 3. Database
pub struct Database {
    tables: Vec<DatabaseTable>,
    next: Mutex<i64>,
    foreign: Mutex<HashMap<i64, HashSet<(i32, i64)>>>,
}

// Database Method
impl Database {
    pub fn new(schemas: Vec<Table>) -> Database {
        let mut db = Database {
            tables: vec![],
            next: Mutex::new(1),
            foreign: Mutex::new(HashMap::new())
        };

        for schema in schemas {
            db.tables.push(DatabaseTable {
                schema,
                rows: Mutex::new(HashMap::new())
            });
        }

        db
    }
}

/*************************************** Helper Functions *************************************/
// Validity Checkers
// Helper 1. Bad_Table Checker
fn valid_table_id (tables: &Vec<DatabaseTable>, table_id: i32) -> bool {
    return table_id >= 1 &&
        table_id <= tables.len().try_into().unwrap();
}

// Helper 2. Bad_Query Checker
fn valid_column_id (table: &DatabaseTable, col_id: i32) -> bool {
    return col_id >= 1 &&
        col_id <= table.schema.t_cols.len().try_into().unwrap();
}

// Helper 3. Bad_Row Checker
fn valid_row (values: &Vec<Value>, table_cols: &Vec<Column>) -> bool {
    return values.len() == table_cols.len();
}

// Helper 4. Bad_Value & Bad_Foreign Checker
fn valid_value (values: &Vec<Value>, table_cols: &Vec<Column>, db: &Database)
                -> Result<bool, i32> {
    // // First check if the number of elements in one row agrees with the number of columns
    // if !valid_row(values, table_cols) {
    //     return Err(Response::BAD_ROW);
    // }

    // For each column, check its value
    for index in 0..values.len() {
        // BAD_VALUE Checker
        if !valid_type(&values[index], table_cols, index) {
            return Err(Response::BAD_VALUE);
        }

        // BAD_FOREIGN Checker
        match values[index] {
            Value::Foreign(refer_row_id) => {
                if refer_row_id != 0 {
                    // Get referenced table id
                    let refer_table_id = (table_cols[index].c_ref - 1) as usize;

                    // Get referenced table and rows
                    let refer_table = db.tables.get(refer_table_id).unwrap();
                    let refer_rows = refer_table.rows.lock().unwrap();

                    // Check if the referenced row exists
                    if !refer_rows.contains_key(&refer_row_id) {
                        return Err(Response::BAD_FOREIGN);
                    }
                }
            },
            _ => continue,
        }
    }

    Ok(true)
}

// Helper 5. BAD_QUERY Checker
fn valid_query (value: &Value, table: &DatabaseTable, index: usize)
                -> Result<bool, i32> {
    if !valid_column_id(table, (index+1) as i32) {
        return Err(Response::BAD_QUERY);
    }

    let table_cols = &table.schema.t_cols;

    if !valid_type(value, table_cols, index) {
        return Err(Response::BAD_QUERY);
    }

    Ok(true)
}

// Helper 6. Valid Type Checker
fn valid_type (value: &Value, table_cols: &Vec<Column>, index: usize)
               -> bool {
    match value {
        // Type 1. NULL
        Value::Null => {
            if table_cols[index].c_type != Value::NULL {
                return false;
            }
        },
        // Type 2. Integer
        Value::Integer(_) => {
            if table_cols[index].c_type != Value::INTEGER {
                return false;
            }
        },
        // Type 3. Float
        Value::Float(_) => {
            if table_cols[index].c_type != Value::FLOAT {
                return false;
            }
        },
        // Type 4. Text
        Value::Text(_) => {
            if table_cols[index].c_type != Value::STRING {
                return false;
            }
        },
        // Type 5. Foreign
        Value::Foreign(_) => {
            if table_cols[index].c_type != Value::FOREIGN {
                return false;
            }
        },
    };

    return true;
}

// Accessors
// Helper 7. Get referenced rows
fn get_refer_rows (values: &Vec<Value>) -> HashSet<i64> {
    // list all row_ids referenced by this row
    let mut refer_rows: HashSet<i64> = HashSet::new();

    // Iterate though every value
    for index in 0..values.len() {
        // Add values of Foreign type to the result
        if let Value::Foreign(refer_row_id) = &values[index] {
            // If the row is referencing other rows
            if *refer_row_id != 0 {
                refer_rows.insert(*refer_row_id);
            }
        }
    }

    refer_rows
}

// Helper 8. Add to foreign referenced map
fn add_foreign_map (foreign_map: &mut HashMap<i64, HashSet<(i32, i64)>>,
                    refer_rows: &HashSet<i64>, refer_row_id: i64, refer_table_id: i32) {
    for foreign_id in refer_rows {
        if !foreign_map.contains_key(&foreign_id) {
            // Create a new list for referenced row that consists of new added row
            foreign_map.insert(*foreign_id, HashSet::new());
        }

        foreign_map.get_mut(foreign_id).unwrap().insert((refer_table_id, refer_row_id));
    }
}

// Helper 9. Remove from foreign referenced map
fn remove_foreign_map (foreign_map: &mut HashMap<i64, HashSet<(i32, i64)>>,
                       refer_rows: &HashSet<i64>, refer_row_id: i64, refer_table_id: i32) {
    for foreign_id in refer_rows {
        foreign_map.get_mut(foreign_id).unwrap().remove(&(refer_table_id, refer_row_id));
    }
}

// Helper 10. Recursively drop referenced rows (helper function of handle_drop())
fn cacading_drop(db: &Database, table_id: i32, object_id: i64, foreign_ref_map: & mut MutexGuard<HashMap<i64, HashSet<(i32, i64)>>>)
                 -> Result<Response, i32> {
    // First check the validity of the table id
    if !valid_table_id(&db.tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    // Remove the qualified_rows rows and report error if not found
    let dropped_row = db.tables.get((table_id - 1) as usize).unwrap().rows.lock().unwrap().remove(&object_id);
    if dropped_row.is_none() {
        return Err(Response::NOT_FOUND);
    }

    // Track those dropped rows which are related to foreign refreferences
    let ref_rows = match foreign_ref_map.remove(&object_id) {
        Some(rows) => rows,
        None => return Ok(Response::Drop)
    };

    // Recursively delete these foreign rows
    // ref_tuple consists of ref_table_id & ref_row_id
    for ref_tuple in ref_rows {
        let ret = cacading_drop(db, ref_tuple.0, ref_tuple.1, foreign_ref_map);
    }

    Ok(Response::Drop)
}

/*************************************** Handler Functions *************************************/
/*
 * Function 0. Handler
 */
/* Receive the request packet from client and send a response back */
pub fn handle_request(request: Request, db: &Database) -> Response
{
    /* Handle a valid request */
    let result = match request.command {
        Command::Insert(values) =>
            handle_insert(db, request.table_id, values),
        Command::Update(id, version, values) =>
            handle_update(db, request.table_id, id, version, values),
        Command::Drop(id) => handle_drop(db, request.table_id, id),
        Command::Get(id) => handle_get(db, request.table_id, id),
        Command::Query(column_id, operator, value) =>
            handle_query(db, request.table_id, column_id, operator, value),
        /* should never get here */
        Command::Exit => Err(Response::UNIMPLEMENTED),
    };

    /* Send back a response */
    match result {
        Ok(response) => response,
        Err(code) => Response::Error(code),
    }
}

/*
 * Function 1. Insert
 */
fn handle_insert(db: &Database, table_id: i32, values: Vec<Value>)
                 -> Result<Response, i32>
{
    // Error Handling 1: Bad_Table
    if !valid_table_id(&db.tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    // Get table information
    // index starts from 0, table id starts from 1
    let table = db.tables.get((table_id - 1) as usize).unwrap();
    let table_cols = &table.schema.t_cols;

    // Error Handling 2: Bad_Row
    if !valid_row(&values, table_cols) {
        return Err(Response::BAD_ROW);
    }

    // Error Handling 3: Bad_Value & Bad_Foreign
    if let Err(error) = valid_value(&values, table_cols, &db) {
        return Err(error);
    }

    // Error Handling Ends, Normal Cases Starts
    // Step 1. Determine Key
    let mut row_key = db.next.lock().unwrap();
    let insert_row_key = *row_key;

    //Step 2. Update Row Key
    *row_key += 1;
    drop(row_key);

    // Step 3. Update database's foreign map
    let foreign_rows = get_refer_rows(&values);
    let mut foreign_map = db.foreign.lock().unwrap();

    add_foreign_map(&mut *foreign_map, &foreign_rows,
                    insert_row_key, table_id);
    drop(foreign_map);

    // Step 4. Insert row into table
    let mut table_rows = table.rows.lock().unwrap();
    table_rows.insert(insert_row_key, TableRow {
        version: 1,
        data: values,
    });

    Ok(Response::Insert(insert_row_key, 1))
}

/*
 * Function 2. Update
 */
fn handle_update(db: &Database, table_id: i32, object_id: i64, version: i64, values: Vec<Value>) -> Result<Response, i32>
{
    // Error Handling: check if the table id is valid
    if !valid_table_id(&db.tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    // Retrieve the foreign map
    let mut foreign_ref_map = db.foreign.lock().unwrap();

    // Fetch the target table and table rows & cols
    let target_table = db.tables.get((table_id - 1) as usize).unwrap();
    let mut qualified_rows = target_table.rows.lock().unwrap();
    let target_cols = &target_table.schema.t_cols;

    // Error Handling 2: Bad_Row
    if !valid_row(&values, rarget_cols) {
        return Err(Response::BAD_ROW);
    }

    // Check if the new value is of the correct properties. i.e. types ...
    if let Err(msg) = valid_value(&values, target_cols, &db) {
        return Err(msg);
    }

    if !qualified_rows.contains_key(&object_id) {
        return Err(Response::NOT_FOUND);
    }

    let target_row = qualified_rows.get(&object_id).unwrap();
    if version != 0 && target_row.version != version {
        return Err(Response::TXN_ABORT)
    }

    let prev_ref_rows = get_refer_rows(&target_row.data);
    let new_ref_rows = get_refer_rows(&values);

    remove_foreign_map(&mut *foreign_ref_map, &prev_ref_rows, object_id, table_id);
    add_foreign_map(&mut *foreign_ref_map, &new_ref_rows, object_id, table_id);

    // Update the row in the database
    if let Some(row) = qualified_rows.get_mut(&object_id) {
        row.data = values;
        row.version = version + 1;
    }

    Ok(Response::Update(version + 1))
}

/*
 * Function 3. Drop
 */
fn handle_drop(db: &Database, table_id: i32, object_id: i64) -> Result<Response, i32>
{
    let mut foreign_ref_map = db.foreign.lock().unwrap();

    return cacading_drop(&db, table_id, object_id, & mut foreign_ref_map);
}

/*
 * Function 4. Get
 */
fn handle_get(db: & Database, table_id: i32, object_id: i64)
              -> Result<Response, i32>
{
    // Error Handling 1. BAD_TABLE
    if !valid_table_id(&db.tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    let table = db.tables.get((table_id - 1) as usize).unwrap();
    let table_rows = table.rows.lock().unwrap();

    // Error Handling 2. NOT_FOUND
    let row = table_rows.get(&object_id);
    let target_row = match row {
        Some(row) => row,
        _ => return Err(Response::NOT_FOUND)
    };

    let mut res = Vec::new();

    for val in &target_row.data {
        res.push(val.clone());
    }

    Ok(Response::Get(target_row.version, res))
}

/*
 * Function 5. Query
 */
fn handle_query(db: & Database, table_id: i32, column_id: i32,
                operator: i32, other: Value)
                -> Result<Response, i32>
{
    // Error Handling 1. BAD_TABLE
    if !valid_table_id(&db.tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    // Get Target Rows
    let table = db.tables.get((table_id - 1) as usize).unwrap();
    let table_rows = table.rows.lock().unwrap();
    // Get Target Cols
    let table_cols = &table.schema.t_cols;

    // Case 1. Operator ALL
    if operator == OP_AL {
        // Error Handling 2.1 BAD_QUERY
        if column_id != 0 {
            return Err(Response::BAD_QUERY);
        } else {
            // this holds all valid ids for the query
            let mut res: Vec<i64> = Vec::new();
            for (id, _row) in table_rows.iter() {
                res.push(*id);
            }
            return Ok(Response::Query(res));
        }
    }

    // Error Handling 2.2 BAD_QUERY
    let column_id = (column_id - 1) as usize;
    if let Err(error) = valid_query(&other, &table, column_id) {
        return Err(error);
    }

    let mut result: Vec<i64> = Vec::new();

    // Case2. Other Operators
    match operator {
        OP_EQ =>
            for (id, row) in table_rows.iter() {
                if column_id == 0 {
                    // it is possible to scan a table for id of a row, using column_id 0
                    if let Value::Integer(value) = other{
                        if *id == value {
                            result.push(*id);
                        }
                    }
                }
                if row.data[column_id] == other {
                    result.push(*id)
                };
            },
        OP_NE =>
            for (id, row) in table_rows.iter() {
                if column_id == 0 {
                    // it is possible to scan a table for id of a row, using column_id 0
                    if let Value::Integer(value) = other{
                        if *id == value {
                            result.push(*id);
                        }
                    }
                }
                if row.data[column_id] != other {
                    result.push(*id)
                };
            },
        OP_LT => // column id and foreign fields only supported EQ and NE operators.
        // return error for all other operator types
            if table_cols[column_id].c_type == Value::FOREIGN || column_id == 0 {
                return Err(Response::BAD_QUERY);
            } else {
                for (id, row) in table_rows.iter() {
                    if row.data[column_id] < other {
                        result.push(*id);
                    }
                }
            },
        OP_GT => // column id and foreign fields only supported EQ and NE operators.
        // return error for all other operator types
            if table_cols[column_id].c_type == Value::FOREIGN || column_id == 0 {
                return Err(Response::BAD_QUERY);
            } else {
                for (id, row) in table_rows.iter() {
                    if row.data[column_id] > other {
                        result.push(*id);
                    }
                }
            },
        OP_LE => // column id and foreign fields only supported EQ and NE operators.
        // return error for all other operator types
            if table_cols[column_id].c_type == Value::FOREIGN || column_id == 0 {
                return Err(Response::BAD_QUERY);
            } else {
                for (id, row) in table_rows.iter() {
                    if row.data[column_id] <= other {
                        result.push(*id);
                    }
                }
            },
        OP_GE => // column id and foreign fields only supported EQ and NE operators.
        // return error for all other operator types
            if table_cols[column_id].c_type == Value::FOREIGN || column_id == 0 {
                return Err(Response::BAD_QUERY);
            } else {
                for (id, row) in table_rows.iter() {
                    if row.data[column_id] >= other {
                        result.push(*id);
                    }
                }
            },
        // checks if operator number is valid
        _ => return Err(Response::BAD_QUERY)
    };

    Ok(Response::Query(result))

