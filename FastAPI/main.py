# main.py

import configparser
import pyodbc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from create_db import create_db, create_table

app = FastAPI()

db_name = "Fast_API"
table_name = "todo"

# Select the DB only here
def get_sql_con():
    config = configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\FastAPI\config.config")
    
    driver = config['ssms']['driver']
    server = config['ssms']['server']
    trusted_conn = config['ssms']['trusted_connection']

    conn_str = (
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={db_name};"
        f"Trusted_Connection={trusted_conn};"
    )
    return pyodbc.connect(conn_str)

@app.on_event("startup")
def startup():
    create_db(db_name)
    create_table(table_name, db_name)

# ---- FastAPI Models and Routes (same as before) ----
class Todo(BaseModel):
    title: str
    description: str = None
    completed: bool | None = None

class TodoCompleted(BaseModel):
    completed: bool

@app.post("/todos/")
async def create_todo(todo: Todo):
    con = get_sql_con()
    cursor = con.cursor()
    completed_value = int(todo.completed) if todo.completed is not None else 0
    cursor.execute(
        "INSERT INTO todo (title, description, completed) VALUES (?, ?, ?)",
        (todo.title, todo.description, completed_value)
    )
    con.commit()
    con.close()
    return {"message": "Todo created successfully"}


@app.get("/todos/")
async def get_todos():
    con = get_sql_con()
    cursor = con.cursor()
    cursor.execute("SELECT id, title, description, completed FROM todo")
    rows = cursor.fetchall()
    result = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
    con.close()
    return result

@app.get("/todos/{todo_id}")
async def get_one_todo(todo_id: int):
    con = get_sql_con()
    cursor = con.cursor()
    cursor.execute("SELECT id, title, description, completed FROM todo WHERE id = ?", (todo_id,))
    row = cursor.fetchone()
    con.close()
    if row:
        return dict(zip([column[0] for column in cursor.description], row))
    raise HTTPException(status_code=404, detail="Todo not found")

@app.put("/todos/{todo_id}")
async def update_todo(todo_id: int, todo: Todo):
    con = get_sql_con()
    cursor = con.cursor()
    cursor.execute(
        "UPDATE todo SET title = ?, description = ?, completed = ? WHERE id = ?",
        (todo.title, todo.description, int(todo.completed), todo_id)
    )
    if cursor.rowcount == 0:
        con.close()
        raise HTTPException(status_code=404, detail="Todo not found")
    con.commit()
    con.close()
    return {"message": f"Todo {todo_id} updated successfully"}

@app.patch("/todos/{todo_id}")
async def patch_todo(todo_id: int, status: TodoCompleted):
    con = get_sql_con()
    cursor = con.cursor()
    cursor.execute("UPDATE todo SET completed = ? WHERE id = ?", (int(status.completed), todo_id))
    if cursor.rowcount == 0:
        con.close()
        raise HTTPException(status_code=404, detail="Todo not found")
    con.commit()
    con.close()
    return {"message": f"Todo {todo_id} status updated"}

@app.delete("/todos/{todo_id}")
async def delete_todo(todo_id: int):
    con = get_sql_con()
    cursor = con.cursor()
    cursor.execute("DELETE FROM todo WHERE id = ?", (todo_id,))
    if cursor.rowcount == 0:
        con.close()
        raise HTTPException(status_code=404, detail="Todo not found")
    con.commit()
    con.close()
    return {"message": f"Todo {todo_id} deleted"}
