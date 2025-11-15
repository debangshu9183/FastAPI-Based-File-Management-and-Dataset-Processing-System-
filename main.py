import os
import uuid
import pandas as pd
import psycopg2
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Form
from minio import Minio
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from dotenv import load_dotenv
import traceback

# ------------------------------------------------------
# Load environment variables
# ------------------------------------------------------
load_dotenv()

# ------------------------------------------------------
# Initialize FastAPI app
# ------------------------------------------------------
app = FastAPI(title="FastAPI File Management System (In-Memory Cache)")

# ------------------------------------------------------
# Connect to PostgreSQL
# ------------------------------------------------------
conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)
cursor = conn.cursor()

# ------------------------------------------------------
# Connect to MinIO
# ------------------------------------------------------
minio_client = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

BUCKET = os.getenv("MINIO_BUCKET")

if not minio_client.bucket_exists(BUCKET):
    minio_client.make_bucket(BUCKET)

# ------------------------------------------------------
# Initialize In-Memory Cache (No Redis Needed)
# ------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
    print(" In-memory cache initialized")


# ------------------------------------------------------
#  Upload Single File
# ------------------------------------------------------
@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    uploaded_by: str = Form("Debangshu"),
    description: str = Form("Uploaded via FastAPI")
):

    if not file.filename.endswith(('.csv', '.xlsx')):
        raise HTTPException(status_code=400, detail="Only CSV or Excel files allowed")

    filename = file.filename
    file_format = filename.split(".")[-1]

    # Get file size
    file.file.seek(0, os.SEEK_END)
    size = file.file.tell()
    file.file.seek(0)

    # Upload to MinIO
    try:
        minio_client.put_object(BUCKET, filename, file.file, length=-1, part_size=10*1024*1024)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MinIO upload failed: {e}")

    # Insert metadata
    cursor.execute("""
        INSERT INTO files2 (name, format, size, description, uploaded_by, file_path, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
    """, (filename, file_format, size, description, uploaded_by, filename, "active"))
    
    conn.commit()
    file_id = cursor.fetchone()[0]

    return {"message": "File uploaded successfully ✅", "file_id": file_id}


# ------------------------------------------------------
#  List Active Files
# ------------------------------------------------------
@app.get("/files")
def list_files():
    cursor.execute("""
        SELECT id, name, format, size, uploaded_by, status, upload_time
        FROM files2 WHERE status != 'deleted' ORDER BY id ASC;
    """)
    files = cursor.fetchall()

    return [
        {
            "id": f[0],
            "name": f[1],
            "format": f[2],
            "size": f[3],
            "uploaded_by": f[4],
            "status": f[5],
            "upload_time": f[6].strftime("%Y-%m-%d %H:%M:%S")
        }
        for f in files
    ]


# ------------------------------------------------------
#  Merge (Temporary + Cached)
# ------------------------------------------------------
@app.get("/merge")
async def merge_files(
    file1_id: int,
    file2_id: int,
    join_column: str = "customer_id",
    join_type: str = "inner"
):

    valid_joins = ["inner", "left", "right", "outer"]
    if join_type not in valid_joins:
        raise HTTPException(status_code=400, detail=f"Invalid join type: {valid_joins}")

    # Get file details
    cursor.execute("SELECT name, format FROM files2 WHERE id=%s", (file1_id,))
    f1 = cursor.fetchone()

    cursor.execute("SELECT name, format FROM files2 WHERE id=%s", (file2_id,))
    f2 = cursor.fetchone()

    if not f1 or not f2:
        raise HTTPException(status_code=404, detail="One or both files not found")

    try:
        # Read from MinIO
        f1_obj = minio_client.get_object(BUCKET, f1[0])
        f2_obj = minio_client.get_object(BUCKET, f2[0])

        df1 = pd.read_csv(f1_obj) if f1[1] == "csv" else pd.read_excel(f1_obj)
        df2 = pd.read_csv(f2_obj) if f2[1] == "csv" else pd.read_excel(f2_obj)

        f1_obj.close()
        f2_obj.close()

        # Normalize columns
        df1.columns = [c.lower().replace(" ", "_") for c in df1.columns]
        df2.columns = [c.lower().replace(" ", "_") for c in df2.columns]
        join_column = join_column.lower()

        if join_column not in df1.columns or join_column not in df2.columns:
            raise HTTPException(status_code=400,
                                detail=f"Column '{join_column}' missing in both files")

        merged_df = pd.merge(df1, df2, on=join_column, how=join_type)

        cache_key = str(uuid.uuid4())
        cache = FastAPICache.get_backend()
        await cache.set(cache_key, merged_df.to_json(), expire=600)

        return {
            "message": f"Merged using '{join_type}' join on '{join_column}'",
            "preview": merged_df.head().to_dict(orient="records"),   
            "cache_key": cache_key
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Merge error: {e}")


# ------------------------------------------------------
#  Save Merged Dataset Permanently
# ------------------------------------------------------
@app.post("/save_merged")
async def save_merged(cache_key: str):

    cache = FastAPICache.get_backend()
    merged_json = await cache.get(cache_key)

    if not merged_json:
        raise HTTPException(status_code=404, detail="Cache key expired or missing")

    merged_df = pd.read_json(merged_json)
    filename = f"merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    merged_df.to_csv(filename, index=False)

    try:
        minio_client.fput_object(BUCKET, filename, filename)

        cursor.execute("""
            INSERT INTO files2 (name, format, size, description, uploaded_by, file_path, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (filename, "csv", os.path.getsize(filename),
              "Merged dataset", "System", filename, "merged"))

        conn.commit()

    except Exception as e:
        os.remove(filename)
        raise HTTPException(status_code=500, detail=f"Save failed: {e}")

    await cache.clear(cache_key)
    os.remove(filename)

    return {"message": "Merged file saved successfully ", "file_name": filename}


# ------------------------------------------------------
#  DELETE File (MinIO + Database)
# ------------------------------------------------------
@app.delete("/delete/{file_id}")
async def delete_file(file_id: int):
    try:
        cursor.execute("SELECT file_path FROM files2 WHERE id=%s", (file_id,))
        result = cursor.fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="File not found")

        file_path = result[0]

        # Delete from MinIO
        minio_client.remove_object(BUCKET, file_path)

        # Delete DB row
        cursor.execute("DELETE FROM files2 WHERE id=%s", (file_id,))
        conn.commit()

        return {"message": f"File deleted and removed from DB (ID {file_id})"}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Delete error: {e}")
