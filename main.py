import hashlib
from datetime import datetime

import sqlalchemy

from fastapi import FastAPI, Request, HTTPException
import os
from sqlalchemy import create_engine, Table, insert, Column, MetaData

from orjson import orjson

app = FastAPI()

data_dir = os.path.expanduser("~/recoll_web_cache")
# create data_dir if not exist
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

sqlite_path = os.path.expanduser("~/recoll_web_cache/recoll_web_cache.db")
# create sqlite database
engine = create_engine("sqlite:///" + sqlite_path, echo=True)
# connect to database
connection = engine.connect()
# create table if not exist
metadata = MetaData()
if not engine.dialect.has_table(connection, "recoll_web_cache"):
    recoll_web_cache_table = Table("recoll_web_cache", metadata,
                                   Column("url", sqlalchemy.String),
                                   Column("readability", sqlalchemy.String),
                                   Column("timestamp", sqlalchemy.String),
                                   Column("hash_filename", sqlalchemy.String),
                                   )
    metadata.create_all(engine)


@app.post("/write_file_cache")
async def write_readability_to_file(data: Request):
    try:
        data = await data.json()
        url = data["url"]
        readability = data["readability"]
        #  hash from url
        hash_filename = hashlib.sha256(url.encode("utf-8")).hexdigest()
        json_string = {"url": url,
                       "readability": readability,
                       "timestamp": datetime.now().isoformat(),
                       "hash_filename": hash_filename}

        with open(os.path.join(data_dir, f"{hash_filename}.json"), "w") as f:
            # write json string to file
            f.write(orjson.dumps(json_string).decode("utf-8"))

        return {"status": "ok",
                "url": url,
                "readability": readability,
                "hash_filename": hash_filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/write_database")
async def write_readability_to_database(data: Request):
    data = await data.json()
    url = data["url"]
    readability = data["readability"]
    #  hash from url
    hash_filename = hashlib.sha256(url.encode("utf-8")).hexdigest()
    # write to database
    try:
        query = insert(recoll_web_cache_table).values(url=url,
                                                      readability=readability,
                                                      timestamp=datetime.now().isoformat(),
                                                      hash_filename=hash_filename)
        connection.execute(query)
        connection.commit()
        return {"status": "ok",
                "url": url,
                "readability": readability,
                "hash_filename": hash_filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
