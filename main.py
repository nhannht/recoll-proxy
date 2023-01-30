import hashlib
from datetime import datetime
from typing import List

# from sys import path as syspath
import sqlalchemy

# syspath.append("/usr/lib/python3.10/site-packages/recoll/")
from recoll import recoll, rclextract
from fastapi import FastAPI, Request, HTTPException
import os
from sqlalchemy import create_engine, Table, insert, Column, MetaData

from orjson import orjson
from starlette.responses import JSONResponse

db = recoll.connect()
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
async def write_readability_to_file(request: Request):
    try:
        request = await request.json()
        url = request["url"]
        readability = request["readability"]
        #  hash from url
        hash_filename = hashlib.sha256(url.encode("utf-8")).hexdigest()
        json_string = {"url": url,
                       "readability": readability,
                       "timestamp": datetime.now().isoformat(),
                       "hash_filename": hash_filename}

        with open(os.path.join(data_dir, f"{hash_filename}.json"), "w") as f:
            # write json string to file
            f.write(orjson.dumps(json_string).decode("utf-8"))

        res = {"status": "ok",
               "url": url,
               "readability": readability,
               "hash_filename": hash_filename}
        return JSONResponse(res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/write_database")
async def write_readability_to_database(request: Request):
    request = await request.json()
    url = request["url"]
    readability = request["readability"]
    #  hash from url
    hash_filename = hashlib.sha256(url.encode("utf-8")).hexdigest()
    # write to database
    try:
        query_ = insert(recoll_web_cache_table).values(url=url,
                                                       readability=readability,
                                                       timestamp=datetime.now().isoformat(),
                                                       hash_filename=hash_filename)
        connection.execute(query_)
        connection.commit()
        res = {"status": "ok",
               "url": url,
               "readability": readability,
               "hash_filename": hash_filename}
        return JSONResponse(res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query")
async def query(request: Request):
    q_json = await request.json()
    q = q_json["query"]
    query_: recoll.Query = db.query()
    query_.execute(q)
    docs: List[recoll.Doc] = query_.fetchmany(10)
    results = []
    for doc in docs:
        title = doc.title
        filename = doc.filename
        doc_abstract = query_.highlight(doc.abstract)
        url = doc.url
        snippet_abstract = query_.highlight(query_.makedocabstract(doc))
        relevant_rating = doc.relevancyrating
        result = {"title": title,
                  "filename": filename,
                  "doc_abstract": doc_abstract,
                  "url": url,
                  "snippet_abstract": snippet_abstract,
                  "relevant_rating": relevant_rating}
        results.append(result)
    query_.close()
    return JSONResponse(results)
