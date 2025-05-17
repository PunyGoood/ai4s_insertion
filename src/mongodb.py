import json
import urllib.parse

import pymongo

MAX_DOC_SIZE = 5 << 20  # 5MiB
FLUSH_SIZE = 1 << 20  # 1MiB
FLUSH_COUNT = 200
MAX_RETRIES = 10


def json_dumps(d: dict, **kwargs) -> str:
    return json.dumps(d, ensure_ascii=False, **kwargs)


def get_mongo_client():
    # host = "10.140.84.61:30017,10.140.84.62:30018"
    host = "127.0.0.1:27017"
    username = "mongoadmin"
    password = urllib.parse.quote_plus("123456")
    database = "h_project"
    auth_database = "admin"
    uri = f"mongodb://{username}:{password}@{host}/{database}?authSource={auth_database}"
    client = pymongo.MongoClient(uri)
    return client


def get_mongo_db():
    client = get_mongo_client()
    return client.get_database()


def get_collection(name: str):
    db = get_mongo_db()
    return db.get_collection(name)


class MongoBulkWriter:
    """CAUTION: Not thread-safe."""

    def __init__(
        self,
        collection: str,
        upsert=True,
        max_doc_size=MAX_DOC_SIZE,
        flush_size=FLUSH_SIZE,
        flush_count=FLUSH_COUNT,
        # max_retries=MAX_RETRIES,
    ) -> None:
        self.coll = get_collection(collection)
        self.buffer = []
        self.buffer_size = 0
        self.upsert = upsert
        self.max_doc_size = max_doc_size
        self.flush_size = flush_size
        self.flush_count = flush_count

    def write(self, doc: dict, id=None, pk=[]):
        if not id and not pk:
            raise Exception("param id & pk cannot be both empty.")
        if id is not None:
            filter = {"_id": id}
        else:
            filter = {k: doc.get(k) for k in pk}
        if all(v is None for v in filter.values()):
            raise Exception("all filter values are null.")

        doc_str = json_dumps(doc).encode("utf-8")
        doc_size = len(doc_str)
        if doc_size > self.max_doc_size:
            raise Exception(f"doc [{doc_str[:512]}] is too large.")

        self.buffer.append((filter, doc))

        self.buffer_size += doc_size
        if self.buffer_size >= self.flush_size or len(self.buffer) >= self.flush_count:
            self.__flush()

    def flush(self):
        if self.buffer_size > 0:
            self.__flush()

    def __flush(self):
        bulkOps = []
        for filter, doc in self.buffer:
            bulkOps.append(
                pymongo.UpdateOne(
                    filter,
                    {"$set": doc},
                    upsert=True,
                )
            )
        self.coll.bulk_write(bulkOps)
