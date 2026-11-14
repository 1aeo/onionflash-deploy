"""Test doubles and base fixtures shared by the deployment tests."""
import base64, hashlib, io, os, sqlite3, tempfile, unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError, EndpointConnectionError
import publish

OLD, NEW, OTHER = ("11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333")
ENV = {
    "CLOUDFLARE_ACCOUNT_ID": "a" * 32, "CLOUDFLARE_API_TOKEN": "unit-test-not-a-real-secret",
    "CLOUDFLARE_ZONE_TOKEN": "zone-test-not-a-real-secret", "CLOUDFLARE_ZONE_ID": "b" * 32,
    "DO_SPACES_KEY": "spaces-test-not-a-real-key", "DO_SPACES_SECRET": "spaces-test-not-a-real-secret",
}


class FakeS3:
    def __init__(self, *, cleanup_error=False, retention_error=False, listed=()):
        self.objects, self.puts, self.heads, self.uploads, self.deleted = {}, [], [], [], []
        self.fail_puts, self.on_put = 0, None
        self.cleanup_error, self.retention_error, self.listed = cleanup_error, retention_error, list(listed)

    def head_object(self, Bucket, Key):
        self.heads.append(Key)
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        obj = self.objects[Key]
        return {"ContentLength": len(obj["body"]), "Metadata": obj["metadata"], "ETag": '"' + hashlib.md5(obj["body"]).hexdigest() + '"'}

    def put_object(self, Bucket, Key, Body, ContentLength=None, ContentMD5=None, ContentType="application/octet-stream", ACL="private", CacheControl=None, Metadata=None):
        if self.fail_puts:
            self.fail_puts -= 1
            raise EndpointConnectionError(endpoint_url="https://storage.example")
        body = Body.read()
        if ContentLength is not None:
            assert len(body) == ContentLength
        if ContentMD5 is not None:
            assert base64.b64encode(hashlib.md5(body).digest()).decode() == ContentMD5
        self.objects[Key] = {"body": body, "metadata": Metadata or {}, "acl": ACL, "type": ContentType, "cache": CacheControl}
        self.puts.append(Key)
        self.uploads.append((Bucket, Key, body, ACL))
        if self.on_put:
            self.on_put(Key)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key]["body"])}

    def delete_object(self, Bucket, Key):
        self.deleted.append((Bucket, Key))
        if self.cleanup_error:
            raise RuntimeError("cleanup unavailable")
        self.objects.pop(Key, None)

    def get_object_acl(self, Bucket, Key):
        grants = [{"Grantee": {"Type": "CanonicalUser", "ID": "owner"}, "Permission": "FULL_CONTROL"}]
        if self.objects[Key]["acl"] == "public-read":
            grants.append({"Grantee": {"Type": "Group", "URI": "http://acs.amazonaws.com/groups/global/AllUsers"}, "Permission": "READ"})
        return {"Owner": {"ID": "owner"}, "Grants": grants}

    def get_paginator(self, name):
        return self

    def paginate(self, **kwargs):
        if self.retention_error:
            raise RuntimeError("retention listing unavailable")
        return [{"Contents": self.listed}]


def seed_public_site(public_dir, db_path=None, *, full_schema=False):
    public_dir = Path(public_dir)
    public_dir.mkdir(parents=True, exist_ok=True)
    bodies = {}
    for name in publish.public_files():
        body = b"safe generated content"
        if name == "feed.xml":
            body = b"<rss version='2.0'><channel><title>News</title></channel></rss>"
        elif name == "feed.json":
            body = b'{"items":[]}'
        target = public_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        bodies[name] = body
    if db_path is not None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(db_path)) as db:
            db.executescript(
                "CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);"
                "INSERT OR REPLACE INTO metadata VALUES('publication_generation','1');"
                "INSERT OR REPLACE INTO metadata VALUES('publication_build_pending','0');"
            )
            if full_schema:
                db.execute("CREATE TABLE IF NOT EXISTS public_artifacts(path TEXT PRIMARY KEY,sha256 TEXT,size INTEGER,content_type TEXT,updated_at TEXT,uploaded_sha256 TEXT)")
                for name, body in bodies.items():
                    db.execute("INSERT OR REPLACE INTO public_artifacts VALUES(?,?,?,'text/plain','2026-11-10T00:00:00Z',NULL)",
                               (name, hashlib.sha256(body).hexdigest(), len(body)))
            else:
                db.execute("CREATE TABLE IF NOT EXISTS public_artifacts(path TEXT PRIMARY KEY, sha256 TEXT)")
                for name, body in bodies.items():
                    db.execute("INSERT OR REPLACE INTO public_artifacts VALUES(?,?)", (name, hashlib.sha256(body).hexdigest()))
            db.commit()
    return public_dir


def make_publisher(test, output_dir="build/public"):
    temporary = tempfile.TemporaryDirectory()
    test.addCleanup(temporary.cleanup)
    directory = Path(temporary.name).resolve()
    environment = patch.dict(os.environ, ENV)
    environment.start()
    test.addCleanup(environment.stop)
    (directory / "deployment.toml").write_text((publish.ROOT / "deployment.example.toml").read_text())
    (directory / "onionflash.toml").write_text(
        f'[site]\nbase_url = "https://example.com/onionflash/"\n[storage]\ndatabase = "state/news.sqlite3"\n'
        f'output_dir = "{output_dir}"\n[[sources]]\nid = "test"\nurl = "https://example.org/feed.xml"\n'
    )
    return directory, publish.Publisher(directory / "deployment.toml")


class DeployTestCase(unittest.TestCase):
    OUTPUT_DIR = "build/public"

    def setUp(self):
        self.directory, self.p = make_publisher(self, output_dir=self.OUTPUT_DIR)
        self.root = self.directory
        self.publisher = self.p

    def seed_public(self, *, full_schema=False):
        return seed_public_site(self.p.public, self.p.database, full_schema=full_schema)

    def add_article(self, story_id):
        body = f"<p>{story_id}</p>".encode()
        target = self.p.public / "news" / story_id / "index.html"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        with closing(sqlite3.connect(self.p.database)) as db, db:
            db.execute("INSERT INTO public_artifacts VALUES(?,?)", (f"news/{story_id}/index.html", hashlib.sha256(body).hexdigest()))
