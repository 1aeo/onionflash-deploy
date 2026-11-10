import base64
import hashlib
import io
import json
import sqlite3
import tempfile
import tomllib
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from botocore.exceptions import ClientError, EndpointConnectionError

from archive_store import ArchiveBudgetReached, ArchiveStore, HOT_FILES, ALL_USERS, content_type


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.puts = []
        self.fail_puts = 0
        self.on_put = None

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        obj = self.objects[Key]
        return {"ContentLength": len(obj["body"]), "Metadata": obj["metadata"], "ETag": '"' + hashlib.md5(obj["body"]).hexdigest() + '"'}

    def put_object(self, Bucket, Key, Body, ContentLength, ContentMD5, ContentType, ACL, CacheControl, Metadata):
        if self.fail_puts:
            self.fail_puts -= 1
            raise EndpointConnectionError(endpoint_url="https://storage.example")
        body = Body.read()
        assert len(body) == ContentLength
        assert base64.b64encode(hashlib.md5(body).digest()).decode() == ContentMD5
        self.objects[Key] = {"body": body, "metadata": Metadata, "acl": ACL, "type": ContentType, "cache": CacheControl}
        self.puts.append(Key)
        if self.on_put:
            self.on_put(Key)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key]["body"])}

    def get_object_acl(self, Bucket, Key):
        grants = [{"Grantee": {"Type": "CanonicalUser", "ID": "owner"}, "Permission": "FULL_CONTROL"}]
        acl = self.objects[Key]["acl"]
        if acl == "public-read":
            grants.append({"Grantee": {"Type": "Group", "URI": ALL_USERS}, "Permission": "READ"})
        elif acl == "public-write":
            grants.append({"Grantee": {"Type": "Group", "URI": ALL_USERS}, "Permission": "WRITE"})
        return {"Owner": {"ID": "owner"}, "Grants": grants}


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        cfg = tomllib.loads((Path(__file__).parents[1] / "deployment.example.toml").read_text())["archive"]
        self.waits = []
        self.publisher = SimpleNamespace(cfg={"archive": cfg}, public=self.root / "public", database=self.root / "news.sqlite3",
                                         state=self.root / "state", net={"attempts": 2, "timeout_seconds": 10},
                                         secret_values=lambda: ["fake-credential-123"], delay=self.waits.append)
        self.publisher.public.mkdir()
        self.publisher.state.mkdir()
        self.db = sqlite3.connect(self.publisher.database)
        self.addCleanup(self.db.close)
        self.db.executescript("""CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
            INSERT INTO metadata VALUES('publication_generation','1');
            INSERT INTO metadata VALUES('publication_build_pending','0');
            CREATE TABLE public_artifacts(path TEXT PRIMARY KEY,sha256 TEXT,size INTEGER,content_type TEXT,updated_at TEXT,uploaded_sha256 TEXT);
        """)
        self.s3 = FakeS3()
        self.archive = ArchiveStore(self.publisher, self.s3)
        for path in HOT_FILES:
            self.artifact(path, b"safe hot asset")

    def artifact(self, path, body):
        target = self.publisher.public / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        sha = hashlib.sha256(body).hexdigest()
        self.db.execute("INSERT INTO public_artifacts VALUES(?,?,?,?,?,NULL) ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256,size=excluded.size,content_type=excluded.content_type", (path, sha, len(body), content_type(path), "2026-11-10T00:00:00Z"))
        self.db.commit()
        return sha

    def generation(self, value):
        self.db.execute("UPDATE metadata SET value=? WHERE key='publication_generation'", (str(value),))
        self.db.commit()

    def root_manifest(self, result):
        key = self.archive.cfg["public_prefix"] + "manifests/" + result["manifest_sha256"] + ".json"
        return json.loads(self.s3.objects[key]["body"])

    def lookup(self, result, relative):
        root = self.root_manifest(result)
        shard = hashlib.sha256(relative.encode()).hexdigest()[:root["shard_chars"]]
        key = self.archive.cfg["public_prefix"] + "manifests/" + root["shards"][shard] + ".json"
        return json.loads(self.s3.objects[key]["body"])["entries"][relative]

    def test_sync_publishes_immutable_objects_and_bounded_hot_snapshot(self):
        sha = self.artifact("news/example/index.html", b"<p>A story</p>")
        result = self.archive.sync()
        self.assertTrue(result["ready"])
        self.assertEqual(result["uploaded"], 1)
        self.assertEqual(self.lookup(result, "news/example/index.html")["sha256"], sha)
        self.assertEqual(self.archive.manifest(["index.html", "news/example/index.html"], result["generation"])["news/example/index.html"], sha)
        hot = self.root / "hot"
        self.assertEqual(set(self.archive.snapshot_hot(hot, result["generation"])), HOT_FILES)
        self.assertEqual({p.name for p in hot.iterdir()}, HOT_FILES)
        self.assertTrue(all("/objects/" in key or "/manifests/" in key for key in self.s3.objects))

    def test_repeat_sync_has_no_uploads_and_hot_change_reuses_archive_root(self):
        self.artifact("feed.xml", b"<rss/>")
        first = self.archive.sync()
        puts = list(self.s3.puts)
        repeated = self.archive.sync()
        self.assertEqual(repeated["manifest_sha256"], first["manifest_sha256"])
        self.assertEqual(repeated["uploaded"], 0)
        self.assertEqual(self.s3.puts, puts)
        self.artifact("index.html", b"new source availability times")
        self.generation(2)
        changed = self.archive.sync()
        self.assertTrue(changed["ready"])
        self.assertEqual(changed["manifest_sha256"], first["manifest_sha256"])
        self.assertEqual(self.s3.puts, puts)

    def test_correction_keeps_old_manifest_and_object_for_rollback(self):
        path = "news/example/index.html"
        old_sha = self.artifact(path, b"old valid story")
        old = self.archive.sync()
        new_sha = self.artifact(path, b"corrected story")
        self.generation(2)
        new = self.archive.sync()
        self.assertNotEqual(old["manifest_sha256"], new["manifest_sha256"])
        self.assertEqual(self.lookup(old, path)["sha256"], old_sha)
        self.assertEqual(self.lookup(new, path)["sha256"], new_sha)
        self.assertIn(self.archive.cfg["public_prefix"] + "objects/" + old_sha, self.s3.objects)
        self.assertTrue(all(obj["acl"] == "public-read" for obj in self.s3.objects.values()))

    def test_crash_after_upload_resumes_without_replacing_object(self):
        sha = self.artifact("feed.xml", b"<rss/>")
        upload = self.archive._upload_artifact
        def crash(*args):
            upload(*args)
            raise RuntimeError("crash after verified object upload")
        with patch.object(self.archive, "_upload_artifact", side_effect=crash):
            with self.assertRaises(RuntimeError):
                self.archive.sync()
        result = self.archive.sync()
        self.assertTrue(result["ready"])
        self.assertEqual(self.s3.puts.count(self.archive.cfg["public_prefix"] + "objects/" + sha), 1)

    def test_public_body_corruption_or_bad_acl_is_not_accepted(self):
        sha = self.artifact("feed.xml", b"<rss/>")
        self.archive.sync()
        key = self.archive.cfg["public_prefix"] + "objects/" + sha
        self.db.execute("UPDATE public_artifacts SET uploaded_sha256=NULL WHERE path='feed.xml'")
        self.db.commit()
        self.s3.objects[key]["body"] = b"<bad/>"
        with self.assertRaisesRegex(ValueError, "content verification"):
            self.archive.sync()
        self.assertEqual(self.s3.puts.count(key), 1)
        self.s3.objects[key]["body"] = b"<rss/>"
        self.s3.objects[key]["acl"] = "public-write"
        with self.assertRaisesRegex(ValueError, "ACL"):
            self.archive.sync()

    def test_generation_change_defers_activation_and_next_sync_resumes(self):
        self.artifact("feed.xml", b"<rss/>")
        self.s3.on_put = lambda key: self.generation(2) if "/objects/" in key else None
        result = self.archive.sync()
        self.assertFalse(result["ready"])
        self.assertIsNone(result["manifest_sha256"])
        with self.assertRaisesRegex(ValueError, "not ready"):
            self.archive.snapshot_hot(self.root / "stale", "1")
        self.s3.on_put = None
        self.assertTrue(self.archive.sync()["ready"])

    def test_incomplete_build_prevents_network_requests(self):
        self.db.execute("UPDATE metadata SET value='1' WHERE key='publication_build_pending'")
        self.db.commit()
        with self.assertRaisesRegex(ValueError, "not completed"):
            self.archive.sync()
        self.assertEqual(self.s3.puts, [])

    def test_manifest_refuses_partial_build_even_without_explicit_generation(self):
        self.artifact("feed.xml", b"<rss/>")
        self.archive.sync()
        self.db.execute("UPDATE metadata SET value='1' WHERE key='publication_build_pending'")
        self.db.commit()
        with self.assertRaisesRegex(ValueError, "not completed"):
            self.archive.manifest(["feed.xml"])

    def test_upload_count_and_time_budgets_resume_without_losing_history(self):
        self.archive.cfg["max_uploads_per_run"] = 1
        self.artifact("feed.xml", b"<rss/>")
        self.artifact("feed.json", b'{"items":[]}')
        first = self.archive.sync()
        self.assertFalse(first["ready"])
        self.assertEqual(first["pending"], 1)
        self.assertTrue(self.archive.sync()["ready"])
        self.artifact("feed.json", b'{"items":[1]}')
        self.generation(2)
        with patch.object(self.archive, "_budget", side_effect=ArchiveBudgetReached):
            held = self.archive.sync()
        self.assertFalse(held["ready"])
        self.assertTrue(held["budget_reached"])
        self.assertTrue(self.archive.sync()["ready"])

    def test_unknown_paths_symlinks_and_credentials_never_upload(self):
        for path in ["private/evidence.json", "news/a/../../secrets.env"]:
            with self.assertRaises(ValueError):
                self.archive._file(path)
        self.artifact("feed.xml", b"<rss/>")
        target = self.publisher.public / "feed.xml"
        target.unlink()
        target.symlink_to(self.root / "not-public")
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.archive.sync()
        target.unlink()
        self.artifact("feed.xml", b"x" * 65530 + b"fake-credential-123")
        with self.assertRaisesRegex(ValueError, "Credential"):
            self.archive.sync()
        self.assertEqual(self.s3.puts, [])

    def test_private_archives_are_immutable_and_verified(self):
        path = self.root / "private.gz"
        path.write_bytes(b"private evidence")
        key = self.archive.private_prefix + "evidence/example.json.gz"
        receipt = self.archive.put_private(path, key)
        self.assertEqual(self.s3.objects[key]["acl"], "private")
        self.assertEqual(self.archive.put_private(path, key), receipt)
        path.write_bytes(b"different evidence")
        with self.assertRaisesRegex(ValueError, "immutable"):
            self.archive.put_private(path, key)
        self.assertEqual(self.s3.puts.count(key), 1)

    def test_missing_source_row_does_not_remove_published_permalink(self):
        path = "news/old/index.html"
        self.artifact(path, b"permanent")
        first = self.archive.sync()
        self.db.execute("DELETE FROM public_artifacts WHERE path=?", (path,))
        self.db.commit()
        self.generation(2)
        second = self.archive.sync()
        self.assertEqual(second["manifest_sha256"], first["manifest_sha256"])
        self.assertEqual(self.lookup(second, path)["sha256"], hashlib.sha256(b"permanent").hexdigest())

    def test_obsolete_pagination_path_leaves_active_manifest_but_not_history(self):
        path = "page/3/index.html"
        sha = self.artifact(path, b"old page three")
        first = self.archive.sync()
        self.db.execute("DELETE FROM public_artifacts WHERE path=?", (path,))
        self.db.commit()
        self.generation(2)
        second = self.archive.sync()
        self.assertTrue(second["ready"])
        self.assertEqual(self.lookup(first, path)["sha256"], sha)
        with self.assertRaises(KeyError):
            self.lookup(second, path)
        self.assertIn(self.archive.cfg["public_prefix"] + "objects/" + sha, self.s3.objects)

    def test_transient_storage_failures_use_configured_backoff(self):
        self.s3.fail_puts = 1
        self.artifact("feed.xml", b"<rss/>")
        self.assertTrue(self.archive.sync()["ready"])
        self.assertEqual(self.waits, [0])

    def test_destination_change_reuploads_to_the_new_namespace(self):
        self.artifact("feed.xml", b"<rss/>")
        self.archive.sync()
        self.archive.cfg["public_prefix"] = "newproject/public/"
        result = self.archive.sync()
        self.assertTrue(result["ready"])
        self.assertTrue(any(key.startswith("newproject/public/objects/") for key in self.s3.objects))
        self.assertTrue(any(key.startswith("onionflash/public/objects/") for key in self.s3.objects))

    def test_repartition_preserves_history_and_reuses_immutable_objects(self):
        path = "news/old/index.html"
        self.artifact(path, b"permanent")
        first = self.archive.sync()
        puts = [key for key in self.s3.puts if "/objects/" in key]
        self.db.execute("DELETE FROM public_artifacts WHERE path=?", (path,))
        self.db.commit()
        self.archive.cfg["shard_prefix_chars"] = 3
        second = self.archive.sync()
        self.assertTrue(second["ready"])
        self.assertEqual(self.root_manifest(second)["shard_chars"], 3)
        self.assertNotEqual(first["manifest_sha256"], second["manifest_sha256"])
        self.assertEqual(self.lookup(second, path)["sha256"], hashlib.sha256(b"permanent").hexdigest())
        self.assertEqual([key for key in self.s3.puts if "/objects/" in key], puts)

    def test_mime_changes_do_not_overwrite_content_addressed_body(self):
        self.artifact("feed.xml", b"<rss/>")
        first = self.archive.sync()
        puts = list(self.s3.puts)
        self.db.execute("UPDATE public_artifacts SET content_type='application/xml' WHERE path='feed.xml'")
        self.db.commit()
        self.generation(2)
        second = self.archive.sync()
        self.assertTrue(second["ready"])
        self.assertEqual(self.s3.puts, puts)
        self.assertEqual(self.lookup(second, "feed.xml")["content_type"], "application/rss+xml; charset=utf-8")

    def test_oversized_artifact_raises_instead_of_waiting_forever(self):
        self.artifact("feed.xml", b"x" * 2048)
        self.archive.cfg["max_object_bytes"] = 1024
        with self.assertRaisesRegex(ValueError, "size"):
            self.archive.sync()


if __name__ == "__main__":
    unittest.main()
