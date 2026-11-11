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

from archive_store import ArchiveBudgetReached, ArchiveStore, HOT_FILES, content_type

ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"


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

    def delete_object(self, Bucket, Key):
        del self.objects[Key]

    def get_object_acl(self, Bucket, Key):
        grants = [{"Grantee": {"Type": "CanonicalUser", "ID": "owner"}, "Permission": "FULL_CONTROL"}]
        if self.objects[Key]["acl"] == "public-read":
            grants.append({"Grantee": {"Type": "Group", "URI": ALL_USERS}, "Permission": "READ"})
        return {"Owner": {"ID": "owner"}, "Grants": grants}


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        cfg = tomllib.loads((Path(__file__).parents[1] / "deployment.example.toml").read_text())["archive"]
        self.waits = []
        self.unsigned, self.exposure = [], None  # unsigned privacy checks and their finding
        self.publisher = SimpleNamespace(cfg={"archive": cfg}, public=self.root / "public", database=self.root / "news.sqlite3",
                                         state=self.root / "state", net={"attempts": 2, "timeout_seconds": 10},
                                         secret_values=lambda: ["fake-credential-123"], delay=self.waits.append,
                                         unsigned_read_problem=lambda *request: self.unsigned.append(request) or self.exposure)
        self.publisher.public.mkdir()
        self.publisher.state.mkdir()
        self.db = sqlite3.connect(self.publisher.database)
        self.addCleanup(self.db.close)
        self.db.executescript("""CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
            INSERT INTO metadata VALUES('publication_generation','1');
            INSERT INTO metadata VALUES('publication_build_pending','0');
            INSERT INTO metadata VALUES('publication_no_browser_js_ready','1');
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

    def test_failed_root_upload_rebuilds_from_committed_new_shards(self):
        path = "feed.xml"
        old_sha = self.artifact(path, b"<rss>old</rss>")
        old = self.archive.sync()
        new_sha = self.artifact(path, b"<rss>corrected</rss>")
        self.generation(2)
        upload = self.archive._public_immutable
        attempted_roots = []
        def fail_root(file, key, sha, size, mime="application/octet-stream"):
            if file.name == "manifest-root.json":
                attempted_roots.append(sha)
                raise RuntimeError("root upload failed after shards committed")
            return upload(file, key, sha, size, mime)
        with patch.object(self.archive, "_public_immutable", side_effect=fail_root):
            with self.assertRaisesRegex(RuntimeError, "root upload failed"):
                self.archive.sync()
        self.assertEqual(self.db.execute("SELECT count(*) FROM archive_shards WHERE dirty!=0").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT value FROM metadata WHERE key='archive_manifest_sha256'").fetchone()[0], "")
        self.assertEqual(self.db.execute("SELECT value FROM metadata WHERE key='archive_manifest_generation'").fetchone()[0], "")
        current = self.archive.sync()
        self.assertTrue(current["ready"])
        self.assertEqual(current["manifest_sha256"], attempted_roots[0])
        self.assertNotEqual(current["manifest_sha256"], old["manifest_sha256"])
        self.assertEqual(self.lookup(current, path)["sha256"], new_sha)
        self.assertEqual(self.lookup(old, path)["sha256"], old_sha)

    def test_crash_after_root_upload_resumes_without_reuploading_root(self):
        path = "news/example/index.html"
        self.artifact(path, b"old article")
        old = self.archive.sync()
        new_sha = self.artifact(path, b"corrected article")
        self.generation(2)
        upload = self.archive._public_immutable
        uploaded_roots = []
        def crash_after_root(file, key, sha, size, mime="application/octet-stream"):
            upload(file, key, sha, size, mime)
            if file.name == "manifest-root.json":
                uploaded_roots.append(key)
                raise RuntimeError("crash before root metadata commit")
        with patch.object(self.archive, "_public_immutable", side_effect=crash_after_root):
            with self.assertRaisesRegex(RuntimeError, "root metadata"):
                self.archive.sync()
        self.assertEqual(self.db.execute("SELECT value FROM metadata WHERE key='archive_manifest_sha256'").fetchone()[0], "")
        current = self.archive.sync()
        self.assertTrue(current["ready"])
        self.assertNotEqual(current["manifest_sha256"], old["manifest_sha256"])
        self.assertEqual(self.lookup(current, path)["sha256"], new_sha)
        self.assertEqual(self.s3.puts.count(uploaded_roots[0]), 1)

    def test_stale_cached_root_is_not_rebound_to_a_new_shard_generation(self):
        path = "feed.json"
        self.artifact(path, b'{"items":[]}')
        old = self.archive.sync()
        new_sha = self.artifact(path, b'{"items":[1]}')
        self.generation(2)
        current = self.archive.sync()
        # Simulate a stale root pointer left by the earlier implementation.
        self.db.execute("UPDATE metadata SET value=? WHERE key='archive_manifest_sha256'", (old["manifest_sha256"],))
        self.db.commit()
        repaired = self.archive.sync()
        self.assertTrue(repaired["ready"])
        self.assertEqual(repaired["manifest_sha256"], current["manifest_sha256"])
        self.assertEqual(self.lookup(repaired, path)["sha256"], new_sha)

    def test_public_object_changed_in_storage_is_not_accepted_or_overwritten(self):
        sha = self.artifact("feed.xml", b"<rss/>")
        self.archive.sync()
        key = self.archive.cfg["public_prefix"] + "objects/" + sha
        self.db.execute("UPDATE public_artifacts SET uploaded_sha256=NULL WHERE path='feed.xml'")
        self.db.commit()
        self.s3.objects[key]["body"] = b"<bad/>"  # same size and metadata, other bytes
        with self.assertRaisesRegex(ValueError, "changed outside this publisher"):
            self.archive.sync()
        self.assertEqual(self.s3.puts.count(key), 1)

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
        self.assertEqual(self.unsigned, [(self.archive.cfg["endpoint_url"], self.archive.bucket, key)])
        self.assertEqual(self.archive.put_private(path, key), receipt)
        # A private ACL is no proof once a bucket policy grants reads: the unsigned
        # request decides. A new object is removed; an existing one may back references.
        self.exposure = "was publicly readable"
        for target in (key, self.archive.private_prefix + "evidence/new.json.gz"):
            with self.assertRaisesRegex(ValueError, "publicly readable"):
                self.archive.put_private(path, target)
        self.assertEqual(set(self.s3.objects), {key})
        self.exposure = None
        path.write_bytes(b"different evidence")
        with self.assertRaisesRegex(ValueError, "immutable"):
            self.archive.put_private(path, key)
        self.assertEqual(self.s3.puts.count(key), 1)

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

    def test_topic_html_and_pagination_have_exact_safe_paths(self):
        from archive_store import archive_path_allowed
        paths = ["topics/security/index.html", "topics/security/page/2/index.html"]
        for path in paths:
            self.assertTrue(archive_path_allowed(path))
            self.artifact(path, b"<p>Static topic index</p>")
        for path in ["topics/private/index.html", "topics/security/../private/index.html",
                     "topics/security/page/0/index.html", "topics/security/page/02/index.html",
                     "topics/security/page/1000000000/index.html", "topics/security/app.js"]:
            self.assertFalse(archive_path_allowed(path))
        before = self.archive.sync()
        self.db.execute("DELETE FROM public_artifacts WHERE path=?", (paths[1],))
        self.db.commit()
        self.generation(2)
        after = self.archive.sync()
        self.assertTrue(after["ready"])
        self.assertEqual(self.lookup(before, paths[1])["content_type"], "text/html; charset=utf-8")
        self.assertEqual(self.lookup(after, paths[0])["content_type"], "text/html; charset=utf-8")
        with self.assertRaises(KeyError):
            self.lookup(after, paths[1])

    def test_retired_script_leaves_new_root_and_hot_snapshot_but_old_root_survives(self):
        self.artifact("news/permanent/index.html", b"<p>Permanent article</p>")
        first = self.archive.sync()
        # Simulate an older all-file archive catalog, including immutable script
        # bytes and a root which an existing version can still select.
        script = b"console.log('legacy browser behavior')"
        script_sha = self.artifact("app.js", script)
        prefix = self.archive.cfg["public_prefix"]
        self.s3.objects[prefix + "objects/" + script_sha] = {
            "body": script, "metadata": {"sha256": script_sha}, "acl": "public-read", "type": "text/javascript", "cache": "immutable"}
        self.archive._initialize()
        shard = hashlib.sha256(b"app.js").hexdigest()[:self.archive.cfg["shard_prefix_chars"]]
        self.db.execute("INSERT INTO archive_index VALUES(?,?,?,?,?)", ("app.js", script_sha, len(script), "text/javascript", shard))
        self.db.execute("INSERT INTO archive_shards(shard,dirty) VALUES(?,1) ON CONFLICT(shard) DO UPDATE SET dirty=1", (shard,))
        self.db.commit()
        with tempfile.TemporaryDirectory(dir=self.publisher.state) as temporary:
            old_hash = self.archive._prepare_manifest("1", Path(temporary))
        old = {"manifest_sha256": old_hash}
        self.assertEqual(self.lookup(old, "app.js")["sha256"], script_sha)
        existing_objects = dict(self.s3.objects)
        self.generation(2)
        current = self.archive.sync()
        self.assertTrue(current["ready"])
        self.assertNotEqual(current["manifest_sha256"], old_hash)
        with self.assertRaises(KeyError):
            self.lookup(current, "app.js")
        self.assertEqual(self.lookup(old, "app.js")["sha256"], script_sha)
        self.assertEqual(self.lookup(first, "news/permanent/index.html"), self.lookup(current, "news/permanent/index.html"))
        for key, value in existing_objects.items():
            self.assertEqual(self.s3.objects[key], value)
        snapshot = self.root / "no-script-hot"
        self.assertNotIn("app.js", self.archive.snapshot_hot(snapshot, current["generation"]))
        self.assertFalse((snapshot / "app.js").exists())

    def test_static_migration_gate_prevents_any_archive_upload(self):
        for value in [None, "0"]:
            with self.subTest(value=value):
                self.db.execute("DELETE FROM metadata WHERE key='publication_no_browser_js_ready'")
                if value is not None:
                    self.db.execute("INSERT INTO metadata VALUES('publication_no_browser_js_ready',?)", (value,))
                self.db.commit()
                with self.assertRaisesRegex(ValueError, "upgraded core build"):
                    self.archive.sync()
                self.assertEqual(self.s3.puts, [])

    def test_scripted_archive_html_is_rejected_after_hash_validation(self):
        self.artifact("topics/security/index.html", b'<p onclick="alert(1)">Bad</p>')
        with self.assertRaisesRegex(ValueError, "event handler"):
            self.archive.sync()
        self.assertEqual(self.s3.puts, [])

    def test_transient_storage_failures_use_configured_backoff(self):
        self.s3.fail_puts = 1
        self.artifact("feed.xml", b"<rss/>")
        self.assertTrue(self.archive.sync()["ready"])
        self.assertEqual(self.waits, [0])

    def test_destination_change_reuploads_to_the_new_namespace(self):
        self.artifact("feed.xml", b"<rss/>")
        self.artifact("page/3/index.html", b"old page three")
        self.archive.sync()
        # The core removes pagination that a larger page size makes obsolete.
        self.db.execute("DELETE FROM public_artifacts WHERE path='page/3/index.html'")
        self.db.commit()
        self.generation(2)
        self.archive.cfg["public_prefix"] = "newproject/public/"
        result = self.archive.sync()
        self.assertTrue(result["ready"])
        with self.assertRaises(KeyError):
            self.lookup(result, "page/3/index.html")
        self.assertTrue(any(key.startswith("newproject/public/objects/") for key in self.s3.objects))
        self.assertTrue(any(key.startswith("onionflash/public/objects/") for key in self.s3.objects))

    def test_repartition_preserves_history_and_reuses_immutable_objects(self):
        path = "news/old/index.html"
        self.artifact(path, b"permanent")
        first = self.archive.sync()
        puts = [key for key in self.s3.puts if "/objects/" in key]
        (self.publisher.public / path).unlink()  # Local rebuild may lag verified ledger records.
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
