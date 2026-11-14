import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import tomllib
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import archive_store
from archive_store import ArchiveStore
from publish import public_files
from tests.fakes import FakeS3

# Serves stored objects to the real Worker as Spaces would, and prints what it answers.
WORKER = """
import { readFileSync } from "node:fs";
const state = JSON.parse(readFileSync(0, "utf8"));
const { default: worker } = await import(state.worker);
globalThis.fetch = async (url, options) => {
  const object = state.objects[String(url).slice(state.env.ARCHIVE_ORIGIN.length)];
  if (!object) return new Response(null, { status: 403 });
  const body = Buffer.from(object.body, "hex");
  return new Response(options.method === "HEAD" ? null : body,
    { headers: { "content-length": String(body.length), "x-amz-meta-sha256": object.sha256 } });
};
const env = { ...state.env, ASSETS: { fetch: () => new Response(null, { status: 404 }) } };
const served = {};
for (const path of state.paths) for (const method of ["GET", "HEAD"]) {
  const response = await worker.fetch(new Request("https://example.com" + path, { method }), env);
  served[`${method} ${path}`] = [response.status, response.headers.get("etag"), Buffer.from(await response.arrayBuffer()).toString("hex")];
}
console.log(JSON.stringify(served));
"""


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
            CREATE TABLE public_artifacts(path TEXT PRIMARY KEY,sha256 TEXT,size INTEGER,content_type TEXT,updated_at TEXT,uploaded_sha256 TEXT);
        """)
        self.s3 = FakeS3()
        self.archive = ArchiveStore(self.publisher, self.s3)
        self.prefix = self.archive.cfg["public_prefix"]
        for path in public_files():
            self.artifact(path, b"safe hot asset")

    def artifact(self, path, body):
        target = self.publisher.public / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        sha = hashlib.sha256(body).hexdigest()
        self.db.execute("INSERT INTO public_artifacts VALUES(?,?,?,'text/plain',?,NULL) ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256,size=excluded.size",
                        (path, sha, len(body), "2026-11-10T00:00:00Z"))
        self.db.commit()
        return sha

    def generation(self, value):
        self.db.execute("UPDATE metadata SET value=? WHERE key='publication_generation'", (str(value),))
        self.db.commit()

    def root_manifest(self, result):
        return json.loads(self.s3.objects[self.prefix + "manifests/" + result["manifest_sha256"] + ".json"]["body"])

    def lookup(self, result, relative):
        root = self.root_manifest(result)
        shard = hashlib.sha256(relative.encode()).hexdigest()[:root["shard_chars"]]
        return json.loads(self.s3.objects[self.prefix + "manifests/" + root["shards"][shard] + ".json"]["body"])["entries"][relative]

    def is_root(self, key):
        return b'"shard_chars"' in self.s3.objects[key]["body"]

    def test_sync_uploads_permalinks_and_the_snapshot_holds_every_other_file(self):
        shas = {path: self.artifact(path, path.encode()) for path in (
            "news/a/index.html", "news/a/story.json", "page/2/index.html", "topics/security/index.html", "tags/security.xml")}
        result = self.archive.sync()
        self.assertTrue(result["ready"])
        self.assertEqual(result["uploaded"], 2)
        self.assertTrue(all("/objects/" in key or "/manifests/" in key for key in self.s3.objects))
        for path in ("news/a/index.html", "news/a/story.json"):
            self.assertEqual(self.lookup(result, path)["sha256"], shas[path])
        hot = self.root / "hot"
        manifest = self.archive.snapshot(hot, result["generation"])
        self.assertEqual({p.relative_to(hot).as_posix() for p in hot.rglob("*") if p.is_file()},
                         public_files() | {"page/2/index.html", "topics/security/index.html", "tags/security.xml"})
        # The publication manifest adds the article pages its root routes.
        self.assertEqual(set(manifest), {*public_files(), *shas} - {"news/a/story.json"})
        self.assertEqual(manifest["news/a/index.html"], shas["news/a/index.html"])

    def test_repeat_sync_and_a_hot_change_make_no_storage_request(self):
        self.artifact("news/a/index.html", b"<p>A</p>")
        first = self.archive.sync()
        requests = (list(self.s3.heads), list(self.s3.puts), list(self.unsigned))
        repeated = self.archive.sync()
        self.artifact("index.html", b"new source availability times")
        self.generation(2)
        changed = self.archive.sync()
        self.assertTrue(changed["ready"])
        self.assertEqual({repeated["manifest_sha256"], changed["manifest_sha256"]}, {first["manifest_sha256"]})
        # Objects stored by an earlier run are not checked again, anonymously or otherwise.
        self.assertEqual((self.s3.heads, self.s3.puts, self.unsigned), requests)

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
        self.assertIn(self.prefix + "objects/" + old_sha, self.s3.objects)
        self.assertTrue(all(obj["acl"] == "public-read" for obj in self.s3.objects.values()))

    def test_crash_after_upload_resumes_without_replacing_object(self):
        sha = self.artifact("news/a/index.html", b"<p>A</p>")
        def crash(key):
            self.s3.on_put = None
            raise RuntimeError("crash after the object upload")
        self.s3.on_put = crash
        with self.assertRaisesRegex(RuntimeError, "crash"):
            self.archive.sync()
        self.assertTrue(self.archive.sync()["ready"])
        self.assertEqual(self.s3.puts.count(self.prefix + "objects/" + sha), 1)

    def test_an_interrupted_root_upload_resumes_from_the_uploaded_shards(self):
        path = "news/a/index.html"
        old_sha = self.artifact(path, b"old")
        old = self.archive.sync()
        for stored in (False, True):  # The upload fails, or the run stops before recording it.
            new_sha = self.artifact(path, b"corrected %d" % stored)
            self.generation(3 + stored)
            roots = []
            def interrupt(key):
                if self.is_root(key):
                    roots.append(key)
                    if not stored:
                        del self.s3.objects[key]
                    raise RuntimeError("interrupted")
            self.s3.on_put = interrupt
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                self.archive.sync()
            self.s3.on_put = None
            with self.assertRaisesRegex(ValueError, "not ready"):
                self.archive.snapshot(self.root / f"hot-{stored}", str(3 + stored))
            heads, puts = len(self.s3.heads), len(self.s3.puts)
            current = self.archive.sync()
            self.assertTrue(current["ready"])
            # The shards are not checked again; the root is stored once.
            self.assertEqual(roots, [self.prefix + "manifests/" + current["manifest_sha256"] + ".json"])
            self.assertEqual(self.s3.heads[heads:], roots)
            self.assertEqual(self.s3.puts[puts:], [] if stored else roots)
            self.assertEqual(self.lookup(current, path)["sha256"], new_sha)
        self.assertEqual(self.lookup(old, path)["sha256"], old_sha)

    def test_public_object_changed_in_storage_is_not_accepted_or_overwritten(self):
        sha = self.artifact("news/a/index.html", b"<p>A</p>")
        self.archive.sync()
        key = self.prefix + "objects/" + sha
        self.db.execute("UPDATE public_artifacts SET uploaded_sha256=NULL WHERE path='news/a/index.html'")
        self.db.commit()
        self.s3.objects[key]["body"] = b"<p>B</p>"  # same size and metadata, other bytes
        with self.assertRaisesRegex(ValueError, "changed outside this publisher"):
            self.archive.sync()
        self.assertEqual(self.s3.puts.count(key), 1)

    def test_a_rebuild_during_upload_defers_the_root_until_its_bytes_are_stored(self):
        path = "news/a/index.html"
        self.artifact(path, b"<p>A</p>")
        def rebuild(key):  # A direct core build or correction while the old bytes upload.
            self.s3.on_put = None
            self.artifact(path, b"<p>A, corrected</p>")
            self.generation(2)
        self.s3.on_put = rebuild
        result = self.archive.sync()
        self.assertFalse(result["ready"])
        self.assertIsNone(result["manifest_sha256"])
        with self.assertRaisesRegex(ValueError, "not ready"):
            self.archive.snapshot(self.root / "stale", "1")
        current = self.archive.sync()
        self.assertTrue(current["ready"])
        self.assertIn(self.prefix + "objects/" + self.lookup(current, path)["sha256"], self.s3.objects)

    def test_an_incomplete_build_is_neither_uploaded_nor_snapshotted(self):
        result = self.archive.sync()
        puts = list(self.s3.puts)
        self.artifact("news/a/index.html", b"<p>A</p>")
        self.db.execute("UPDATE metadata SET value='1' WHERE key='publication_build_pending'")
        self.db.commit()
        for attempt in (self.archive.sync, lambda: self.archive.snapshot(self.root / "hot", result["generation"])):
            with self.assertRaisesRegex(ValueError, "not completed"):
                attempt()
        self.assertEqual(self.s3.puts, puts)

    def test_count_and_byte_budgets_leave_the_rest_for_the_next_run(self):
        for budget, value in (("MAX_UPLOADS_PER_RUN", 1), ("MAX_UPLOAD_BYTES_PER_RUN", 1500)):
            for name in "ab":
                self.artifact(f"news/{name}/index.html", (budget + name).encode().ljust(1000, b"."))
            with patch.object(archive_store, budget, value):
                held = self.archive.sync()
            self.assertEqual((held["ready"], held["uploaded"], held["pending"]), (False, 1, 1), budget)
            self.assertTrue(self.archive.sync()["ready"])

    def test_a_deadline_reached_mid_run_keeps_the_uploads_finished_before_it(self):
        paths = ["news/a/index.html", "news/b/index.html"]
        shas = [self.artifact(path, path.encode()) for path in paths]
        clock = [0.0]
        def expire(key):  # The deadline passes once the first object is stored.
            clock[0] = 1e9
        self.s3.on_put = expire
        with patch.object(archive_store.time, "monotonic", lambda: clock[0]):
            held = self.archive.sync()
        self.assertEqual((held["ready"], held["budget_reached"], held["uploaded"], held["pending"]), (False, True, 1, 1))
        self.s3.on_put = None
        resumed = self.archive.sync()
        self.assertTrue(resumed["ready"])
        self.assertEqual([self.lookup(resumed, path)["sha256"] for path in paths], shas)
        self.assertEqual(len(self.s3.puts), len(set(self.s3.puts)))

    def test_unknown_paths_symlinks_and_credentials_never_upload(self):
        for path in ["private/evidence.json", "news/a/../../secrets.env"]:
            with self.assertRaises(ValueError):
                self.archive._file(path)
        path = "news/a/index.html"
        self.artifact(path, b"<p>A</p>")
        target = self.publisher.public / path
        target.unlink()
        target.symlink_to(self.root / "not-public")
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.archive.sync()
        target.unlink()
        self.artifact(path, b"x" * 65530 + b"fake-credential-123")
        with self.assertRaisesRegex(ValueError, "Credential"):
            self.archive.sync()
        self.assertEqual(self.s3.puts, [])

    def test_every_public_object_and_manifest_must_be_readable_without_credentials(self):
        self.artifact("news/a/index.html", b"<p>A</p>")
        self.archive.sync()
        # The object, its shard and the root are each checked once stored, where the Worker reads them.
        self.assertEqual([key.split("/")[-2] for key in self.s3.puts], ["objects", "manifests", "manifests"])
        self.assertEqual(self.unsigned, [(self.archive.cfg["public_origin"] + key.removeprefix(self.prefix), True)
                                         for key in self.s3.puts])
        # A bucket policy can deny anonymous reads of a new object, and a later change those of one
        # already stored. Neither is recorded as uploaded, so the next run checks it again.
        sha = self.artifact("news/b/index.html", b"<p>B</p>")
        self.generation(2)
        self.exposure = "is not publicly readable"
        for attempt in ("new", "already stored"):
            with self.assertRaisesRegex(ValueError, f"Public archive object {self.prefix}objects/{sha} is not publicly readable"):
                self.archive.sync()
        self.assertEqual(self.s3.puts.count(self.prefix + "objects/" + sha), 1)
        self.exposure = None
        self.assertTrue(self.archive.sync()["ready"])

    def test_an_exposed_private_object_that_cannot_be_deleted_is_named_in_the_error(self):
        path = self.root / "private.gz"
        path.write_bytes(b"private evidence")
        key = self.archive.private_prefix + "evidence/exposed.json.gz"
        self.exposure = "was publicly readable"
        def failing_delete(Bucket, Key):
            raise RuntimeError("cleanup unavailable")
        self.s3.delete_object = failing_delete
        with self.assertRaisesRegex(ValueError, "exposed.json.gz was publicly readable, and deleting it failed: .*cleanup unavailable"):
            self.archive.put_private(path, key, hashlib.sha256(b"private evidence").hexdigest())

    def test_private_archives_are_immutable_and_verified(self):
        path = self.root / "private.gz"
        path.write_bytes(b"private evidence")
        key = self.archive.private_prefix + "evidence/example.json.gz"
        receipt = self.archive.put_private(path, key, hashlib.sha256(b"private evidence").hexdigest())
        self.assertEqual(self.s3.objects[key]["acl"], "private")
        self.assertEqual(self.unsigned, [(self.archive.cfg["endpoint_url"].rstrip("/") + "/" + self.archive.bucket + "/" + key,)])
        self.assertEqual(self.archive.put_private(path, key, receipt["sha256"]), receipt)
        # A private ACL is no proof once a bucket policy grants reads: the unsigned
        # request decides. A new object is removed; an existing one may back references.
        self.exposure = "was publicly readable"
        for target in (key, self.archive.private_prefix + "evidence/new.json.gz"):
            with self.assertRaisesRegex(ValueError, "publicly readable"):
                self.archive.put_private(path, target, receipt["sha256"])
        self.assertEqual(set(self.s3.objects), {key})
        self.exposure = None
        path.write_bytes(b"different evidence")
        with self.assertRaisesRegex(ValueError, "immutable"):
            self.archive.put_private(path, key, hashlib.sha256(b"different evidence").hexdigest())
        self.assertEqual(self.s3.puts.count(key), 1)

    def test_the_snapshot_refuses_a_ledger_that_static_publication_refuses(self):
        # The core keeps the old app.js in its ledger until every article is rebuilt without it.
        self.artifact("app.js", b"legacy")
        result = self.archive.sync()
        with self.assertRaisesRegex(ValueError, "Unexpected public file: app.js"):
            self.archive.snapshot(self.root / "script", result["generation"])
        self.db.execute("DELETE FROM public_artifacts WHERE path IN ('app.js','_headers')")
        self.db.commit()
        with self.assertRaisesRegex(ValueError, "Missing required public file: _headers"):
            self.archive.snapshot(self.root / "headers", result["generation"])

    def test_an_asset_the_application_adds_ships_in_the_bundle(self):
        with patch("onionflash.render.ASSET_FILES", ("style.css", "mark.svg", "favicon.svg")):
            self.artifact("favicon.svg", b"<svg/>")
            result = self.archive.sync()
            self.assertTrue(result["ready"])
            self.assertIn("favicon.svg", self.archive.snapshot(self.root / "hot", result["generation"]))

    def test_transient_storage_failures_use_the_publisher_backoff(self):
        self.s3.fail_puts = 1
        self.artifact("news/a/index.html", b"<p>A</p>")
        self.assertTrue(self.archive.sync()["ready"])
        self.assertEqual(self.waits, [0])

    def test_destination_change_uploads_every_object_shard_and_root_again(self):
        path = "news/a/index.html"
        sha = self.artifact(path, b"<p>A</p>")
        first = self.archive.sync()
        self.prefix = self.archive.cfg["public_prefix"] = "newproject/public/"
        second = self.archive.sync()
        self.assertTrue(second["ready"])
        self.assertEqual(second["manifest_sha256"], first["manifest_sha256"])  # a root names no destination
        self.assertEqual(self.lookup(second, path)["sha256"], sha)
        self.assertIn(self.prefix + "objects/" + sha, self.s3.objects)
        self.assertTrue(any(key.startswith("onionflash/public/objects/") for key in self.s3.objects))

    def test_oversized_artifact_raises_instead_of_waiting_forever(self):
        self.artifact("news/a/index.html", b"x" * 2048)
        with patch.object(archive_store, "MAX_OBJECT_BYTES", 1024), self.assertRaisesRegex(ValueError, "size"):
            self.archive.sync()

    def test_validate_config_rejects_missing_or_non_string_region_and_credential_names(self):
        base = dict(self.archive.cfg)
        for key, bad, pattern in [
            ("region", "", "region"),
            ("region", None, "region"),
            ("access_key_env", "bad-var", "credential variable"),
            ("secret_key_env", 123, "credential variable"),
        ]:
            with self.subTest(key=key, bad=bad), self.assertRaisesRegex(ValueError, pattern):
                archive_store.validate_config({**base, key: bad})

    @unittest.skipUnless(shutil.which("node"), "the Worker contract needs node")
    def test_the_worker_serves_the_archive_this_store_writes(self):
        bodies = {"news/a/index.html": b"<p>Story</p>", "news/a/story.json": b'{"id":"a"}'}
        for path, body in bodies.items():
            self.artifact(path, body)
        result = self.archive.sync()
        urls = {path: "/onionflash/" + path.removesuffix("index.html") for path in bodies}
        state = {"worker": (Path(__file__).parents[1] / "worker.mjs").as_uri(), "paths": list(urls.values()),
                 "env": {"BASE_PATH": "/onionflash", **self.archive.worker_vars(result["manifest_sha256"])},
                 "objects": {key.removeprefix(self.prefix): {"body": obj["body"].hex(), "sha256": obj["metadata"]["sha256"]}
                             for key, obj in self.s3.objects.items()}}
        served = json.loads(subprocess.run([shutil.which("node"), "--input-type=module", "-e", WORKER], input=json.dumps(state),
                                           capture_output=True, text=True, check=True).stdout)
        for path, body in bodies.items():
            etag = '"' + hashlib.sha256(body).hexdigest() + '"'
            self.assertEqual(served["GET " + urls[path]], [200, etag, body.hex()])
            self.assertEqual(served["HEAD " + urls[path]], [200, etag, ""])


if __name__ == "__main__":
    unittest.main()
