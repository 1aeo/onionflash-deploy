"""Unit and Worker-contract tests for content-addressed archive storage."""
import gzip, hashlib, json, shutil, subprocess, unittest
from pathlib import Path
from unittest.mock import patch

import archive_store
from onionflash.store import set_meta
from publish import ROOT, public_files
from tests.fakes import DeployTestCase, ENV

NODE = str(bundled) if (bundled := ROOT / "node_modules/node/bin/node").is_file() else shutil.which("node")

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


class ArchiveTests(DeployTestCase):
    """Tests for ArchiveStore object uploads, manifest generation, and privacy checks."""

    OUTPUT_DIR = "public"

    def setUp(self):
        """Enable archive storage and seed baseline public output artifacts."""
        super().setUp()
        self.enable_archive()
        self.prefix = self.archive.prefix
        self.seed_public()

    def meta(self, **values):
        """Update publication and archive metadata entries in the test database."""
        with self.db() as db:
            set_meta(db, **{k: str(v) for k, v in values.items()})

    def lookup(self, result, relative):
        """Decompress the uploaded archive manifest for result and return relative's entry."""
        doc = json.loads(gzip.decompress(self.s3.objects[f"{self.prefix}manifests/{result['manifest_sha256']}.json.gz"]["body"]))
        self.assertEqual(doc["format"], "onionflash-archive-manifest-v1")
        return doc["entries"][relative]

    def test_sync_uploads_permalinks_to_single_gzipped_manifest_and_snapshots_hot_files(self):
        """Upload permalink objects to a single gzipped manifest while keeping hot files local."""
        shas = {path: self.put_artifact(path, path.encode()) for path in (
            "news/a/index.html", "news/a/story.json", "page/2/index.html", "topics/security/index.html", "tags/security.xml")}
        result = self.archive.sync()
        self.assertTrue(result["ready"])
        self.assertEqual(result["uploaded"], 2)
        self.assertTrue(all("/objects/" in key or key.endswith(".json.gz") for key in self.s3.objects))
        for path in ("news/a/index.html", "news/a/story.json"):
            entry = self.lookup(result, path)
            self.assertEqual((entry["sha256"], entry["key"]), (shas[path], f"objects/{shas[path]}"))
        hot = self.root / "hot"
        manifest = self.archive.snapshot(hot, result["generation"])
        self.assertEqual({p.relative_to(hot).as_posix() for p in hot.rglob("*") if p.is_file()},
                         public_files() | {"page/2/index.html", "topics/security/index.html", "tags/security.xml"})
        self.assertEqual(set(manifest), {*public_files(), *shas} - {"news/a/story.json"})
        self.assertEqual(manifest["news/a/index.html"], shas["news/a/index.html"])

    def test_repeat_sync_and_a_hot_change_make_no_storage_request(self):
        """Skip S3 requests when syncing an unchanged archive or updating non-article hot files."""
        self.put_artifact("news/a/index.html", b"<p>A</p>")
        first = self.archive.sync()
        requests = (list(self.s3.heads), list(self.s3.puts), list(self.unsigned))
        repeated = self.archive.sync()
        self.put_artifact("index.html", b"new source availability times")
        self.meta(publication_generation=2)
        changed = self.archive.sync()
        self.assertTrue(changed["ready"])
        self.assertEqual({repeated["manifest_sha256"], changed["manifest_sha256"]}, {first["manifest_sha256"]})
        self.assertEqual((self.s3.heads, self.s3.puts, self.unsigned), requests)

    def test_correction_and_interrupted_uploads_preserve_immutable_objects_and_resume(self):
        """Retain prior content-addressed objects on story edits and resume interrupted manifests."""
        path = "news/example/index.html"
        old_sha = self.put_artifact(path, b"old valid story")
        old = self.archive.sync()
        new_sha = self.put_artifact(path, b"corrected story")
        self.meta(publication_generation=2)
        new = self.archive.sync()
        self.assertNotEqual(old["manifest_sha256"], new["manifest_sha256"])
        self.assertEqual((self.lookup(old, path)["sha256"], self.lookup(new, path)["sha256"]), (old_sha, new_sha))
        self.assertIn(self.prefix + "objects/" + old_sha, self.s3.objects)
        self.assertTrue(all(obj["acl"] == "public-read" for obj in self.s3.objects.values()))

        for stored in (False, True):
            step_sha = self.put_artifact(path, f"corrected {stored}".encode())
            self.meta(publication_generation=3 + stored)
            manifests = []
            def interrupt(key):
                """Simulate an interrupted manifest upload before or after S3 persistence."""
                if key.endswith(".json.gz"):
                    manifests.append(key)
                    if not stored:
                        del self.s3.objects[key]
                    raise RuntimeError("interrupted")
            self.s3.on_put = interrupt
            with self.subTest(stored=stored), self.assertRaisesRegex(RuntimeError, "interrupted"):
                self.archive.sync()
            self.s3.on_put = None
            with self.assertRaisesRegex(ValueError, "not ready"):
                self.archive.snapshot(self.root / f"hot-{stored}", str(3 + stored))
            heads, puts = len(self.s3.heads), len(self.s3.puts)
            current = self.archive.sync()
            self.assertTrue(current["ready"])
            self.assertEqual(manifests, [f"{self.prefix}manifests/{current['manifest_sha256']}.json.gz"])
            self.assertEqual(self.s3.heads[heads:], manifests)
            self.assertEqual(self.s3.puts[puts:], [] if stored else manifests)
            self.assertEqual(self.lookup(current, path)["sha256"], step_sha)

    def test_crash_tampered_storage_and_mid_upload_rebuild_guards(self):
        """Detect tampered remote objects and defer readiness when a rebuild occurs mid-sync."""
        sha = self.put_artifact("news/a/index.html", b"<p>A</p>")
        def crash(key):
            """Abort once immediately after the first object upload."""
            self.s3.on_put = None
            raise RuntimeError("crash after the object upload")
        self.s3.on_put = crash
        with self.assertRaisesRegex(RuntimeError, "crash"):
            self.archive.sync()
        self.assertTrue(self.archive.sync()["ready"])
        key = self.prefix + "objects/" + sha
        self.assertEqual(self.s3.puts.count(key), 1)

        with self.db() as db:
            db.execute("UPDATE public_artifacts SET uploaded_sha256=NULL WHERE path='news/a/index.html'")
        self.s3.objects[key]["body"] = b"<p>B</p>"
        with self.assertRaisesRegex(ValueError, "changed outside this publisher"):
            self.archive.sync()
        self.s3.objects[key]["body"] = b"<p>A</p>"

        self.put_artifact("news/b/index.html", b"<p>B1</p>")
        self.meta(publication_generation=2)
        def rebuild(k):
            """Simulate a concurrent core rebuild advancing publication_generation mid-upload."""
            self.s3.on_put = None
            self.put_artifact("news/b/index.html", b"<p>B2</p>")
            self.meta(publication_generation=3)
        self.s3.on_put = rebuild
        mid = self.archive.sync()
        self.assertFalse(mid["ready"])
        self.assertIsNone(mid["manifest_sha256"])
        self.assertTrue(self.archive.sync()["ready"])

    def test_incomplete_builds_budgets_deadlines_and_size_limits(self):
        """Enforce pending-build guards, upload count/byte/time budgets, and manifest limits."""
        result = self.archive.sync()
        puts = list(self.s3.puts)
        self.meta(publication_build_pending=1)
        for attempt in (self.archive.sync, lambda: self.archive.snapshot(self.root / "hot", result["generation"])):
            with self.assertRaisesRegex(ValueError, "not completed"):
                attempt()
        self.assertEqual(self.s3.puts, puts)
        self.meta(publication_build_pending=0)

        for budget, value in (("MAX_UPLOADS_PER_RUN", 1), ("MAX_UPLOAD_BYTES_PER_RUN", 1500)):
            for name in "ab":
                self.put_artifact(f"news/{name}/index.html", (budget + name).encode().ljust(1000, b"."))
            with self.subTest(budget=budget), patch.object(archive_store, budget, value):
                held = self.archive.sync()
                self.assertEqual((held["ready"], held["uploaded"], held["pending"]), (False, 1, 1))
            self.assertTrue(self.archive.sync()["ready"])

        shas = [self.put_artifact(p, f"dl-{p}".encode()) for p in ("news/a/index.html", "news/b/index.html")]
        clock = [0.0]
        self.s3.on_put = lambda key: clock.__setitem__(0, 1e9)
        with patch.object(archive_store.time, "monotonic", lambda: clock[0]):
            held = self.archive.sync()
        self.assertEqual((held["ready"], held["budget_reached"], held["uploaded"], held["pending"]), (False, True, 1, 1))
        self.s3.on_put = None
        resumed = self.archive.sync()
        self.assertTrue(resumed["ready"])
        self.assertEqual([self.lookup(resumed, p)["sha256"] for p in ("news/a/index.html", "news/b/index.html")], shas)

        self.put_artifact("news/big/index.html", b"x" * 2048)
        with patch.object(archive_store, "MAX_OBJECT_BYTES", 1024), self.assertRaisesRegex(ValueError, "size"):
            self.archive.sync()
        self.put_artifact("news/big/index.html", b"x")
        with patch.object(archive_store, "MAX_MANIFEST_RAW_BYTES", 64), self.assertRaisesRegex(ValueError, "decompressed size limit"):
            self.archive.sync()
        migrated = self.archive.sync()
        self.assertTrue(migrated["ready"])
        with self.db() as db:
            db.execute("DELETE FROM metadata WHERE key='archive_manifest_format'")
        self.meta(archive_manifest_sha256="0" * 64)
        puts_before = list(self.s3.puts)
        del self.s3.objects[f"{self.prefix}manifests/{migrated['manifest_sha256']}.json.gz"]
        upgraded = self.archive.sync()
        self.assertEqual(upgraded["manifest_sha256"], migrated["manifest_sha256"])
        self.assertEqual(self.s3.puts[len(puts_before):], [f"{self.prefix}manifests/{migrated['manifest_sha256']}.json.gz"])

    def test_unknown_paths_symlinks_credentials_and_ledger_validation(self):
        """Reject disallowed paths, symlinks, leaked secrets, and missing required hot files."""
        for path in ("private/evidence.json", "news/a/../../secrets.env"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.archive._file(path)
        path = "news/a/index.html"
        self.put_artifact(path, b"<p>A</p>")
        target = self.publisher.public / path
        target.unlink()
        target.symlink_to(self.root / "not-public")
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.archive.sync()
        target.unlink()
        self.put_artifact(path, b"x" * 65530 + ENV["DO_SPACES_SECRET"].encode())
        with self.assertRaisesRegex(ValueError, "Credential"):
            self.archive.sync()
        self.assertEqual(self.s3.puts, [])

        self.put_artifact(path, b"<p>clean</p>")
        self.put_artifact("app.js", b"legacy")
        result = self.archive.sync()
        with self.assertRaisesRegex(ValueError, "Unexpected public file: app.js"):
            self.archive.snapshot(self.root / "script", result["generation"])
        with self.db() as db:
            db.execute("DELETE FROM public_artifacts WHERE path IN ('app.js','_headers')")
        with self.assertRaisesRegex(ValueError, "Missing required public file: _headers"):
            self.archive.snapshot(self.root / "headers", result["generation"])

    def test_public_readability_private_immutability_backoff_destination_and_config(self):
        """Verify anonymous readability/privacy probes, retry backoff, prefix resets, and config checks."""
        self.s3.fail_puts = 1
        self.put_artifact("news/a/index.html", b"<p>A</p>")
        self.assertTrue(self.archive.sync()["ready"])
        self.assertEqual(self.waits, [0])
        self.assertEqual([key.split("/")[-2] for key in self.s3.puts], ["objects", "manifests"])
        self.assertEqual(self.unsigned, [(self.archive.cfg["public_origin"] + key.removeprefix(self.prefix), True) for key in self.s3.puts])

        sha = self.put_artifact("news/b/index.html", b"<p>B</p>")
        self.meta(publication_generation=2)
        self.exposure = "is not publicly readable"
        for _ in ("new", "already stored"):
            with self.assertRaisesRegex(ValueError, f"Public archive object {self.prefix}objects/{sha} is not publicly readable"):
                self.archive.sync()
        self.assertEqual(self.s3.puts.count(self.prefix + "objects/" + sha), 1)
        self.exposure = None
        first = self.archive.sync()
        self.assertTrue(first["ready"])

        self.prefix = self.archive.cfg["public_prefix"] = "newproject/public/"
        second = self.archive.sync()
        self.assertEqual(second["manifest_sha256"], first["manifest_sha256"])
        self.assertIn(self.prefix + "objects/" + sha, self.s3.objects)

        from onionflash import render
        self.assertTrue(all(isinstance(name, str) and name for name in render.ASSET_FILES))
        self.assertLessEqual(set(render.ASSET_FILES), public_files())
        patched_assets = (
            {**render.ASSET_FILES, "favicon.svg": "image/svg+xml"}
            if isinstance(render.ASSET_FILES, dict)
            else (*render.ASSET_FILES, "favicon.svg")
        )
        with patch("onionflash.render.ASSET_FILES", patched_assets):
            self.assertIn("favicon.svg", public_files())
            with self.assertRaisesRegex(ValueError, "Missing required public file: favicon.svg"):
                self.archive.snapshot(self.root / "hot-asset", second["generation"])

        path = self.root / "private.gz"
        path.write_bytes(b"private evidence")
        priv_sha = hashlib.sha256(b"private evidence").hexdigest()
        key = self.archive.private_prefix + "evidence/example.json.gz"
        receipt = self.archive.put_private(path, key, priv_sha)
        self.assertEqual(self.archive.put_private(path, key, priv_sha), receipt)
        self.exposure = "was publicly readable"
        for target in (key, self.archive.private_prefix + "evidence/new.json.gz"):
            with self.assertRaisesRegex(ValueError, "publicly readable"):
                self.archive.put_private(path, target, priv_sha)
        self.assertNotIn(self.archive.private_prefix + "evidence/new.json.gz", self.s3.objects)
        self.s3.cleanup_error = True
        with self.assertRaisesRegex(ValueError, "exposed.json.gz was publicly readable, and deleting it failed: .*cleanup unavailable"):
            self.archive.put_private(path, self.archive.private_prefix + "evidence/exposed.json.gz", priv_sha)
        self.s3.cleanup_error = False
        self.s3.objects.pop(self.archive.private_prefix + "evidence/exposed.json.gz", None)
        self.exposure = None
        path.write_bytes(b"different evidence")
        with self.assertRaisesRegex(ValueError, "immutable"):
            self.archive.put_private(path, key, hashlib.sha256(b"different evidence").hexdigest())

        base = dict(self.archive.cfg, public_prefix="onionflash/public/")
        bad_urls = ("https://@example.com/onionflash/public/", "https://:@example.com/onionflash/public/",
                    "https://example.com:abc/onionflash/public/", "https://example.com:99999/onionflash/public/",
                    "https://example.com:0/onionflash/public/", "https://example.com/a b/", "https://example.com\\onionflash/public/")
        for bad_url in bad_urls:
            with self.subTest(bad_url=bad_url), self.assertRaisesRegex(ValueError, "clean HTTPS URL"):
                archive_store.validate_config({**base, "public_origin": bad_url})
        for k, bad, pattern in [("region", "", "region"), ("region", None, "region"),
                                ("access_key_env", "bad-var", "credential variable"), ("secret_key_env", 123, "credential variable"),
                                ("retired_key", 1, "Unknown archive settings")]:
            with self.subTest(key=k), self.assertRaisesRegex(ValueError, pattern):
                archive_store.validate_config({**base, k: bad})

    def test_put_private_deletes_unverified_or_budget_expired_uploads(self):
        """Remove newly uploaded private objects on verification, ACL, or deadline failures."""
        path = self.root / "private-budget.gz"
        path.write_bytes(b"private bundle")
        priv_sha = hashlib.sha256(b"private bundle").hexdigest()
        clock = [0.0]
        budget_key = self.archive.private_prefix + "evidence/budget-expired.json.gz"
        self.s3.deleted.clear()
        with patch.object(archive_store.time, "monotonic", lambda: clock[0]), \
                patch.object(self.p, "unsigned_read_problem", side_effect=lambda *req: clock.__setitem__(0, 1e9) or "was publicly readable"), \
                self.archive.budget(10), self.assertRaisesRegex(ValueError, "publicly readable"):
            self.archive.put_private(path, budget_key, priv_sha)
        self.assertEqual(self.s3.deleted, [(self.archive.bucket, budget_key)])
        self.assertNotIn(budget_key, self.s3.objects)

        verify_budget_key = self.archive.private_prefix + "evidence/verify-budget.json.gz"
        clock[0] = 0.0
        self.s3.on_put = lambda k: clock.__setitem__(0, 1e9)
        self.s3.deleted.clear()
        with patch.object(archive_store.time, "monotonic", lambda: clock[0]), \
                self.archive.budget(10), self.assertRaises(archive_store.ArchiveBudgetReached):
            self.archive.put_private(path, verify_budget_key, priv_sha)
        self.assertEqual(self.s3.deleted, [(self.archive.bucket, verify_budget_key)])
        self.assertNotIn(verify_budget_key, self.s3.objects)

        acl_key = self.archive.private_prefix + "evidence/acl-cleanup-err.json.gz"
        self.s3.on_put = lambda k: self.s3.objects[k].update(acl="public-read")
        self.s3.cleanup_error = True
        with self.assertRaisesRegex(RuntimeError, "acl-cleanup-err.json.gz could not be removed: .*Unexpected archive object ACL.*cleanup unavailable") as acl_err:
            self.archive.put_private(path, acl_key, priv_sha)
        self.assertIsInstance(acl_err.exception.__cause__, ValueError)

    @unittest.skipUnless(NODE, "the Worker contract needs node")
    def test_the_worker_serves_the_archive_this_store_writes(self):
        """Confirm worker.mjs serves GET and HEAD requests from manifests and objects written by sync."""
        bodies = {"news/a/index.html": b"<p>Story</p>", "news/a/story.json": b'{"id":"a"}'}
        for path, body in bodies.items():
            self.put_artifact(path, body)
        result = self.archive.sync()
        urls = {path: "/onionflash/" + path.removesuffix("index.html") for path in bodies}
        state = {
            "worker": (Path(__file__).parents[1] / "worker.mjs").as_uri(), "paths": list(urls.values()),
            "env": {"BASE_PATH": "/onionflash", **self.archive.worker_vars(result["manifest_sha256"])},
            "objects": {key.removeprefix(self.prefix): {"body": obj["body"].hex(), "sha256": obj["metadata"]["sha256"]}
                        for key, obj in self.s3.objects.items()},
        }
        served = json.loads(subprocess.run([NODE, "--input-type=module", "-e", WORKER],
                                           input=json.dumps(state), capture_output=True, text=True, check=True).stdout)
        for path, body in bodies.items():
            etag = '"' + hashlib.sha256(body).hexdigest() + '"'
            self.assertEqual(served["GET " + urls[path]], [200, etag, body.hex()])
            self.assertEqual(served["HEAD " + urls[path]], [200, etag, ""])


if __name__ == "__main__":
    unittest.main()
