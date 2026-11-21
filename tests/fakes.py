"""Test doubles and base fixtures shared by the deployment tests."""
import base64, hashlib, io, json, os, sqlite3, tempfile, unittest
from contextlib import ExitStack, closing, contextmanager, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError, EndpointConnectionError
from onionflash.store import set_meta
import publish

OLD, NEW, OTHER = ("11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333")
ENV = {
    "CLOUDFLARE_ACCOUNT_ID": "a" * 32, "CLOUDFLARE_API_TOKEN": "unit-test-not-a-real-secret",
    "CLOUDFLARE_ZONE_TOKEN": "zone-test-not-a-real-secret", "CLOUDFLARE_ZONE_ID": "b" * 32,
    "DO_SPACES_KEY": "spaces-test-not-a-real-key", "DO_SPACES_SECRET": "spaces-test-not-a-real-secret",
}
_AUTO = object()


def receipt(version, previous=None):
    """Build a synthetic publication receipt dictionary for version."""
    return {"version_id": version, "previous_version_id": previous, "content_hash": version,
            "published_at": "2026-11-09T10:00:00Z", "manifest": {"index.html": version}}


class FakeS3:
    """In-memory S3-compatible client stub recording uploads, heads, ACLs, and deletions."""

    def __init__(self, *, cleanup_error=False, retention_error=False, listed=()):
        self.objects, self.puts, self.heads, self.uploads, self.deleted = {}, [], [], [], []
        self.fail_puts, self.on_put = 0, None
        self.cleanup_error, self.retention_error, self.listed = cleanup_error, retention_error, list(listed)

    def head_object(self, Bucket, Key):
        """Return stored object length, metadata, and MD5 ETag, or raise a 404 ClientError."""
        self.heads.append(Key)
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        obj = self.objects[Key]
        return {"ContentLength": len(obj["body"]), "Metadata": obj["metadata"], "ETag": '"' + hashlib.md5(obj["body"]).hexdigest() + '"'}

    def put_object(self, Bucket, Key, Body, ContentLength=None, ContentMD5=None, ContentType="application/octet-stream", ACL="private", CacheControl=None, Metadata=None):
        """Store uploaded bytes and ACL in memory after validating optional length and MD5 headers."""
        if self.fail_puts:
            self.fail_puts -= 1
            raise EndpointConnectionError(endpoint_url="https://storage.example")
        body = Body.read()
        if ContentLength is not None:
            assert len(body) == ContentLength
        if ContentMD5 is not None:
            assert base64.b64encode(hashlib.md5(body).digest()).decode() == ContentMD5
        self.objects[Key] = {"body": body, "metadata": Metadata or {}, "acl": ACL}
        self.puts.append(Key)
        self.uploads.append((Bucket, Key, body, ACL))
        if self.on_put:
            self.on_put(Key)

    def get_object(self, Bucket, Key):
        """Return a readable BytesIO body stream for a stored object."""
        return {"Body": io.BytesIO(self.objects[Key]["body"])}

    def delete_object(self, Bucket, Key):
        """Record deletion of Key and remove it unless cleanup_error is configured."""
        self.deleted.append((Bucket, Key))
        if self.cleanup_error:
            raise RuntimeError("cleanup unavailable")
        self.objects.pop(Key, None)

    def get_object_acl(self, Bucket, Key):
        """Return canonical owner grants plus public-read group grant when applicable."""
        grants = [{"Grantee": {"Type": "CanonicalUser", "ID": "owner"}, "Permission": "FULL_CONTROL"}]
        if self.objects[Key]["acl"] == "public-read":
            grants.append({"Grantee": {"Type": "Group", "URI": "http://acs.amazonaws.com/groups/global/AllUsers"}, "Permission": "READ"})
        return {"Owner": {"ID": "owner"}, "Grants": grants}

    def get_paginator(self, name):
        """Return self as a single-page list_objects_v2 paginator stub."""
        return self

    def paginate(self, **kwargs):
        """Yield one page containing the configured listed objects or raise on retention_error."""
        if self.retention_error:
            raise RuntimeError("retention listing unavailable")
        return [{"Contents": self.listed}]


def seed_public_site(public_dir, db_path=None):
    """Write required baseline public files into public_dir and optionally seed public_artifacts."""
    public_dir = Path(public_dir)
    public_dir.mkdir(parents=True, exist_ok=True)
    bodies = {}
    for name in publish.public_files():
        body = {"feed.xml": b"<rss version='2.0'><channel><title>News</title></channel></rss>",
                "feed.json": b'{"items":[]}'}.get(name, b"safe generated content")
        target = public_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        bodies[name] = body
    if db_path is not None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(db_path)) as db, db:
            db.executescript(
                "CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);"
                "CREATE TABLE IF NOT EXISTS public_artifacts("
                "path TEXT PRIMARY KEY,sha256 TEXT,size INTEGER,content_type TEXT,updated_at TEXT,uploaded_sha256 TEXT);"
            )
            set_meta(db, publication_generation="1", publication_build_pending="0")
            for name, body in bodies.items():
                db.execute("INSERT OR REPLACE INTO public_artifacts VALUES(?,?,?,'text/plain','2026-11-10T00:00:00Z',NULL)",
                           (name, hashlib.sha256(body).hexdigest(), len(body)))
    return public_dir


def app_toml(base_url="https://example.com/onionflash/", database="state/news.sqlite3", output_dir="build/public", extra=""):
    """Render a minimal onionflash.toml configuration string for tests."""
    return (f'[site]\nbase_url = "{base_url}"\n[storage]\ndatabase = "{database}"\n'
            f'output_dir = "{output_dir}"\n{extra}[[sources]]\nid = "test"\nurl = "https://example.org/feed.xml"\n')


def init_deploy_dir(test, *, env=ENV, clear_env=False, output_dir="build/public", extra=""):
    """Create a temporary deployment directory with patched environment and config files."""
    temporary = tempfile.TemporaryDirectory()
    test.addCleanup(temporary.cleanup)
    directory = Path(temporary.name).resolve()
    patched = patch.dict(os.environ, env, clear=clear_env)
    patched.start()
    test.addCleanup(patched.stop)
    (directory / "deployment.toml").write_text((publish.ROOT / "deployment.example.toml").read_text())
    (directory / "onionflash.toml").write_text(app_toml(output_dir=output_dir, extra=extra))
    return directory


class DeployTestCase(unittest.TestCase):
    """Base test case providing a temporary Publisher, S3 archive stub, and Cloudflare mocks."""

    OUTPUT_DIR = "build/public"

    def setUp(self):
        """Initialize an isolated deployment directory and Publisher instance."""
        self.directory = self.root = init_deploy_dir(self, output_dir=self.OUTPUT_DIR)
        self.p = self.publisher = publish.Publisher(self.directory / "deployment.toml")

    def _patch(self, name, **kwargs):
        patcher = patch.object(self.p, name, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    @contextmanager
    def db(self):
        """Open the publisher SQLite database with row mappings and automatic commit."""
        with closing(sqlite3.connect(self.p.database)) as db, db:
            db.row_factory = sqlite3.Row
            yield db

    def enable_archive(self, *, retention=False):
        """Enable archive storage backed by FakeS3 and stub anonymous access probes."""
        from archive_store import ArchiveStore
        self.p.cfg["archive"]["enabled"], self.p.cfg["retention"]["enabled"] = True, retention
        self.s3 = FakeS3()
        self.waits, self.unsigned, self.exposure = [], [], None
        self.p.delay = self.waits.append
        self.p._archive = self.archive = ArchiveStore(self.p, client=self.s3)
        self.addCleanup(self.p.close)
        self._patch("unsigned_read_problem", side_effect=lambda *request: self.unsigned.append(request) or self.exposure)
        return self.archive

    def fake_cloudflare(self, live, *, recorded=_AUTO, failing_check=None):
        """Stub Wrangler commands, Worker version checks, and live/preview verification."""
        pub_path = self.p.state / "publication.json"
        if recorded is _AUTO and live is None:
            pub_path.unlink(missing_ok=True)
        elif recorded is not False:
            publish.atomic_json(pub_path, receipt(live) if recorded is _AUTO else recorded)
        self.calls, self.live = [], [live]
        preview = "https://candidate-news.example.workers.dev"
        def command(arguments, metadata=None):
            """Record Wrangler CLI invocations and simulate version upload or initial deploy."""
            self.calls.append(" ".join(arguments[:2]))
            if arguments[:2] == ["versions", "upload"]:
                Path(metadata).write_text(json.dumps({"type": "version-upload", "version_id": NEW, "preview_url": preview}) + "\n")
            elif arguments[0] == "deploy":
                self.live[0] = NEW
            return ""
        def verify(url, manifest, archive=None):
            """Record preview or live verification and fail when matching failing_check."""
            self.calls.append("verify " + ("preview" if url.startswith(preview) else "live"))
            if failing_check and self.calls[-1] == "verify " + failing_check:
                raise ValueError("check failed")
        self._patch("command", side_effect=command)
        self._patch("activate", side_effect=lambda version: (self.calls.append("activate " + version), self.live.__setitem__(0, version)))
        self._patch("verify", side_effect=verify)
        self._patch("current_version", side_effect=lambda: self.live[0])
        self._patch("origin_url", return_value="https://news.example.workers.dev/onionflash")
        self._patch("app_version", return_value="test")

    def seed_public(self):
        """Populate baseline public output files and ledger rows for self.p."""
        return seed_public_site(self.p.public, self.p.database)

    def put_artifact(self, path, body):
        """Write body to public/path, upsert its SHA-256 into public_artifacts, and return the digest."""
        target = self.p.public / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        sha = hashlib.sha256(body).hexdigest()
        with self.db() as db:
            db.execute(
                "INSERT INTO public_artifacts VALUES(?,?,?,'text/plain','2026-11-10T00:00:00Z',NULL) "
                "ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256,size=excluded.size",
                (path, sha, len(body)),
            )
        return sha

    def add_article(self, story_id):
        """Write a synthetic article permalink page and register it in public_artifacts."""
        return self.put_artifact(f"news/{story_id}/index.html", f"<p>{story_id}</p>".encode())

    def run_cli(self, *args, patch_publisher=True, fake_toolchain=False):
        """Invoke publish.main with sys.argv set to args and return (exit_code, stdout)."""
        out = io.StringIO()
        with ExitStack() as stack:
            for ctx in (patch("sys.argv", ["publish.py", *args]), redirect_stdout(out),
                        *([patch("publish.Publisher", return_value=self.p)] if patch_publisher else []),
                        *([patch.object(self.p, "node", Path(__file__)), patch.object(self.p, "wrangler", Path(__file__))] if fake_toolchain else [])):
                stack.enter_context(ctx)
            return publish.main(), out.getvalue()
