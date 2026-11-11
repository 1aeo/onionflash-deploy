"""Failure-path checks for owned deployment transitions and private backups."""
from datetime import datetime, timezone
import gzip
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import ANY, MagicMock, patch
from urllib.error import HTTPError, URLError

import publish


OLD = "11111111-1111-4111-8111-111111111111"
NEW = "22222222-2222-4222-8222-222222222222"
OTHER = "33333333-3333-4333-8333-333333333333"


def receipt(version, previous=None):
    return {"version_id": version, "previous_version_id": previous,
            "content_hash": version, "published_at": "2026-11-09T10:00:00Z",
            "manifest": {"index.html": version}}


class FakeSpaces:
    def __init__(self, *, cleanup_error=False, retention_error=False, listed=()):
        self.cleanup_error, self.retention_error = cleanup_error, retention_error
        self.objects, self.uploads, self.deleted, self.listed = {}, [], [], list(listed)

    def put_object(self, *, Bucket, Key, Body, ACL, ContentType):
        Body = Body.read()
        self.objects[Bucket, Key] = Body
        self.uploads.append((Bucket, Key, Body, ACL))

    def delete_object(self, *, Bucket, Key):
        self.deleted.append((Bucket, Key))
        if self.cleanup_error:
            raise RuntimeError("cleanup unavailable")
        self.objects.pop((Bucket, Key), None)

    def get_paginator(self, name):
        return self

    def paginate(self, **kwargs):
        if self.retention_error:
            raise RuntimeError("retention listing unavailable")
        return [{"Contents": self.listed}]


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name).resolve()
        environment = patch.dict(os.environ, {
            "CLOUDFLARE_ACCOUNT_ID": "a" * 32,
            "CLOUDFLARE_API_TOKEN": "test-cloudflare-secret",
            "CLOUDFLARE_ZONE_TOKEN": "test-zone-secret",
            "CLOUDFLARE_ZONE_ID": "b" * 32,
            "DO_SPACES_KEY": "test-spaces-key",
            "DO_SPACES_SECRET": "test-spaces-secret",
        })
        environment.start()
        self.addCleanup(environment.stop)
        (self.directory / "deployment.toml").write_text(
            (publish.ROOT / "deployment.example.toml").read_text())
        (self.directory / "onionflash.toml").write_text('''[site]
base_url = "https://example.com/onionflash/"
[storage]
database = "state/news.sqlite3"
output_dir = "build/public"
[[sources]]
id = "test"
url = "https://example.org/feed.xml"
''')
        self.p = publish.Publisher(self.directory / "deployment.toml")

    def seed_public(self):
        """Generated output and the core's ledger of it."""
        self.p.public.mkdir(parents=True)
        with closing(sqlite3.connect(self.p.database)) as db:
            db.executescript("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT);"
                             "CREATE TABLE public_artifacts(path TEXT PRIMARY KEY, sha256 TEXT);")
            for name in publish.public_files():
                body = b"generated public content"
                if name == "feed.xml":
                    body = b"<rss version='2.0'><channel><title>News</title></channel></rss>"
                elif name == "feed.json":
                    body = b'{"items":[]}'
                (self.p.public / name).write_bytes(body)
                db.execute("INSERT INTO public_artifacts VALUES(?,?)", (name, publish.hashlib.sha256(body).hexdigest()))
            db.commit()

    def fake_cloudflare(self, live, *, failing_check=None):
        """Patch wrangler, the API and checks; record calls in self.calls."""
        if live is not None and not (self.p.state / "publication.json").exists():
            publish.atomic_json(self.p.state / "publication.json", receipt(live))
        self.calls, self.live = [], [live]
        preview = "https://candidate-news.example.workers.dev"
        def command(arguments, metadata=None):
            self.calls.append(" ".join(arguments[:2]))
            if arguments[:2] == ["versions", "upload"]:
                Path(metadata).write_text(json.dumps({"type": "version-upload", "version_id": NEW, "preview_url": preview}) + "\n")
            elif arguments[0] == "deploy":
                self.live[0] = NEW
            return ""
        def activate(version, config):
            self.calls.append("activate " + version)
            self.live[0] = version
        def verify(url, manifest, archive=None):
            self.calls.append("verify " + ("preview" if url.startswith(preview) else "live"))
            if failing_check and self.calls[-1] == "verify " + failing_check:
                raise ValueError("check failed")
        for name, value in [("command", command), ("activate", activate), ("verify", verify),
                            ("current_version", lambda: self.live[0])]:
            patcher = patch.object(self.p, name, side_effect=value)
            patcher.start()
            self.addCleanup(patcher.stop)
        for name, value in [("origin_url", "https://news.example.workers.dev/onionflash"), ("app_version", "test")]:
            patcher = patch.object(self.p, name, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_publish_checks_the_preview_before_activating_and_records_the_receipt(self):
        self.seed_public()
        publish.atomic_json(self.p.state / "receipts" / (OTHER + ".json"), receipt(OTHER))
        self.fake_cloudflare(OLD)
        result = self.p.publish()
        self.assertEqual(self.calls, ["versions upload", "verify preview", "activate " + NEW, "verify live"])
        self.assertEqual((result["version_id"], result["previous_version_id"]), (NEW, OLD))
        self.assertEqual(publish.read_json(self.p.state / "publication.json")["version_id"], NEW)
        # Static receipts list every file, so only the live one and the rollback target are kept.
        self.assertEqual({path.name for path in (self.p.state / "receipts").iterdir()}, {OLD + ".json", NEW + ".json"})
        self.assertEqual(publish.read_json(self.p.state / "receipts" / (OLD + ".json")), receipt(OLD))

    def test_publication_never_drops_an_article_the_live_receipt_lists(self):
        self.seed_public()
        self.fake_cloudflare(OLD)
        gone = "news/gone/index.html"
        for previous in ({**receipt(OLD), "manifest": {gone: "a" * 64}}, {**receipt(OLD), "articles": [gone]}):
            with self.subTest(previous=previous):  # a static or a permanent-storage receipt
                publish.atomic_json(self.p.state / "publication.json", previous)
                with self.assertRaisesRegex(ValueError, "previously published article"):
                    self.p.publish()
                self.assertEqual(self.calls, [])

    def test_failed_live_check_rolls_back_and_fails_without_holding(self):
        self.seed_public()
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))
        self.fake_cloudflare(OLD, failing_check="live")
        with self.assertRaisesRegex(ValueError, "check failed"):
            self.p.publish()
        self.assertEqual(self.calls[-2:], ["verify live", "activate " + OLD])
        self.assertEqual(self.live[0], OLD)
        self.assertEqual(publish.read_json(self.p.state / "publication.json")["version_id"], OLD)
        self.assertFalse((self.p.state / "publication-hold.json").exists())

    def test_a_failed_restore_reports_both_failures_and_stops_the_next_run_for_inspection(self):
        self.seed_public()
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))
        self.fake_cloudflare(OLD, failing_check="live")
        def activate(version, config):
            if version == OLD:
                raise RuntimeError("restore unavailable")
            self.live[0] = version
        with patch.object(self.p, "activate", side_effect=activate):
            with self.assertRaisesRegex(RuntimeError, "check failed.*restore unavailable"):
                self.p.publish()
        current = publish.read_json(self.p.state / "publication.json")
        self.assertEqual(current["version_id"], OLD)
        self.assertNotIn("content_hash", current)
        with self.assertRaisesRegex(ValueError, rf"inspect before publishing \(live {NEW}, recorded {OLD}\)"):
            self.p.publish()

    def test_an_upload_without_a_reported_version_fails_clearly(self):
        self.seed_public()
        self.fake_cloudflare(OLD)
        with patch.object(self.p, "command", return_value=""):
            with self.assertRaisesRegex(RuntimeError, "did not report the uploaded version"):
                self.p.publish()

    def test_an_interrupted_publication_or_rollback_cannot_skip_the_next_upload(self):
        self.seed_public()
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))
        self.fake_cloudflare(OLD)
        with patch.object(self.p, "activate", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.p.publish()
        self.assertNotIn("content_hash", publish.read_json(self.p.state / "publication.json"))
        publish.atomic_json(self.p.state / "publication.json", receipt(NEW, OLD))
        self.live[0] = NEW
        with patch.object(self.p, "activate", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.p.rollback()
        self.assertNotIn("content_hash", publish.read_json(self.p.state / "publication.json"))
        self.assertTrue((self.p.state / "publication-hold.json").is_file())

    def test_failed_preview_check_never_activates(self):
        self.seed_public()
        self.fake_cloudflare(OLD, failing_check="preview")
        with self.assertRaisesRegex(ValueError, "check failed"):
            self.p.publish()
        self.assertFalse(any(call.startswith("activate") for call in self.calls))

    def test_a_new_worker_is_deployed_directly(self):
        self.seed_public()
        self.fake_cloudflare(None)
        receipt = self.p.publish()
        self.assertEqual(self.calls, ["deploy --config", "verify live"])
        self.assertEqual((receipt["version_id"], receipt["previous_version_id"]), (NEW, None))

    def test_a_new_worker_that_fails_its_live_check_is_recorded_and_the_next_run_uploads(self):
        self.seed_public()
        self.fake_cloudflare(None, failing_check="live")
        with self.assertRaisesRegex(ValueError, "check failed"):
            self.p.publish()
        self.assertEqual(publish.read_json(self.p.state / "publication.json"), {"version_id": NEW})
        self.fake_cloudflare(NEW)
        self.p.publish()
        self.assertEqual(self.calls, ["versions upload", "verify preview", "activate " + NEW, "verify live"])

    def test_a_failed_deploy_records_the_worker_it_created(self):
        self.seed_public()
        self.fake_cloudflare(None)
        def deploy(arguments, metadata=None):
            self.live[0] = NEW
            raise RuntimeError("deploy reported an error")
        with patch.object(self.p, "command", side_effect=deploy):
            with self.assertRaisesRegex(RuntimeError, "deploy reported"):
                self.p.publish()
        self.assertEqual(publish.read_json(self.p.state / "publication.json"), {"version_id": NEW})

    def test_a_failed_deploy_keeps_its_error_when_the_version_lookup_also_fails(self):
        self.seed_public()
        self.fake_cloudflare(None)
        lookups = iter([None])  # the check before deploying succeeds; the one after fails
        def current_version():
            for version in lookups:
                return version
            raise RuntimeError("version lookup failed")
        def deploy(arguments, metadata=None):
            raise RuntimeError("deploy reported an error")
        with patch.object(self.p, "command", side_effect=deploy), \
                patch.object(self.p, "current_version", side_effect=current_version):
            with self.assertRaisesRegex(RuntimeError, "deploy reported"):
                self.p.publish()

    def test_failed_check_does_not_roll_back_an_external_version(self):
        self.seed_public()
        self.fake_cloudflare(OLD)
        def verify(url, manifest, archive=None):
            if "candidate-" not in url:
                self.live[0] = OTHER
                raise ValueError("external deployment happened during verification")
        with patch.object(self.p, "verify", side_effect=verify):
            with self.assertRaisesRegex(RuntimeError, "external version retained"):
                self.p.publish()
        self.assertEqual(self.live[0], OTHER)
        self.assertEqual([call for call in self.calls if call.startswith("activate")], ["activate " + NEW])

    def test_changed_content_does_not_replace_an_external_version(self):
        self.seed_public()
        for recorded in (receipt(OLD), {}):  # {}: for example, a lost state directory
            with self.subTest(recorded=recorded.get("version_id")):
                publish.atomic_json(self.p.state / "publication.json", recorded)
                self.fake_cloudflare(OTHER)
                with self.assertRaisesRegex(ValueError, f"differs.*live {OTHER}, recorded {recorded.get('version_id')}"):
                    self.p.publish()
                self.assertEqual(self.calls, [])
                self.assertEqual(self.live[0], OTHER)

    def test_external_activation_during_preview_is_preserved(self):
        self.seed_public()
        self.fake_cloudflare(OLD)
        def verify(url, manifest, archive=None):
            self.live[0] = OTHER
        with patch.object(self.p, "verify", side_effect=verify):
            with self.assertRaisesRegex(ValueError, "changed during preview"):
                self.p.publish()
        self.assertFalse(any(call.startswith("activate") for call in self.calls))
        self.assertEqual(self.live[0], OTHER)

    def test_fallible_provenance_is_collected_before_activation(self):
        self.seed_public()
        self.fake_cloudflare(OLD)
        with patch.object(self.p, "app_version", side_effect=RuntimeError("metadata unavailable")):
            with self.assertRaisesRegex(RuntimeError, "metadata unavailable"):
                self.p.publish()
        self.assertEqual(self.calls, [])

    def test_rollback_restores_the_previous_receipt_and_forces_the_next_upload(self):
        old = dict(receipt(OLD), published_at="2026-11-08T10:00:00Z",
                   archive={"manifest_sha256": "a" * 64, "generation": "1", "origin": "https://example.org/public/"})
        publish.atomic_json(self.p.state / "receipts" / (OLD + ".json"), old)
        publish.atomic_json(self.p.state / "publication.json", receipt(NEW, OLD))
        with patch.object(self.p, "activate"), patch.object(self.p, "current_version", return_value=NEW):
            self.p.rollback()
        old.pop("content_hash")
        self.assertEqual(publish.read_json(self.p.state / "publication.json"),
                         {**old, "rolled_back_from": NEW, "rolled_back_at": ANY})

    def test_rollback_holds_before_activation_and_forces_the_next_upload(self):
        publish.atomic_json(self.p.state / "publication.json", receipt(NEW, OLD))
        def activate(version, config):
            self.assertTrue((self.p.state / "publication-hold.json").is_file())
        with patch.object(self.p, "activate", side_effect=activate) as activation, patch.object(self.p, "current_version", return_value=NEW):
            self.p.rollback()
        activation.assert_called_once()
        self.assertEqual(activation.call_args[0][0], OLD)
        current = publish.read_json(self.p.state / "publication.json")
        self.assertEqual(current["version_id"], OLD)
        self.assertNotIn("content_hash", current)

    def test_failed_rollback_keeps_the_hold_and_the_receipt(self):
        publish.atomic_json(self.p.state / "publication.json", receipt(NEW, OLD))
        with patch.object(self.p, "activate", side_effect=RuntimeError("API unavailable")), patch.object(self.p, "current_version", return_value=NEW):
            with self.assertRaises(RuntimeError):
                self.p.rollback()
        self.assertEqual(publish.read_json(self.p.state / "publication-hold.json")["reason"], "rollback requested")
        self.assertEqual(publish.read_json(self.p.state / "publication.json")["version_id"], NEW)

    def test_manual_rollback_never_replaces_an_external_version(self):
        original = receipt(NEW, OLD)
        publish.atomic_json(self.p.state / "publication.json", original)
        with patch.object(self.p, "current_version", return_value=OTHER), patch.object(self.p, "activate") as activate:
            with self.assertRaisesRegex(ValueError, "rollback not performed"):
                self.p.rollback()
        activate.assert_not_called()
        self.assertFalse((self.p.state / "publication-hold.json").exists())
        self.assertEqual(publish.read_json(self.p.state / "publication.json"), original)

    def seed_backup(self):
        self.p.cfg["spaces"].update(enabled=True)
        with closing(sqlite3.connect(self.p.database)) as db:
            db.execute("CREATE TABLE evidence (value TEXT)")
            db.execute("INSERT INTO evidence VALUES ('private source evidence')")
            db.commit()
        self.seed_public()
        # Deliberately unlike the current build: an offline build can be newer
        # than the last successful publication receipt.
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))

    def test_publication_failure_does_not_prevent_due_backup(self):
        with patch.object(self.p, "application", return_value='{"sources_ok": 1}'), \
                patch.object(self.p, "publish", side_effect=ValueError("Cloudflare unavailable")), \
                patch.object(self.p, "backup") as backup:
            result = self.p.run_cycle()
        backup.assert_called_once()
        self.assertFalse(result["ok"])
        self.assertEqual(result["phases"]["publication"], "failed")
        self.assertEqual(result["phases"]["backup"], "ok")

    def test_collection_failure_skips_publication_but_preserves_backup_attempt(self):
        with patch.object(self.p, "application", side_effect=ValueError("collection failed")), \
                patch.object(self.p, "publish") as publication, \
                patch.object(self.p, "backup") as backup:
            result = self.p.run_cycle()
        publication.assert_not_called()
        backup.assert_called_once()
        self.assertFalse(result["ok"])
        self.assertEqual(result["phases"]["publication"], "skipped")

    def test_publication_hold_still_collects_and_backs_up(self):
        publish.atomic_json(self.p.state / "publication-hold.json", {"reason": "operator hold"})
        with patch.object(self.p, "application", return_value='{"sources_ok": 1}') as application, \
                patch.object(self.p, "publish") as publish_call, \
                patch.object(self.p, "backup") as backup:
            result = self.p.run_cycle()
        application.assert_called_once_with("run")
        publish_call.assert_not_called()
        backup.assert_called_once()
        self.assertTrue(result["ok"])
        self.assertEqual(result["phases"]["publication"], "held")

    def test_optional_backup_error_is_visible_as_degraded_status(self):
        with patch.object(self.p, "application", return_value='{"sources_ok": 1}'), patch.object(self.p, "publish"), \
                patch.object(self.p, "backup", side_effect=ValueError("Spaces unavailable")):
            result = self.p.run_cycle()
        self.assertTrue(result["ok"])
        self.assertEqual(result["phases"]["backup"], "failed")
        self.assertTrue(result["warnings"])

    def test_busy_admin_is_not_reported_as_success(self):
        for command, expected in [("hold", 75), ("rollback", 75), ("check", 75), ("run", 0)]:
            with self.subTest(command=command), publish.lock(self.p.state / "run.lock"), \
                    patch("publish.Publisher", return_value=self.p), \
                    patch("sys.argv", ["publish.py", command]):
                self.assertEqual(publish.main(), expected)
        self.assertFalse((self.p.state / "publication-hold.json").exists())

    def test_status_uses_shared_probe_and_never_creates_rotating_log(self):
        with publish.lock(self.p.state / "run.lock", shared=True), \
                patch("publish.Publisher", return_value=self.p), \
                patch("sys.argv", ["publish.py", "status"]), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(publish.main(), 0)
        self.assertIn('"running": false', output.getvalue())
        self.assertFalse((self.p.state / "run.log").exists())

    def test_atomic_json_concurrent_writes_are_complete_and_private(self):
        path = self.directory / "race.json"
        original_files = set(self.directory.iterdir())
        values = [{"writer": n, "payload": str(n) * 100000} for n in range(4)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda value: publish.atomic_json(path, value), values))
        self.assertIn(json.loads(path.read_text()), values)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(set(self.directory.iterdir()), original_files | {path})

    def test_failed_atomic_json_preserves_old_record_and_cleans_temporary_file(self):
        path = self.directory / "record.json"
        publish.atomic_json(path, {"generation": "old"})
        original_files = set(self.directory.iterdir())
        with self.assertRaises(TypeError):
            publish.atomic_json(path, {"unsupported": object()})
        self.assertEqual(publish.read_json(path), {"generation": "old"})
        self.assertEqual(set(self.directory.iterdir()), original_files)

    def test_successful_check_replaces_previous_failed_check_receipt(self):
        publish.atomic_json(self.p.state / "last-run.json", {"ok": False, "command": "check", "error": "old failure"})
        with patch("publish.Publisher", return_value=self.p), \
                patch.object(self.p, "application", return_value='{"configuration": "valid"}'), \
                patch.object(self.p, "node", Path(__file__)), patch.object(self.p, "wrangler", Path(__file__)), \
                patch("sys.argv", ["publish.py", "check"]), redirect_stdout(io.StringIO()):
            self.assertEqual(publish.main(), 0)
        result = publish.read_json(self.p.state / "last-run.json")
        self.assertTrue(result["ok"])
        self.assertEqual(result["command"], "check")
        self.assertNotIn("error", result)

    def test_app_errors_and_timeouts_are_recorded_not_silent(self):
        import subprocess
        failed = subprocess.CompletedProcess([], 2, stdout="", stderr="onionflash: Output ownership marker does not match")
        with patch("publish.subprocess.run", return_value=failed), patch.object(self.p, "publish"), \
                patch.object(self.p, "backup"):
            result = self.p.run_cycle()
        self.assertEqual(result["phases"]["collection"], "failed")
        self.assertIn("Output ownership marker", result["errors"][0])
        with patch("publish.subprocess.run", side_effect=subprocess.TimeoutExpired(["onionflash"], 540)), \
                patch.object(self.p, "publish") as publication, patch.object(self.p, "backup") as backup:
            result = self.p.run_cycle()
        self.assertIn("timed out", result["errors"][0])
        publication.assert_not_called()
        backup.assert_called_once()

    def test_run_report_is_kept_in_the_cycle_result(self):
        import subprocess
        done = subprocess.CompletedProcess([], 0, stdout='{"sources_ok": 15, "sources_failed": 3}\n', stderr="")
        with patch("publish.subprocess.run", return_value=done) as run, patch.object(self.p, "publish"), \
                patch.object(self.p, "backup"):
            result = self.p.run_cycle()
        self.assertEqual(result["report"], {"sources_ok": 15, "sources_failed": 3})
        self.assertGreater(run.call_args.kwargs["timeout"], 0)

    def test_schedule_logs_cron_output_and_wrangler_logs_to_a_directory(self):
        import subprocess
        calls = []
        def fake(arguments, **kwargs):
            calls.append((arguments, kwargs))
            return subprocess.CompletedProcess(arguments, 0, stdout="MAILTO=\"\"\n", stderr="")
        with patch("publish.subprocess.run", side_effect=fake):
            self.p.schedule()
            self.p.command(["whoami"])
        installed = calls[1][1]["input"]
        self.assertIn("| logger -t onionflash", installed)
        self.assertNotIn("/dev/null", installed)
        self.assertTrue(calls[2][1]["env"]["WRANGLER_LOG_PATH"].endswith("wrangler-logs"))

    def test_missing_status_record_can_disappear_without_error(self):
        self.assertEqual(publish.read_json(self.directory / "missing.json", {}), {})

    def anonymous(self, *codes):
        """Patch the publisher's unsigned request, one outcome per request (the last
        repeats): None means it succeeded, an exception is raised as is, and a number
        is an HTTP status. Retry delays are skipped."""
        self.checked_urls, outcomes = [], list(codes)
        def request(url, method="GET", data=None, token=None):
            self.assertEqual((method, token), ("HEAD", None))
            self.checked_urls.append(url)
            code = outcomes.pop(0) if len(outcomes) > 1 else outcomes[0]
            if isinstance(code, Exception):
                raise code
            if code is not None:
                raise HTTPError(url, code, "anonymous", {}, None)
            return b""
        return patch.multiple(self.p, request=MagicMock(side_effect=request), delay=MagicMock())

    def test_backup_is_a_private_sqlite_snapshot_and_respects_the_interval(self):
        self.seed_backup()
        from onionflash.store import backup_database
        client = FakeSpaces()
        def snapshot(source, destination):
            with self.assertRaises(publish.BusyLock):  # taken under the application lock
                with publish.lock(str(self.p.database) + ".lock", nonblocking=True):
                    pass
            return backup_database(source, destination)
        class Frozen(datetime):  # both uploads happen in the same UTC second
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 11, 10, 12, 0, 0, tzinfo=timezone.utc)
        with patch("boto3.client", return_value=client), self.anonymous(403), \
                patch("onionflash.store.backup_database", side_effect=snapshot), patch("publish.datetime", Frozen):
            self.p.backup()
            self.p.backup()
            self.assertEqual(len(client.uploads), 1)
            self.p.backup(force=True)
            self.p.cfg["spaces"]["bucket"] = "another-private-bucket"
            self.p.backup()  # a new destination does not wait for the interval
        self.assertEqual(len(client.uploads), 3)
        self.assertEqual(client.uploads[2][0], "another-private-bucket")
        bucket, key, body, acl = client.uploads[0]
        # The anonymous check asks for exactly the object that was uploaded.
        self.assertEqual(self.checked_urls[0], self.p.cfg["spaces"]["endpoint_url"] + "/" + bucket + "/" + key)
        self.assertEqual(acl, "private")
        self.assertRegex(key, r"^onionflash/backups/\d{8}T\d{6}Z-[0-9a-f]{8}\.sqlite3\.gz$")
        self.assertEqual(key[:36], client.uploads[1][1][:36])  # the same second...
        self.assertNotEqual(key, client.uploads[1][1])  # ...but distinct keys
        restored = self.directory / "restored.sqlite3"
        restored.write_bytes(gzip.decompress(body))
        with closing(sqlite3.connect(restored)) as database:
            self.assertEqual(database.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(database.execute("SELECT value FROM evidence").fetchone()[0], "private source evidence")

    def test_a_backup_not_proven_private_is_deleted_and_fails(self):
        self.seed_backup()
        # A success or a 404 is final; a network failure is retried up to the attempt limit.
        for code, message, requests in [(None, "publicly readable", 1), (404, "unexpected anonymous response 404", 1),
                                        (URLError("reset"), "could not be checked", self.p.net["attempts"])]:
            with self.subTest(code=code):
                client = FakeSpaces()
                with patch("boto3.client", return_value=client), self.anonymous(code):
                    with self.assertRaisesRegex(ValueError, message):
                        self.p.backup(force=True)
                self.assertEqual(len(self.checked_urls), requests)
                self.assertFalse((self.p.state / "backup.json").exists())
                self.assertEqual(client.deleted, [client.uploads[0][:2]])

    def test_a_throttled_or_failed_privacy_check_is_retried(self):
        self.seed_backup()
        for first in (503, 429, 408, URLError("reset")):
            with self.subTest(first=first):
                client = FakeSpaces()
                with patch("boto3.client", return_value=client), self.anonymous(first, 403):
                    self.p.backup(force=True)
                    self.p.delay.assert_called_once_with(0)  # backoff before the retry
                self.assertEqual(len(self.checked_urls), 2)
                self.assertEqual(client.deleted, [])

    def test_an_exposed_backup_that_cannot_be_deleted_is_named_in_the_error(self):
        self.seed_backup()
        client = FakeSpaces(cleanup_error=True)
        with patch("boto3.client", return_value=client), self.anonymous(None):
            with self.assertRaises(ValueError) as caught:
                self.p.backup(force=True)
        message = str(caught.exception)
        self.assertIn(client.uploads[0][1], message)
        self.assertRegex(message, "publicly readable, and deleting it failed: .*cleanup unavailable")
        self.assertFalse((self.p.state / "backup.json").exists())

    def test_an_explicit_backup_fails_when_spaces_backups_are_disabled(self):
        with patch("boto3.client") as client, self.assertRaisesRegex(ValueError, "Spaces backups are disabled"):
            self.p.execute("backup", force=True)
        client.assert_not_called()

    def test_an_ambiguous_upload_failure_removes_the_object(self):
        self.seed_backup()
        class LostResponse(FakeSpaces):
            def put_object(self, **kwargs):
                super().put_object(**kwargs)
                raise ConnectionError("response lost")
        client = LostResponse()
        with patch("boto3.client", return_value=client), self.anonymous(403):
            with self.assertRaises(ConnectionError):
                self.p.backup(force=True)
        self.assertEqual(client.deleted, [client.uploads[0][:2]])
        self.assertFalse((self.p.state / "backup.json").exists())
        client = LostResponse(cleanup_error=True)
        with patch("boto3.client", return_value=client), self.anonymous(403):
            with self.assertRaisesRegex(RuntimeError, "could not be removed") as caught:
                self.p.backup(force=True)
        self.assertIn(client.uploads[0][1], str(caught.exception))  # the error names the key

    def test_retention_deletes_only_old_backups_and_its_failure_is_retried(self):
        self.seed_backup()
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        listed = [{"Key": "onionflash/backups/20200101T000000Z-0123abcd.sqlite3.gz", "LastModified": old},
                  {"Key": "onionflash/backups/20200101T000000Z-" + "a" * 12 + ".tar.gz", "LastModified": old},
                  {"Key": "onionflash/backups/notes.txt", "LastModified": old}]
        client = FakeSpaces(listed=listed)
        with patch("boto3.client", return_value=client), self.anonymous(403):
            self.p.backup()
        self.assertEqual([key for _, key in client.deleted], [item["Key"] for item in listed[:2]])
        client = FakeSpaces(retention_error=True)
        with patch("boto3.client", return_value=client), self.anonymous(403):
            with self.assertRaisesRegex(RuntimeError, "retention"):
                self.p.backup(force=True)


if __name__ == "__main__":
    unittest.main()
