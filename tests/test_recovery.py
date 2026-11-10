"""Failure-path checks for owned deployment transitions and private backups."""
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tarfile
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import publish


OLD = "11111111-1111-4111-8111-111111111111"
NEW = "22222222-2222-4222-8222-222222222222"
OTHER = "33333333-3333-4333-8333-333333333333"


def receipt(version, previous=None):
    return {"version_id": version, "previous_version_id": previous,
            "content_hash": version, "published_at": "2026-11-09T10:00:00Z",
            "manifest": {"index.html": version},
            "origin_url": "https://news.example.workers.dev/onionflash"}


class FakeSpaces:
    def __init__(self, *, public_acl=False, corrupt=False, cleanup_error=False,
                 retention_error=False):
        self.public_acl, self.corrupt = public_acl, corrupt
        self.cleanup_error, self.retention_error = cleanup_error, retention_error
        self.objects, self.uploads, self.deleted = {}, [], []

    def upload_file(self, filename, bucket, key, ExtraArgs):
        body = Path(filename).read_bytes()
        self.objects[bucket, key] = (body, ExtraArgs)
        self.uploads.append((bucket, key, body, ExtraArgs))

    def head_object(self, *, Bucket, Key):
        body, options = self.objects[Bucket, Key]
        return {"ContentLength": len(body), "Metadata": {
            "sha256": "corrupt" if self.corrupt else options["Metadata"]["sha256"]}}

    def get_object_acl(self, **kwargs):
        return {"Grants": [{"Grantee": {"Type": "Group"}}] if self.public_acl else []}

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
        return [{"Contents": []}]


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
''')
        self.p = publish.Publisher(self.directory / "deployment.toml")

    def seed_receipts(self):
        old, new = receipt(OLD), receipt(NEW, OLD)
        publish.atomic_json(self.p.state / "publication.json", old)
        publish.atomic_json(self.p.state / "receipts" / (OLD + ".json"), old)
        return old, new

    def seed_transition(self, old, new):
        publish.atomic_json(self.p.state / "transition.json", {
            "kind": "publish", "before_version": OLD, "before_receipt": old,
            "target": new, "started_at": "2026-11-09T11:00:00Z"})

    def seed_backup(self):
        self.p.cfg["spaces"].update(enabled=True, create_bucket=False)
        with closing(sqlite3.connect(self.p.database)) as db:
            db.execute("CREATE TABLE evidence (value TEXT)")
            db.execute("INSERT INTO evidence VALUES ('private source evidence')")
            db.commit()
        self.p.public.mkdir(parents=True)
        for name in publish.PUBLIC_FILES:
            body = b"generated public content"
            if name == "feed.xml":
                body = b"<rss version='2.0'><channel><title>News</title></channel></rss>"
            elif name == "feed.json":
                body = b'{"items":[]}'
            (self.p.public / name).write_bytes(body)
        # Deliberately unlike the current build: an offline build can be newer
        # than the last successful publication receipt.
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))

    def test_interrupted_activation_reconciles_owned_target(self):
        old, new = self.seed_receipts()
        actual = [OLD]
        def interrupt(version, config):
            self.assertTrue((self.p.state / "transition.json").is_file())
            actual[0] = version
            raise KeyboardInterrupt()
        with patch.object(self.p, "activate", side_effect=interrupt), \
                patch.object(self.p, "current_version", side_effect=lambda: actual[0]), \
                patch.object(self.p, "verify_receipt"):
            with self.assertRaises(KeyboardInterrupt):
                self.p.transition(new, OLD, old)
            self.assertTrue((self.p.state / "transition.json").is_file())
            self.p.reconcile()
        current = json.loads((self.p.state / "publication.json").read_text())
        self.assertEqual(current["version_id"], NEW)
        self.assertTrue((self.p.state / "receipts" / (NEW + ".json")).is_file())
        self.assertFalse((self.p.state / "transition.json").exists())

    def test_failed_verification_restores_previous_version_without_holding(self):
        old, new = self.seed_receipts()
        actual = [OLD]
        def activate(version, config):
            actual[0] = version
        def verify(value):
            if value["version_id"] == NEW:
                raise ValueError("new version failed verification")
        with patch.object(self.p, "activate", side_effect=activate), \
                patch.object(self.p, "current_version", side_effect=lambda: actual[0]), \
                patch.object(self.p, "verify_receipt", side_effect=verify):
            with self.assertRaises(Exception):
                self.p.transition(new, OLD, old)
        self.assertEqual(actual[0], OLD)
        self.assertEqual(json.loads((self.p.state / "publication.json").read_text())["version_id"], OLD)
        self.assertFalse((self.p.state / "publication-hold.json").is_file())

    def test_recovery_never_overwrites_an_external_version_change(self):
        old, new = self.seed_receipts()
        actual, activations = [OLD], []
        def activate(version, config):
            activations.append(version)
            actual[0] = version
        def verify(value):
            actual[0] = OTHER
            raise ValueError("verification interrupted by external deployment")
        with patch.object(self.p, "activate", side_effect=activate), \
                patch.object(self.p, "current_version", side_effect=lambda: actual[0]), \
                patch.object(self.p, "verify_receipt", side_effect=verify):
            with self.assertRaises(Exception):
                self.p.transition(new, OLD, old)
        self.assertEqual(activations, [NEW])
        self.assertEqual(actual[0], OTHER)
        self.assertTrue((self.p.state / "transition.json").is_file())
        self.assertFalse((self.p.state / "publication-hold.json").is_file())

    def test_manual_rollback_holds_before_activation_and_preserves_receipt_time(self):
        old, new = self.seed_receipts()
        publish.atomic_json(self.p.state / "publication.json", new)
        actual = [NEW]
        def activate(version, config):
            self.assertTrue((self.p.state / "publication-hold.json").is_file())
            self.assertEqual(publish.read_json(self.p.state / "transition.json")["kind"], "rollback")
            actual[0] = version
        with patch.object(self.p, "activate", side_effect=activate), \
                patch.object(self.p, "current_version", side_effect=lambda: actual[0]), \
                patch.object(self.p, "verify_receipt"):
            self.p.rollback()
        restored = publish.read_json(self.p.state / "publication.json")
        self.assertEqual(actual[0], OLD)
        self.assertEqual(restored["published_at"], old["published_at"])
        self.assertTrue((self.p.state / "publication-hold.json").is_file())
        self.assertFalse((self.p.state / "transition.json").exists())

    def test_reconcile_refuses_unknown_external_version(self):
        old, new = self.seed_receipts()
        self.seed_transition(old, new)
        with patch.object(self.p, "current_version", return_value=OTHER), \
                patch.object(self.p, "activate") as activate, \
                patch.object(self.p, "verify_receipt") as verify:
            with self.assertRaises(Exception):
                self.p.reconcile()
        activate.assert_not_called()
        verify.assert_not_called()
        self.assertEqual(json.loads((self.p.state / "publication.json").read_text())["version_id"], OLD)
        self.assertTrue((self.p.state / "transition.json").exists())

    def test_reconcile_restores_before_pointer_when_remote_already_reverted(self):
        old, new = self.seed_receipts()
        self.seed_transition(old, new)
        publish.atomic_json(self.p.state / "publication.json", new)
        with patch.object(self.p, "current_version", return_value=OLD), \
                patch.object(self.p, "activate") as activate, \
                patch.object(self.p, "verify_receipt"):
            self.p.reconcile()
        activate.assert_not_called()
        self.assertEqual(json.loads((self.p.state / "publication.json").read_text())["version_id"], OLD)
        self.assertFalse((self.p.state / "transition.json").exists())

    def test_reconcile_can_restore_known_good_before_if_target_is_bad(self):
        old, new = self.seed_receipts()
        self.seed_transition(old, new)
        actual = [NEW]
        def activate(version, config):
            actual[0] = version
        def verify(value):
            if value["version_id"] == NEW:
                raise ValueError("target assets fail verification")
        with patch.object(self.p, "current_version", side_effect=lambda: actual[0]), \
                patch.object(self.p, "activate", side_effect=activate), \
                patch.object(self.p, "verify_receipt", side_effect=verify):
            with self.assertRaisesRegex(ValueError, "target assets fail verification"):
                self.p.reconcile()
        self.assertEqual(actual[0], OLD)
        self.assertEqual(json.loads((self.p.state / "publication.json").read_text())["version_id"], OLD)
        self.assertFalse((self.p.state / "transition.json").exists())
        self.assertFalse((self.p.state / "publication-hold.json").is_file())

    def test_version_archive_precedes_current_pointer_write(self):
        old, new = self.seed_receipts()
        original = publish.atomic_json
        writes = []
        def record(path, value):
            if value.get("version_id") == NEW:
                writes.append(Path(path))
                if Path(path) == self.p.state / "publication.json":
                    self.assertTrue((self.p.state / "receipts" / (NEW + ".json")).is_file())
            original(path, value)
        with patch("publish.atomic_json", side_effect=record), \
                patch.object(self.p, "activate"), \
                patch.object(self.p, "current_version", return_value=OLD), \
                patch.object(self.p, "verify_receipt"):
            self.p.transition(new, OLD, old)
        archive = self.p.state / "receipts" / (NEW + ".json")
        self.assertLess(writes.index(archive), writes.index(self.p.state / "publication.json"))

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

    def test_unresolved_held_transition_is_not_reported_healthy(self):
        old, new = self.seed_receipts()
        self.seed_transition(old, new)
        publish.atomic_json(self.p.state / "publication-hold.json", {"reason": "transition failed"})
        with patch.object(self.p, "application", return_value='{"sources_ok": 1}'), patch.object(self.p, "publish") as publication, \
                patch.object(self.p, "reconcile") as reconciliation, patch.object(self.p, "backup") as backup:
            result = self.p.run_cycle()
        self.assertFalse(result["ok"])
        self.assertEqual(result["phases"]["publication"], "unreconciled")
        publication.assert_not_called()
        reconciliation.assert_not_called()  # An operator hold remains authoritative.
        backup.assert_called_once()

    def test_failed_rollback_precondition_keeps_an_explicit_intent_hold(self):
        old, new = self.seed_receipts()
        publish.atomic_json(self.p.state / "publication.json", new)
        with patch.object(self.p, "current_version", side_effect=ValueError("API unavailable")):
            with self.assertRaises(ValueError):
                self.p.rollback()
        self.assertEqual(publish.read_json(self.p.state / "publication-hold.json")["reason"], "rollback requested")
        self.assertFalse((self.p.state / "rollback.json").exists())
        self.assertEqual(publish.read_json(self.p.state / "publication.json")["version_id"], NEW)

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

    def test_nonfinite_and_boolean_network_values_are_rejected(self):
        path = self.directory / "deployment.toml"
        original = path.read_text()
        for bad in ["nan", "inf", "true"]:
            with self.subTest(value=bad):
                path.write_text(original.replace("timeout_seconds = 30", "timeout_seconds = " + bad, 1))
                with self.assertRaises(ValueError):
                    publish.Publisher(path)

    def test_backup_force_and_destination_change_bypass_interval(self):
        self.seed_backup()
        client = FakeSpaces()
        with patch("boto3.client", return_value=client):
            self.p.backup()
            self.p.backup()
            self.assertEqual(len(client.uploads), 1)
            self.p.backup(force=True)
            self.assertEqual(len(client.uploads), 2)
            self.p.cfg["spaces"]["bucket"] = "different-news-bucket"
            self.p.backup()
        self.assertEqual(len(client.uploads), 3)
        self.assertEqual(client.uploads[-1][0], "different-news-bucket")

    def test_failed_backup_privacy_or_integrity_removes_only_new_object(self):
        self.seed_backup()
        for arguments in [{"public_acl": True}, {"corrupt": True}]:
            with self.subTest(arguments=arguments):
                client = FakeSpaces(**arguments)
                client.objects["unrelated", "keep"] = (b"retained", {})
                with patch("boto3.client", return_value=client):
                    with self.assertRaises(ValueError):
                        self.p.backup(force=True)
                bucket, key, _, _ = client.uploads[0]
                self.assertEqual(client.deleted, [(bucket, key)])
                self.assertEqual(set(client.objects), {("unrelated", "keep")})
                self.assertFalse((self.p.state / "backup.json").exists())

    def test_failed_privacy_cleanup_remains_a_reported_error(self):
        self.seed_backup()
        client = FakeSpaces(public_acl=True, cleanup_error=True)
        with patch("boto3.client", return_value=client):
            with self.assertRaisesRegex(Exception, "cleanup"):
                self.p.backup(force=True)
        self.assertFalse((self.p.state / "backup.json").exists())

    def test_retention_failure_does_not_mark_backup_cycle_complete(self):
        self.seed_backup()
        client = FakeSpaces(retention_error=True)
        with patch("boto3.client", return_value=client):
            with self.assertRaisesRegex(RuntimeError, "retention"):
                self.p.backup()
        self.assertFalse((self.p.state / "backup.json").exists())

    def test_snapshot_helper_runs_under_lock_and_archive_has_its_own_manifest(self):
        self.seed_backup()
        from onionflash.store import backup_database
        client = FakeSpaces()
        def snapshot(source, destination):
            self.assertEqual(source, self.p.database)
            with self.assertRaises(publish.BusyLock):
                with publish.lock(str(self.p.database) + ".lock", nonblocking=True):
                    pass
            return backup_database(source, destination)
        with patch("boto3.client", return_value=client), \
                patch("onionflash.store.backup_database", side_effect=snapshot) as snapshot_call:
            self.p.backup()
        snapshot_call.assert_called_once()
        archive = client.uploads[0][2]
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
            snapshot = json.load(tar.extractfile("snapshot-manifest.json"))
            manifest = snapshot["public_manifest"]
            for name, digest in manifest.items():
                self.assertEqual(hashlib.sha256(tar.extractfile("public/" + name).read()).hexdigest(), digest)
            self.assertNotEqual(manifest, json.load(tar.extractfile("publication.json"))["manifest"])
            self.assertFalse(snapshot["matches_publication"])
            restored = self.directory / "restored.sqlite3"
            restored.write_bytes(tar.extractfile("onionflash.sqlite3").read())
        with closing(sqlite3.connect(restored)) as database:
            self.assertEqual(database.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(database.execute("SELECT value FROM evidence").fetchone()[0], "private source evidence")

    def test_invalid_public_tree_does_not_prevent_private_database_backup(self):
        self.seed_backup()
        (self.p.public / "private-notes.txt").write_text("must not enter the archive")
        client = FakeSpaces()
        with patch("boto3.client", return_value=client):
            self.p.backup(force=True)
        with tarfile.open(fileobj=io.BytesIO(client.uploads[0][2]), mode="r:gz") as tar:
            names = tar.getnames()
            self.assertIn("onionflash.sqlite3", names)
            self.assertFalse(any(name == "public" or name.startswith("public/") for name in names))
            snapshot = json.load(tar.extractfile("snapshot-manifest.json"))
            self.assertEqual(snapshot["public_manifest"], {})
            self.assertIn("Unexpected public file", snapshot["public_error"])


if __name__ == "__main__":
    unittest.main()
