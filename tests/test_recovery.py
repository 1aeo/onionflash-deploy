"""Failure-path checks for owned deployment transitions and private backups."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import errno, gzip, json, os, re, sqlite3, subprocess, sys, unittest
from http.client import BadStatusLine
from pathlib import Path
from unittest.mock import ANY, MagicMock, call, patch
from urllib.error import HTTPError, URLError

from botocore.exceptions import ClientError
import publish
from tests.fakes import _AUTO, DeployTestCase, FakeS3, NEW, OLD, OTHER, receipt


class RecoveryTests(DeployTestCase):
    """Tests for guarded activation, rollback, cycle phases, and private Spaces backups."""

    def cycle(self, *, application=_AUTO, publish_err=None, backup_err=None, run=None):
        """Run one deployment cycle with configurable application, publish, and backup stubs."""
        with ExitStack() as stack:
            if run is not None:
                stack.enter_context(patch("publish.subprocess.run", side_effect=run if isinstance(run, BaseException) else None, return_value=run))
            else:
                kw = {"return_value": '{"sources_ok": 1}'} if application is _AUTO else (
                    {"return_value": application} if isinstance(application, str) else {"side_effect": application}
                )
                self.app_mock = stack.enter_context(patch.object(self.p, "application", **kw))
            self.pub_mock = stack.enter_context(patch.object(self.p, "publish", side_effect=publish_err))
            self.bkp_mock = stack.enter_context(patch.object(self.p, "backup", side_effect=backup_err))
            return self.p.run_cycle()

    def seed_backup(self):
        """Enable Spaces backups and populate private SQLite evidence plus baseline public state."""
        self.p.cfg["spaces"].update(enabled=True)
        with self.db() as db:
            db.execute("CREATE TABLE evidence (value TEXT)")
            db.execute("INSERT INTO evidence VALUES ('private source evidence')")
        self.seed_public()
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))

    def anonymous(self, *codes):
        """Patch Publisher.request and delay to simulate anonymous HTTP HEAD status codes."""
        self.checked_urls, outcomes = [], list(codes)
        def request(url, method="GET", data=None, token=None):
            """Assert an unauthenticated HEAD probe and replay the next configured outcome."""
            self.assertEqual((method, token), ("HEAD", None))
            self.checked_urls.append(url)
            code = outcomes.pop(0) if len(outcomes) > 1 else outcomes[0]
            if isinstance(code, BaseException):
                raise code
            if code is not None:
                raise HTTPError(url, code, "anonymous", {}, None)
            return b""
        return patch.multiple(self.p, request=MagicMock(side_effect=request), delay=MagicMock())

    def test_publish_preview_activation_and_receipt_pruning(self):
        """Verify preview-then-activate ordering and pruning of older static receipts."""
        self.seed_public()
        publish.atomic_json(self.p.state / "receipts" / (OTHER + ".json"), receipt(OTHER))
        self.fake_cloudflare(OLD)
        result = self.p.publish()
        self.assertEqual(self.calls, ["versions upload", "verify preview", "activate " + NEW, "verify live"])
        self.assertEqual((result["version_id"], result["previous_version_id"]), (NEW, OLD))
        self.assertEqual({p.name for p in (self.p.state / "receipts").iterdir()}, {OLD + ".json", NEW + ".json"})
        self.assertEqual(publish.read_json(self.p.state / "receipts" / (OLD + ".json")), receipt(OLD))

    def test_permalinks_permanence_bootstrap_rollback_interrupt_and_damage(self):
        """Enforce permalink permanence across rollbacks, interrupted writes, and corrupted state."""
        self.seed_public()
        self.fake_cloudflare(OLD)
        gone = "news/gone/index.html"
        permalinks = self.p.state / "permalinks.json"
        publish.atomic_json(permalinks, [])
        for prev in ({**receipt(OLD), "manifest": {gone: "a" * 64}}, {**receipt(OLD), "articles": [gone]}):
            with self.subTest(prev=prev):
                publish.atomic_json(self.p.state / "publication.json", prev)
                with self.assertRaisesRegex(ValueError, "previously published article"):
                    self.p.publish()
                self.assertEqual(self.calls, [])

        self.add_article("earlier")
        self.add_article("live")
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD, OTHER))
        publish.atomic_json(self.p.state / "receipts" / (OTHER + ".json"), {**receipt(OTHER), "manifest": {"news/other/index.html": "a" * 64}})
        published = self.p.publish()
        expected = ["news/earlier/index.html", "news/live/index.html"]
        self.assertEqual(publish.read_json(permalinks), expected)

        for seed in (None, [], ["news/earlier/index.html"]):
            with self.subTest(seed=seed):
                if seed is None:
                    permalinks.unlink(missing_ok=True)
                else:
                    publish.atomic_json(permalinks, seed)
                self.calls.clear()
                self.assertEqual(self.p.publish(), published)
                self.assertEqual(self.calls, [])
                self.assertEqual(publish.read_json(permalinks), expected)

        self.p.rollback()
        self.p.execute("resume")
        with self.db() as db:
            db.execute("DELETE FROM public_artifacts WHERE path='news/live/index.html'")
        for prev in (None, {"version_id": OLD}, {}):
            with self.subTest(prev=prev):
                if prev is not None:
                    publish.atomic_json(self.p.state / "publication.json", prev)
                self.calls.clear()
                with self.assertRaisesRegex(ValueError, "previously published article.*to withdraw a story, retract it"):
                    self.p.publish()
                self.assertEqual(self.calls, [])

        self.add_article("live")
        self.add_article("interrupted")
        self.fake_cloudflare(OLD)
        write = publish.atomic_json
        def interrupt_receipt(path, val):
            """Raise KeyboardInterrupt when writing the new version receipt."""
            if Path(path).name == NEW + ".json":
                raise KeyboardInterrupt
            write(path, val)
        with patch("publish.atomic_json", side_effect=interrupt_receipt), self.assertRaises(KeyboardInterrupt):
            self.p.publish()
        self.assertIn("news/interrupted/index.html", publish.read_json(permalinks))

        for bad in ("{}", '["news/../x"]', '["news/kept/index.html"'):
            with self.subTest(bad=bad):
                permalinks.write_text(bad)
                self.calls.clear()
                with self.assertRaisesRegex(ValueError, re.escape(f"{permalinks} is damaged")):
                    self.p.publish()
                self.assertEqual(self.calls, [])

    def test_live_and_preview_failure_recovery_and_external_version_guards(self):
        """Restore the prior version on failed live checks and refuse overwriting external changes."""
        self.seed_public()
        self.fake_cloudflare(OLD, failing_check="preview")
        with self.assertRaisesRegex(ValueError, "check failed"):
            self.p.publish()
        self.assertFalse(any(c.startswith("activate") for c in self.calls))

        with patch.object(self.p, "command", return_value=""), self.assertRaisesRegex(RuntimeError, "did not report the uploaded version"):
            self.p.publish()
        with patch.object(self.p, "app_version", side_effect=RuntimeError("metadata unavailable")), \
                self.assertRaisesRegex(RuntimeError, "metadata unavailable"):
            self.calls.clear()
            self.p.publish()
        self.assertEqual(self.calls, [])

        self.add_article("new")
        self.fake_cloudflare(OLD, failing_check="live")
        with self.assertRaisesRegex(ValueError, "check failed"):
            self.p.publish()
        self.assertEqual((self.calls[-2:], self.live[0], publish.read_json(self.p.state / "permalinks.json")),
                         (["verify live", "activate " + OLD], OLD, []))
        self.assertFalse((self.p.state / "publication-hold.json").exists())

        self.fake_cloudflare(OLD, failing_check="live")
        def fail_restore(version):
            """Succeed activating NEW then fail when attempting to restore OLD."""
            if version == OLD:
                raise RuntimeError("restore unavailable")
            self.live[0] = version
        with patch.object(self.p, "activate", side_effect=fail_restore), self.assertRaisesRegex(RuntimeError, "check failed.*restore unavailable"):
            self.p.publish()
        self.assertNotIn("content_hash", publish.read_json(self.p.state / "publication.json"))
        with self.assertRaisesRegex(ValueError, rf"inspect before publishing \(live {NEW}, recorded {OLD}\)"):
            self.p.publish()

        for recorded in (receipt(OLD), {}):
            with self.subTest(recorded=recorded.get("version_id")):
                self.fake_cloudflare(OTHER, recorded=recorded)
                with self.assertRaisesRegex(ValueError, f"differs.*live {OTHER}, recorded {recorded.get('version_id')}"):
                    self.p.publish()
                self.assertEqual((self.calls, self.live[0]), ([], OTHER))

        self.fake_cloudflare(OLD)
        with patch.object(self.p, "verify", side_effect=lambda *a, **k: self.live.__setitem__(0, OTHER)), \
                self.assertRaisesRegex(ValueError, "changed during preview"):
            self.p.publish()
        self.assertEqual(self.live[0], OTHER)

        self.fake_cloudflare(OLD)
        def ext_live(url, manifest, archive=None):
            """Simulate an external deployment replacing the Worker during live verification."""
            if "candidate-" not in url:
                self.live[0] = OTHER
                raise ValueError("external deployment")
        with patch.object(self.p, "verify", side_effect=ext_live), self.assertRaisesRegex(RuntimeError, "external version retained"):
            self.p.publish()
        self.assertEqual(self.live[0], OTHER)

        self.fake_cloudflare(OLD)
        with patch.object(self.p, "activate", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            self.p.publish()
        self.assertNotIn("content_hash", publish.read_json(self.p.state / "publication.json"))
        publish.atomic_json(self.p.state / "publication.json", receipt(NEW, OLD))
        self.live[0] = NEW
        with patch.object(self.p, "activate", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            self.p.rollback()
        self.assertNotIn("content_hash", publish.read_json(self.p.state / "publication.json"))
        self.assertTrue((self.p.state / "publication-hold.json").is_file())

    def test_first_worker_deploy_and_manual_rollback_guards(self):
        """Record first-deploy versions even on failure and enforce hold/receipt checks on rollback."""
        self.seed_public()
        self.fake_cloudflare(None)
        first = self.p.publish()
        self.assertEqual((self.calls, first["version_id"], first["previous_version_id"]), (["deploy --config", "verify live"], NEW, None))

        (self.p.state / "permalinks.json").unlink(missing_ok=True)
        self.fake_cloudflare(None, failing_check="live")
        with self.assertRaisesRegex(ValueError, "check failed"):
            self.p.publish()
        self.assertEqual(publish.read_json(self.p.state / "publication.json"), {"version_id": NEW})
        self.fake_cloudflare(NEW, recorded=False)
        self.p.publish()
        self.assertEqual(self.calls, ["versions upload", "verify preview", "activate " + NEW, "verify live"])

        for versions in ([None, NEW], [None, RuntimeError("lookup failed")]):
            self.fake_cloudflare(None)
            with patch.object(self.p, "command", side_effect=RuntimeError("deploy reported")), \
                    patch.object(self.p, "current_version", side_effect=versions), \
                    self.assertRaisesRegex(RuntimeError, "deploy reported"):
                self.p.publish()
            if versions[1] == NEW:
                self.assertEqual(publish.read_json(self.p.state / "publication.json"), {"version_id": NEW})

        old = dict(receipt(OLD), published_at="2026-11-08T10:00:00Z",
                   archive={"manifest_sha256": "a" * 64, "generation": "1", "origin": "https://example.org/public/"})
        publish.atomic_json(self.p.state / "receipts" / (OLD + ".json"), old)
        publish.atomic_json(self.p.state / "publication.json", receipt(NEW, OLD))
        held_during_activate = []
        with patch.object(self.p, "activate", side_effect=lambda v: held_during_activate.append((self.p.state / "publication-hold.json").is_file())), \
                patch.object(self.p, "current_version", return_value=NEW):
            self.p.rollback()
        self.assertEqual(held_during_activate, [True])
        old.pop("content_hash")
        self.assertEqual(publish.read_json(self.p.state / "publication.json"), {**old, "rolled_back_from": NEW, "rolled_back_at": ANY})

        (self.p.state / "publication-hold.json").unlink()
        publish.atomic_json(self.p.state / "publication.json", receipt(NEW, OLD))
        with patch.object(self.p, "activate", side_effect=RuntimeError("API unavailable")), \
                patch.object(self.p, "current_version", return_value=NEW), self.assertRaises(RuntimeError):
            self.p.rollback()
        self.assertEqual(publish.read_json(self.p.state / "publication-hold.json")["reason"], "rollback requested")
        self.assertEqual(publish.read_json(self.p.state / "publication.json")["version_id"], NEW)

        (self.p.state / "publication-hold.json").unlink()
        original = receipt(NEW, OLD)
        publish.atomic_json(self.p.state / "publication.json", original)
        with patch.object(self.p, "current_version", return_value=OTHER), patch.object(self.p, "activate") as act, \
                self.assertRaisesRegex(ValueError, "rollback not performed"):
            self.p.rollback()
        act.assert_not_called()
        self.assertFalse((self.p.state / "publication-hold.json").exists())

    def test_run_cycle_phases_busy_locks_atomic_json_and_check_receipt(self):
        """Verify run_cycle phase isolation, BusyLock exit codes, atomic_json races, and last-run.json."""
        with self.assertLogs(publish.LOG, "WARNING"):
            res = self.cycle(application=[ValueError("collection failed"), "{}"])
        self.pub_mock.assert_not_called()
        self.bkp_mock.assert_called_once()
        self.assertEqual((res["ok"], res["phases"]["publication"], self.app_mock.call_args_list),
                         (False, "skipped", [call("run"), call("decisions", "summary", "--since", "7d")]))

        res = self.cycle(publish_err=ValueError("Cloudflare unavailable"))
        self.bkp_mock.assert_called_once()
        self.assertEqual((res["ok"], res["phases"]["publication"], res["phases"]["backup"]), (False, "failed", "ok"))

        publish.atomic_json(self.p.state / "publication-hold.json", {"reason": "operator hold"})
        res = self.cycle()
        self.pub_mock.assert_not_called()
        self.bkp_mock.assert_called_once()
        self.assertEqual((res["ok"], res["phases"]["publication"]), (True, "held"))
        (self.p.state / "publication-hold.json").unlink()

        res = self.cycle(backup_err=ValueError("Spaces unavailable"))
        self.assertEqual((res["ok"], res["phases"]["backup"], bool(res["warnings"])), (True, "failed", True))

        failed = subprocess.CompletedProcess([], 2, stdout="", stderr="onionflash: Output ownership marker does not match")
        self.assertIn("Output ownership marker", self.cycle(run=failed)["errors"][0])
        res = self.cycle(run=subprocess.TimeoutExpired(["onionflash"], 540))
        self.assertIn("timed out", res["errors"][0])
        self.pub_mock.assert_not_called()
        self.bkp_mock.assert_called_once()

        done = subprocess.CompletedProcess([], 0, stdout='{"sources_ok": 15, "sources_failed": 3}\n', stderr="")
        self.assertEqual(self.cycle(run=done)["report"], {"sources_ok": 15, "sources_failed": 3})

        for cmd, expected in [("hold", 75), ("rollback", 75), ("check", 75), ("run", 0)]:
            with self.subTest(command=cmd), publish.lock(self.p.state / "run.lock"):
                self.assertEqual(self.run_cli(cmd)[0], expected)
        with publish.lock(self.p.state / "run.lock", shared=True):
            code, out = self.run_cli("status")
        self.assertEqual((code, '"running": false' in out, (self.p.state / "run.log").exists()), (0, True, False))

        race_path = self.directory / "race.json"
        values = [{"writer": n, "payload": str(n) * 100000} for n in range(4)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda value: publish.atomic_json(race_path, value), values))
        self.assertIn(json.loads(race_path.read_text()), values)
        self.assertEqual(race_path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(TypeError):
            publish.atomic_json(race_path, {"unsupported": object()})
        self.assertIn(publish.read_json(race_path), values)
        self.assertEqual(publish.read_json(self.directory / "missing.json", {}), {})

        publish.atomic_json(self.p.state / "last-run.json", {"ok": False, "command": "check", "error": "old failure"})
        with patch.object(self.p, "application", return_value='{"configuration": "valid"}'):
            self.assertEqual(self.run_cli("check", fake_toolchain=True)[0], 0)
        self.assertNotIn("error", publish.read_json(self.p.state / "last-run.json"))

    def test_weekly_summary_and_schedule_logging(self):
        """Verify ISO-week editorial summary generation, failure warnings, and cron/Wrangler log paths."""
        self.addCleanup(os.umask, os.umask(0o022))
        weekly, sizes = self.p.state / "weekly", []
        def app(*arguments):
            """Return stub JSON reports for run and weekly decisions summary invocations."""
            if arguments == ("run",):
                return '{"sources_ok": 1}'
            sizes.append(len(sizes) + 1)
            return '{"feed": {"bytes": %d}, "title": "\\u202e"}' % sizes[-1]
        with patch("publish.datetime") as clock:
            for day in (datetime(2026, 12, 31), datetime(2027, 1, 3), datetime(2027, 1, 4)):
                clock.now.return_value = day.replace(tzinfo=timezone.utc)
                self.assertEqual(self.cycle(application=app)["phases"]["weekly_summary"], "ok")
        self.assertEqual((sizes, sorted(p.name for p in weekly.iterdir())), ([1, 2], ["2026-W53.md", "2027-W01.md", "latest.md"]))
        self.assertEqual((weekly / "latest.md").stat().st_mode & 0o777, 0o600)

        for child in weekly.iterdir():
            child.unlink()
        summary = '{"writer_clears": 0}'
        app_fn = ['{"sources_ok": 1}', summary]
        with patch("publish.datetime") as clock:
            clock.now.return_value = datetime(2026, 11, 11, tzinfo=timezone.utc)
            with self.assertLogs(publish.LOG, "WARNING"):
                self.assertEqual(self.cycle(application=['{"sources_ok": 1}', RuntimeError("summary unavailable")])["warnings"], ["weekly_summary: summary unavailable"])
            (weekly / "latest.md").mkdir(parents=True)
            with self.assertLogs(publish.LOG, "WARNING"):
                self.assertIn("Is a directory", self.cycle(application=app_fn)["warnings"][0])
            (weekly / "latest.md").rmdir()
            with patch("publish.os.fsync", side_effect=[None, None, OSError(errno.ENOSPC, "No space left on device")]), \
                    self.assertLogs(publish.LOG, "WARNING"):
                self.assertIn("No space left on device", self.cycle(application=app_fn)["warnings"][0])
            self.assertEqual([p.name for p in weekly.iterdir()], ["latest.md"])
            self.assertFalse(self.cycle(application=app_fn)["degraded"])
        self.assertEqual(publish.read_json(weekly / "2026-W46.md"), json.loads(summary))

        for child in weekly.iterdir():
            child.unlink()
        self.p.cfg["app_python"] = sys.executable
        self.p.weekly_summary()
        self.assertEqual(json.loads((weekly / "latest.md").read_text())["feed"]["path"], "feed.xml")

        calls = []
        with patch("publish.subprocess.run", side_effect=lambda args, **kw: (calls.append((args, kw)), subprocess.CompletedProcess(args, 0, stdout='MAILTO=""\n', stderr=""))[1]):
            self.p.schedule()
            self.p.command(["whoami"])
        self.assertIn("| logger -t onionflash", calls[1][1]["input"])
        self.assertTrue(calls[2][1]["env"]["WRANGLER_LOG_PATH"].endswith("wrangler-logs"))

    def test_private_spaces_backups_privacy_probes_cleanup_and_retention(self):
        """Verify locked SQLite backup snapshots, anonymous 403 enforcement, cleanup, and pruning."""
        with patch("boto3.client") as unused, self.assertRaisesRegex(ValueError, "Spaces backups are disabled"):
            self.p.execute("backup", force=True)
        unused.assert_not_called()

        self.seed_backup()
        from onionflash.store import backup_database
        client = FakeS3()
        def locked_snapshot(source, destination):
            """Assert the database lock is held shared during backup_database and copy the DB."""
            with self.assertRaises(publish.BusyLock):
                with publish.lock(str(self.p.database) + ".lock", nonblocking=True):
                    pass
            return backup_database(source, destination)
        with patch("boto3.client", return_value=client), self.anonymous(403), \
                patch("publish.backup_database", side_effect=locked_snapshot), patch("publish.datetime") as clock:
            clock.now.return_value = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)
            self.p.backup()
            self.p.backup()
            self.assertEqual(len(client.uploads), 1)
            self.p.backup(force=True)
            self.p.cfg["spaces"]["bucket"] = "another-private-bucket"
            self.p.backup()
        self.assertEqual(len(client.uploads), 3)
        bucket, key, body, acl = client.uploads[0]
        self.assertEqual((acl, self.checked_urls[0]), ("private", f"{self.p.cfg['spaces']['endpoint_url']}/{bucket}/{key}"))
        self.assertNotEqual(key, client.uploads[1][1])
        restored = self.directory / "restored.sqlite3"
        restored.write_bytes(gzip.decompress(body))
        with closing(sqlite3.connect(restored)) as database:
            self.assertEqual(database.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(database.execute("SELECT value FROM evidence").fetchone()[0], "private source evidence")

        (self.p.state / "backup.json").unlink()
        for code, message, requests in [(None, "publicly readable", 1), (404, "unexpected anonymous response 404", 1),
                                        (URLError("reset"), "could not be checked", self.p.net["attempts"])]:
            with self.subTest(code=code):
                c = FakeS3()
                with patch("boto3.client", return_value=c), self.anonymous(code), self.assertRaisesRegex(ValueError, message):
                    self.p.backup(force=True)
                self.assertEqual((len(self.checked_urls), c.deleted), (requests, [c.uploads[0][:2]]))

        for first in (503, 429, 408, URLError("reset")):
            with self.subTest(first=first):
                c = FakeS3()
                with patch("boto3.client", return_value=c), self.anonymous(first, 403):
                    self.p.backup(force=True)
                    self.p.delay.assert_called_once_with(0)
                self.assertEqual((len(self.checked_urls), c.deleted), (2, []))

        c = FakeS3(cleanup_error=True)
        with patch("boto3.client", return_value=c), self.anonymous(None), \
                self.assertRaisesRegex(ValueError, "publicly readable, and deleting it failed: .*cleanup unavailable") as caught:
            self.p.backup(force=True)
        self.assertIn(c.uploads[0][1], str(caught.exception))

        for cleanup_err, exc_type in ((False, ConnectionError), (True, RuntimeError)):
            c = FakeS3(cleanup_error=cleanup_err)
            c.on_put = MagicMock(side_effect=ConnectionError("response lost"))
            with self.subTest(cleanup_err=cleanup_err), patch("boto3.client", return_value=c), self.anonymous(403), \
                    self.assertRaises(exc_type) as caught:
                self.p.backup(force=True)
            self.assertEqual(c.deleted, [c.uploads[0][:2]])
            if cleanup_err:
                self.assertIn(c.uploads[0][1], str(caught.exception))

        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        listed = [{"Key": "onionflash/backups/20200101T000000Z-0123abcd.sqlite3.gz", "LastModified": old},
                  {"Key": "onionflash/backups/20200101T000000Z-" + "a" * 12 + ".tar.gz", "LastModified": old},
                  {"Key": "onionflash/backups/notes.txt", "LastModified": old}]
        c = FakeS3(listed=listed)
        with patch("boto3.client", return_value=c), self.anonymous(403):
            self.p.backup(force=True)
        self.assertEqual([k for _, k in c.deleted], [item["Key"] for item in listed[:2]])
        with patch("boto3.client", return_value=FakeS3(retention_error=True)), self.anonymous(403), \
                self.assertRaisesRegex(RuntimeError, "retention"):
            self.p.backup(force=True)

    def test_static_publish_prunes_untracked_leftover_pages_and_preserves_archive_receipts(self):
        """Prune stale untracked static pages during staging while keeping archive receipts."""
        self.seed_public()
        self.add_article("live")
        for extra in ("news/leftover/index.html", "page/9/index.html"):
            target = self.p.public / extra
            target.parent.mkdir(parents=True)
            target.write_bytes(b"<p>stale</p>")
        archive_receipt_id = "44444444-4444-4444-8444-444444444444"
        publish.atomic_json(self.p.state / "receipts" / (archive_receipt_id + ".json"),
                            {**receipt(archive_receipt_id), "archive": {"generation": "1", "manifest_sha256": "a" * 64, "origin": "https://example.org/public/"}})
        self.fake_cloudflare(OLD)
        first = self.p.publish()
        self.assertFalse({"news/leftover/index.html", "page/9/index.html"} & set(first["manifest"]))
        self.assertEqual(publish.read_json(self.p.state / "permalinks.json"), ["news/live/index.html"])
        self.assertTrue((self.p.state / "receipts" / (archive_receipt_id + ".json")).is_file())
        self.live[0] = NEW
        self.assertEqual(self.p.publish()["version_id"], NEW)
        with self.assertRaisesRegex(ValueError, "untracked file"):
            self.p.publication_readiness({**first["manifest"], "news/leftover/index.html": "b" * 64})

    def test_backup_cleanup_on_base_exception_http_exception_and_missing_key(self):
        """Delete unverified backups on KeyboardInterrupt or BadStatusLine and ignore NoSuchKey cleanup."""
        self.seed_backup()

        c = FakeS3()
        c.on_put = MagicMock(side_effect=KeyboardInterrupt)
        with patch("boto3.client", return_value=c), self.anonymous(403), self.assertRaises(KeyboardInterrupt):
            self.p.backup(force=True)
        self.assertEqual((c.deleted, c.objects), ([c.uploads[0][:2]], {}))

        c = FakeS3()
        with patch("boto3.client", return_value=c), self.anonymous(KeyboardInterrupt()), self.assertRaises(KeyboardInterrupt):
            self.p.backup(force=True)
        self.assertEqual((len(self.checked_urls), c.deleted, c.objects), (1, [c.uploads[0][:2]], {}))

        c = FakeS3()
        with patch("boto3.client", return_value=c), self.anonymous(BadStatusLine("truncated")), \
                self.assertRaisesRegex(ValueError, r"could not be checked for privacy \(BadStatusLine\)"):
            self.p.backup(force=True)
        self.assertEqual((len(self.checked_urls), c.deleted, c.objects), (self.p.net["attempts"], [c.uploads[0][:2]], {}))

        c = FakeS3()
        c.on_put = MagicMock(side_effect=ConnectionError("upload aborted"))
        c.delete_object = MagicMock(side_effect=ClientError({"Error": {"Code": "NoSuchKey"}}, "DeleteObject"))
        with patch("boto3.client", return_value=c), self.anonymous(403), \
                self.assertRaisesRegex(ConnectionError, "upload aborted"):
            self.p.backup(force=True)
        c.delete_object.assert_called_once()

        c = FakeS3()
        c.on_put = MagicMock(side_effect=ConnectionError("upload aborted"))
        c.delete_object = MagicMock(side_effect=ClientError({"Error": {"Code": "AccessDenied"}}, "DeleteObject"))
        with patch("boto3.client", return_value=c), self.anonymous(403), \
                self.assertRaisesRegex(RuntimeError, "upload aborted.*AccessDenied") as denied:
            self.p.backup(force=True)
        self.assertIsInstance(denied.exception.__cause__, ConnectionError)

    def test_run_cycle_skips_all_downstream_phases_when_app_lock_busy(self):
        """Skip publication, backup, retention, and weekly summary when onionflash run reports already-running."""
        self.seed_public()
        self.p.cfg["spaces"].update(enabled=True, required=True)
        self.p.cfg["archive"]["enabled"] = True
        self.p.cfg["retention"]["enabled"] = True
        with publish.lock(str(self.p.database) + ".lock", nonblocking=True), \
                patch.object(self.p, "retain_private_history") as retain_mock, \
                patch.object(self.p, "weekly_summary") as weekly_mock, \
                self.assertLogs(publish.LOG, "WARNING"):
            res = self.cycle(application='{"status": "already-running"}')
            with patch.object(self.p, "application", return_value='{"status": "already-running"}') as cli_app_mock, \
                    patch.object(self.p, "publish") as cli_pub_mock, \
                    patch.object(self.p, "backup") as cli_bkp_mock:
                code, _ = self.run_cli("run")
        self.pub_mock.assert_not_called()
        self.bkp_mock.assert_not_called()
        cli_pub_mock.assert_not_called()
        cli_bkp_mock.assert_not_called()
        retain_mock.assert_not_called()
        weekly_mock.assert_not_called()
        self.app_mock.assert_called_once_with("run")
        cli_app_mock.assert_called_once_with("run")
        self.assertEqual(res["report"], {"status": "already-running"})
        self.assertEqual(res["phases"], {
            "collection": "busy",
            "publication": "skipped",
            "backup": "skipped",
            "retention": "skipped",
            "weekly_summary": "skipped",
        })
        self.assertEqual((res["ok"], res["degraded"], res["errors"], code), (True, True, [], 0))
        self.assertTrue(any("locked by another process" in w for w in res["warnings"]))
        last_run = publish.read_json(self.p.state / "last-run.json")
        self.assertEqual((last_run["ok"], last_run["degraded"], last_run["phases"]["collection"]), (True, True, "busy"))

    def test_upload_metadata_validates_version_uuid_before_preview_verify(self):
        """Ignore non-dict NDJSON lines and reject malformed uploaded version IDs before preview checks."""
        self.seed_public()
        self.fake_cloudflare(OLD)
        preview = "https://candidate-news.example.workers.dev"

        def bad_upload(arguments, metadata=None):
            """Write non-dict NDJSON records followed by an invalid version_id."""
            self.calls.append(" ".join(arguments[:2]))
            Path(metadata).write_text(
                "[]\n123\n" + json.dumps({"type": "version-upload", "version_id": "../bad-version", "preview_url": preview}) + "\n"
            )
            return ""

        self._patch("command", side_effect=bad_upload)
        with self.assertRaisesRegex(ValueError, "Cloudflare returned an invalid version identity"):
            self.p.publish()
        self.assertEqual(self.calls, ["versions upload"])


if __name__ == "__main__":
    unittest.main()
