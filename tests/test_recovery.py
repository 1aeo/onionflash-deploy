"""Failure-path checks for owned deployment transitions and private backups."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, redirect_stdout
from datetime import datetime, timezone
import errno, gzip, io, json, os, re, sqlite3, subprocess, sys, unittest
from pathlib import Path
from unittest.mock import ANY, MagicMock, call, patch
from urllib.error import HTTPError, URLError

import publish
from tests.fakes import DeployTestCase, FakeS3, NEW, OLD, OTHER


def receipt(version, previous=None):
    return {"version_id": version, "previous_version_id": previous, "content_hash": version,
            "published_at": "2026-11-09T10:00:00Z", "manifest": {"index.html": version}}


class RecoveryTests(DeployTestCase):
    def _patch(self, name, **kwargs):
        patcher = patch.object(self.p, name, **kwargs)
        patcher.start()
        self.addCleanup(patcher.stop)

    def fake_cloudflare(self, live, *, failing_check=None):
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
        def verify(url, manifest, archive=None):
            self.calls.append("verify " + ("preview" if url.startswith(preview) else "live"))
            if failing_check and self.calls[-1] == "verify " + failing_check:
                raise ValueError("check failed")
        self._patch("command", side_effect=command)
        self._patch("activate", side_effect=lambda version, config: (self.calls.append("activate " + version), self.live.__setitem__(0, version)))
        self._patch("verify", side_effect=verify)
        self._patch("current_version", side_effect=lambda: self.live[0])
        self._patch("origin_url", return_value="https://news.example.workers.dev/onionflash")
        self._patch("app_version", return_value="test")

    def seed_backup(self):
        self.p.cfg["spaces"].update(enabled=True)
        with closing(sqlite3.connect(self.p.database)) as db:
            db.execute("CREATE TABLE evidence (value TEXT)")
            db.execute("INSERT INTO evidence VALUES ('private source evidence')")
            db.commit()
        self.seed_public()
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))

    def anonymous(self, *codes):
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

    def test_publish_preview_activation_and_receipt_pruning(self):
        self.seed_public()
        publish.atomic_json(self.p.state / "receipts" / (OTHER + ".json"), receipt(OTHER))
        self.fake_cloudflare(OLD)
        result = self.p.publish()
        self.assertEqual(self.calls, ["versions upload", "verify preview", "activate " + NEW, "verify live"])
        self.assertEqual((result["version_id"], result["previous_version_id"]), (NEW, OLD))
        self.assertEqual({p.name for p in (self.p.state / "receipts").iterdir()}, {OLD + ".json", NEW + ".json"})
        self.assertEqual(publish.read_json(self.p.state / "receipts" / (OLD + ".json")), receipt(OLD))

    def test_permalinks_permanence_bootstrap_rollback_interrupt_and_damage(self):
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
        with closing(sqlite3.connect(self.p.database)) as db, db:
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
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))
        self.live[0] = OLD
        write = publish.atomic_json
        with patch("publish.atomic_json", side_effect=lambda path, val: (_ for _ in ()).throw(KeyboardInterrupt) if Path(path).name == NEW + ".json" else write(path, val)), \
                self.assertRaises(KeyboardInterrupt):
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
        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))
        self.fake_cloudflare(OLD, failing_check="live")
        with self.assertRaisesRegex(ValueError, "check failed"):
            self.p.publish()
        self.assertEqual((self.calls[-2:], self.live[0], publish.read_json(self.p.state / "permalinks.json")),
                         (["verify live", "activate " + OLD], OLD, []))
        self.assertFalse((self.p.state / "publication-hold.json").exists())

        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))
        def fail_restore(version, config):
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
                publish.atomic_json(self.p.state / "publication.json", recorded)
                self.fake_cloudflare(OTHER)
                with self.assertRaisesRegex(ValueError, f"differs.*live {OTHER}, recorded {recorded.get('version_id')}"):
                    self.p.publish()
                self.assertEqual((self.calls, self.live[0]), ([], OTHER))

        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))
        self.fake_cloudflare(OLD)
        with patch.object(self.p, "verify", side_effect=lambda *a, **k: self.live.__setitem__(0, OTHER)), \
                self.assertRaisesRegex(ValueError, "changed during preview"):
            self.p.publish()
        self.assertEqual(self.live[0], OTHER)

        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))
        self.fake_cloudflare(OLD)
        def ext_live(url, manifest, archive=None):
            if "candidate-" not in url:
                self.live[0] = OTHER
                raise ValueError("external deployment")
        with patch.object(self.p, "verify", side_effect=ext_live), self.assertRaisesRegex(RuntimeError, "external version retained"):
            self.p.publish()
        self.assertEqual(self.live[0], OTHER)

        publish.atomic_json(self.p.state / "publication.json", receipt(OLD))
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
        self.seed_public()
        self.fake_cloudflare(None)
        first = self.p.publish()
        self.assertEqual((self.calls, first["version_id"], first["previous_version_id"]), (["deploy --config", "verify live"], NEW, None))

        for path in ("publication.json", "permalinks.json"):
            (self.p.state / path).unlink(missing_ok=True)
        self.fake_cloudflare(None, failing_check="live")
        with self.assertRaisesRegex(ValueError, "check failed"):
            self.p.publish()
        self.assertEqual(publish.read_json(self.p.state / "publication.json"), {"version_id": NEW})
        self.fake_cloudflare(NEW)
        self.p.publish()
        self.assertEqual(self.calls, ["versions upload", "verify preview", "activate " + NEW, "verify live"])

        (self.p.state / "publication.json").unlink()
        self.fake_cloudflare(None)
        with patch.object(self.p, "command", side_effect=lambda *a, **k: (self.live.__setitem__(0, NEW), (_ for _ in ()).throw(RuntimeError("deploy reported")))), \
                self.assertRaisesRegex(RuntimeError, "deploy reported"):
            self.p.publish()
        self.assertEqual(publish.read_json(self.p.state / "publication.json"), {"version_id": NEW})

        (self.p.state / "publication.json").unlink()
        self.fake_cloudflare(None)
        lookups = iter([None])
        with patch.object(self.p, "command", side_effect=RuntimeError("deploy reported")), \
                patch.object(self.p, "current_version", side_effect=lambda: next(lookups) if lookups else (_ for _ in ()).throw(RuntimeError("lookup failed"))), \
                self.assertRaisesRegex(RuntimeError, "deploy reported"):
            self.p.publish()

        old = dict(receipt(OLD), published_at="2026-11-08T10:00:00Z",
                   archive={"manifest_sha256": "a" * 64, "generation": "1", "origin": "https://example.org/public/"})
        publish.atomic_json(self.p.state / "receipts" / (OLD + ".json"), old)
        publish.atomic_json(self.p.state / "publication.json", receipt(NEW, OLD))
        held_during_activate = []
        with patch.object(self.p, "activate", side_effect=lambda v, c: held_during_activate.append((self.p.state / "publication-hold.json").is_file())), \
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
        with patch.object(self.p, "application", side_effect=[ValueError("collection failed"), "{}"]) as app, \
                patch.object(self.p, "publish") as pub, patch.object(self.p, "backup") as bkp, self.assertLogs(publish.LOG, "WARNING"):
            res = self.p.run_cycle()
        pub.assert_not_called()
        bkp.assert_called_once()
        self.assertEqual((res["ok"], res["phases"]["publication"], app.call_args_list),
                         (False, "skipped", [call("run"), call("decisions", "summary", "--since", "7d")]))

        with patch.object(self.p, "application", return_value='{"sources_ok": 1}'), \
                patch.object(self.p, "publish", side_effect=ValueError("Cloudflare unavailable")), patch.object(self.p, "backup") as bkp:
            res = self.p.run_cycle()
        bkp.assert_called_once()
        self.assertEqual((res["ok"], res["phases"]["publication"], res["phases"]["backup"]), (False, "failed", "ok"))

        publish.atomic_json(self.p.state / "publication-hold.json", {"reason": "operator hold"})
        with patch.object(self.p, "application", return_value='{"sources_ok": 1}'), \
                patch.object(self.p, "publish") as pub, patch.object(self.p, "backup") as bkp:
            res = self.p.run_cycle()
        pub.assert_not_called()
        bkp.assert_called_once()
        self.assertEqual((res["ok"], res["phases"]["publication"]), (True, "held"))
        (self.p.state / "publication-hold.json").unlink()

        with patch.object(self.p, "application", return_value='{"sources_ok": 1}'), patch.object(self.p, "publish"), \
                patch.object(self.p, "backup", side_effect=ValueError("Spaces unavailable")):
            res = self.p.run_cycle()
        self.assertEqual((res["ok"], res["phases"]["backup"], bool(res["warnings"])), (True, "failed", True))

        failed = subprocess.CompletedProcess([], 2, stdout="", stderr="onionflash: Output ownership marker does not match")
        with patch("publish.subprocess.run", return_value=failed), patch.object(self.p, "publish"), patch.object(self.p, "backup"):
            res = self.p.run_cycle()
        self.assertIn("Output ownership marker", res["errors"][0])
        with patch("publish.subprocess.run", side_effect=subprocess.TimeoutExpired(["onionflash"], 540)), \
                patch.object(self.p, "publish") as pub, patch.object(self.p, "backup") as bkp:
            res = self.p.run_cycle()
        self.assertIn("timed out", res["errors"][0])
        pub.assert_not_called()
        bkp.assert_called_once()

        done = subprocess.CompletedProcess([], 0, stdout='{"sources_ok": 15, "sources_failed": 3}\n', stderr="")
        with patch("publish.subprocess.run", return_value=done), patch.object(self.p, "publish"), patch.object(self.p, "backup"):
            self.assertEqual(self.p.run_cycle()["report"], {"sources_ok": 15, "sources_failed": 3})

        for cmd, expected in [("hold", 75), ("rollback", 75), ("check", 75), ("run", 0)]:
            with self.subTest(command=cmd), publish.lock(self.p.state / "run.lock"), \
                    patch("publish.Publisher", return_value=self.p), patch("sys.argv", ["publish.py", cmd]):
                self.assertEqual(publish.main(), expected)
        with publish.lock(self.p.state / "run.lock", shared=True), patch("publish.Publisher", return_value=self.p), \
                patch("sys.argv", ["publish.py", "status"]), redirect_stdout(io.StringIO()) as out:
            self.assertEqual(publish.main(), 0)
        self.assertIn('"running": false', out.getvalue())
        self.assertFalse((self.p.state / "run.log").exists())

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
        with patch("publish.Publisher", return_value=self.p), patch.object(self.p, "application", return_value='{"configuration": "valid"}'), \
                patch.object(self.p, "node", Path(__file__)), patch.object(self.p, "wrangler", Path(__file__)), \
                patch("sys.argv", ["publish.py", "check"]), redirect_stdout(io.StringIO()):
            self.assertEqual(publish.main(), 0)
        self.assertNotIn("error", publish.read_json(self.p.state / "last-run.json"))

    def test_weekly_summary_and_schedule_logging(self):
        self.addCleanup(os.umask, os.umask(0o022))
        weekly, sizes = self.p.state / "weekly", []
        def app(*arguments):
            if arguments == ("run",):
                return '{"sources_ok": 1}'
            sizes.append(len(sizes) + 1)
            return '{"feed": {"bytes": %d}, "title": "\\u202e"}' % sizes[-1]
        with patch.object(self.p, "application", side_effect=app), patch.object(self.p, "publish"), \
                patch.object(self.p, "backup"), patch("publish.datetime") as clock:
            for day in (datetime(2026, 12, 31), datetime(2027, 1, 3), datetime(2027, 1, 4)):
                clock.now.return_value = day.replace(tzinfo=timezone.utc)
                self.assertEqual(self.p.run_cycle()["phases"]["weekly_summary"], "ok")
        self.assertEqual((sizes, sorted(p.name for p in weekly.iterdir())), ([1, 2], ["2026-W53.md", "2027-W01.md", "latest.md"]))
        self.assertEqual((weekly / "latest.md").stat().st_mode & 0o777, 0o600)

        for child in weekly.iterdir():
            child.unlink()
        failures, summary = [RuntimeError("summary unavailable")], '{"writer_clears": 0}'
        with patch.object(self.p, "application", side_effect=lambda *a: '{"sources_ok": 1}' if a == ("run",) else ( (_ for _ in ()).throw(failures.pop()) if failures else summary)), \
                patch.object(self.p, "publish"), patch.object(self.p, "backup"), patch("publish.datetime") as clock:
            clock.now.return_value = datetime(2026, 11, 11, tzinfo=timezone.utc)
            with self.assertLogs(publish.LOG, "WARNING"):
                self.assertEqual(self.p.run_cycle()["warnings"], ["weekly_summary: summary unavailable"])
            (weekly / "latest.md").mkdir(parents=True)
            with self.assertLogs(publish.LOG, "WARNING"):
                self.assertIn("Is a directory", self.p.run_cycle()["warnings"][0])
            (weekly / "latest.md").rmdir()
            with patch("publish.os.fsync", side_effect=[None, None, OSError(errno.ENOSPC, "No space left on device")]), \
                    self.assertLogs(publish.LOG, "WARNING"):
                self.assertIn("No space left on device", self.p.run_cycle()["warnings"][0])
            self.assertEqual([p.name for p in weekly.iterdir()], ["latest.md"])
            self.assertFalse(self.p.run_cycle()["degraded"])
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
        with patch("boto3.client") as unused, self.assertRaisesRegex(ValueError, "Spaces backups are disabled"):
            self.p.execute("backup", force=True)
        unused.assert_not_called()

        self.seed_backup()
        from onionflash.store import backup_database
        client = FakeS3()
        def locked_snapshot(source, destination):
            with self.assertRaises(publish.BusyLock):
                with publish.lock(str(self.p.database) + ".lock", nonblocking=True):
                    pass
            return backup_database(source, destination)
        class Frozen(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 11, 10, 12, 0, 0, tzinfo=timezone.utc)
        with patch("boto3.client", return_value=client), self.anonymous(403), \
                patch("onionflash.store.backup_database", side_effect=locked_snapshot), patch("publish.datetime", Frozen):
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
            c.on_put = lambda k: (_ for _ in ()).throw(ConnectionError("response lost"))
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


if __name__ == "__main__":
    unittest.main()
