"""Cold-source retention must not invalidate an unfinished final verification."""

import copy
import json
import tomllib
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from onionflash import review, review_gate
from onionflash.analyze import AnalysisError
from onionflash.config import DEFAULTS
from onionflash.store import Store
from retention import RetentionStore
from test_retention import FakeSpaces


OBSERVED = "2026-11-10T12:00:00Z"
OLD = "2026-06-01T00:00:00Z"


def draft():
    return {"publish": True, "title": "Operator reports possible compromise",
            "summary": "The operator reported a plausible compromise and stopped trusting the old keys.",
            "why_it_matters": "Operators must update their relay configurations.",
            "category": "security", "priority": "high", "confidence": "reported", "state": "developing",
            "products": ["tor"], "tags": ["security"], "evidence": ["reported a plausible compromise"]}


def verdict(revise=False):
    criteria = {key: {"outcome": "pass", "source_refs": ["S1"], "reason": "Supported by the supplied report."}
                for key in review.CRITERIA}
    issues = []
    if revise:
        criteria["operational_advice"]["outcome"] = "fail"
        issues = [{"criterion": "operational_advice", "source_refs": ["S1"],
                   "explanation": "The report gives no configuration instruction to relay operators."}]
    return {"verdict": "revise" if revise else "approve", "criteria": criteria, "issues": issues,
            "repair": {"why_it_matters": ""} if revise else None}


class Writer:
    def analyze(self, item):
        return draft()


class Reviewer:
    def __init__(self, *results):
        self.results, self.calls = list(results), []

    def review(self, candidate, packet, *, verification=False):
        self.calls.append(verification)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class ReviewRetentionTests(unittest.TestCase):
    def test_retention_preserves_historical_source_until_verification_resumes(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        state = root / "state"
        state.mkdir()
        cfg = copy.deepcopy(DEFAULTS)
        database = state / "news.sqlite3"
        source = {"source_id": "legacy", "url": "https://example.org/report", "title": "Authority report",
                  "content": "The authority operator reported a plausible compromise and stopped trusting its old keys. " * 3,
                  "published_at": "2026-05-01T00:00:00Z", "updated_at": OLD, "kind": "gitlab_issue",
                  "product": "tor", "authority": "development", "labels": ["Security"]}
        with Store(database) as store:
            store.ingest(source, OLD)
            original = store.pending(1, OBSERVED)[0]
            legacy = store.prepare_candidate(original, draft(), OLD)
            story = legacy["payload"]
            with store.db:
                store.db.execute("INSERT INTO stories VALUES(?,?,?,?,?)", (story["id"], legacy["cluster_key"],
                                 json.dumps(story), story["published_at"], story["updated_at"]))
                store.db.execute("UPDATE items SET status='published',story_id=? WHERE id=?", (story["id"], original["id"]))
                store.record_analysis(original, draft(), "historical-source", OLD)
                cold = json.loads(original["payload"])
                cold.pop("content")
                store.db.execute("UPDATE items SET payload=? WHERE id=?", (json.dumps(cold), original["id"]))
                store.db.execute("INSERT INTO analyses(cache_key,result,created_at,review_record) VALUES(?,?,?,?)",
                                 ("unrelated", '{"publish":false}', OLD, '{"audit":"unrelated completed analysis"}'))
            incoming = {**source, "source_id": "mirror", "url": "https://example.org/mirror", "updated_at": "2026-05-31T00:00:00Z"}
            store.ingest(incoming, OBSERVED)
            pending = store.pending(1, OBSERVED)[0]
            checker = Reviewer(verdict(True), AnalysisError("fixture timeout", retryable=True))
            with self.assertRaises(AnalysisError):
                review_gate.process_item(cfg, store, pending, Writer(), checker, OBSERVED)
            self.assertEqual(checker.calls, [False, True])
            before = tuple(store.db.execute("SELECT stage,record_json FROM review_jobs").fetchone())
            self.assertEqual(before[0], "verify")
            archive = tomllib.loads((Path(__file__).parents[1] / "deployment.example.toml").read_text())["archive"]
            publisher = SimpleNamespace(state=state, database=database, public=root / "public",
                cfg={"retention": {"enabled": True}, "archive": {**archive, "enabled": True,
                     "endpoint_url": "https://s3.example.org", "public_origin": "https://archive.example.org/onionflash/public/"}},
                net={"attempts": 2, "timeout_seconds": 10}, delay=lambda attempt: None,
                unsigned_read_problem=lambda *request: None)
            storage = FakeSpaces()
            retained = RetentionStore(publisher, storage).archive_evidence(datetime(2026, 11, 10, 12, tzinfo=timezone.utc))
            self.assertEqual(retained["analyses_evicted"], 1)
            self.assertTrue(storage.objects)
            self.assertIsNotNone(store.db.execute("SELECT 1 FROM analyses WHERE cache_key='historical-source'").fetchone())
            self.assertIsNone(store.db.execute("SELECT 1 FROM analyses WHERE cache_key='unrelated'").fetchone())
            self.assertEqual(tuple(store.db.execute("SELECT stage,record_json FROM review_jobs").fetchone()), before)
            checker = Reviewer(verdict())
            resumed = review_gate.process_item(cfg, store, pending, Writer(), checker, OBSERVED)
            self.assertEqual(resumed["status"], "published")
            self.assertEqual(resumed["writer_calls"], 0)
            self.assertEqual(resumed["review_calls"], 1)
            self.assertEqual(checker.calls, [True])
            self.assertEqual(store.stories()[0]["why_it_matters"], "")
            self.assertEqual(store.db.execute("SELECT COUNT(*) FROM review_jobs").fetchone()[0], 0)
