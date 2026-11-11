"""Test doubles shared by the deployment tests."""
import base64
import hashlib
import io
import os
from pathlib import Path
import tempfile
from unittest.mock import patch

from botocore.exceptions import ClientError, EndpointConnectionError

import publish

OLD = "11111111-1111-4111-8111-111111111111"
NEW = "22222222-2222-4222-8222-222222222222"
ENV = {"CLOUDFLARE_ACCOUNT_ID": "a" * 32, "CLOUDFLARE_API_TOKEN": "unit-test-not-a-real-secret",
       "CLOUDFLARE_ZONE_TOKEN": "zone-test-not-a-real-secret", "CLOUDFLARE_ZONE_ID": "b" * 32,
       "DO_SPACES_KEY": "spaces-test-not-a-real-key", "DO_SPACES_SECRET": "spaces-test-not-a-real-secret"}


class FakeS3:
    """The object-store calls ArchiveStore makes, keyed by object key."""

    def __init__(self):
        self.objects, self.puts, self.heads = {}, [], []
        self.fail_puts = 0
        self.on_put = None  # called with each key once its object is stored

    def head_object(self, Bucket, Key):
        self.heads.append(Key)
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
            grants.append({"Grantee": {"Type": "Group", "URI": "http://acs.amazonaws.com/groups/global/AllUsers"},
                           "Permission": "READ"})
        return {"Owner": {"ID": "owner"}, "Grants": grants}


def make_publisher(test, output_dir="build/public"):
    """A new private directory with the example deployment.toml, and its Publisher."""
    temporary = tempfile.TemporaryDirectory()
    test.addCleanup(temporary.cleanup)
    directory = Path(temporary.name).resolve()
    environment = patch.dict(os.environ, ENV)
    environment.start()
    test.addCleanup(environment.stop)
    (directory / "deployment.toml").write_text((publish.ROOT / "deployment.example.toml").read_text())
    (directory / "onionflash.toml").write_text(f'''[site]
base_url = "https://example.com/onionflash/"
[storage]
database = "state/news.sqlite3"
output_dir = "{output_dir}"
[[sources]]
id = "test"
url = "https://example.org/feed.xml"
''')
    return directory, publish.Publisher(directory / "deployment.toml")
