# Permanent storage behind Cloudflare

Published articles have no age or count expiry. The homepage is paginated, and
`site.feed_size = 0` includes the complete history in RSS, JSON Feed and topic
feeds. A separate cold-news archive is unnecessary.

Cloudflare keeps `index.html` and the application's shared assets. Article
HTML/public JSON, numbered pages and feeds are uploaded to immutable objects in
Spaces. A small sharded manifest maps their ordinary paths to content hashes.
The Worker version binds one immutable root manifest, so rollback selects the
same historical objects rather than reading files overwritten by a later run.

The Worker routes only known public article, pagination and feed paths. It never
receives Spaces credentials or exposes the private prefix. Browser URLs such as
`https://example.com/onionflash/news/<id>/` do not change.

## Set up a bucket

1. Create a **Standard Storage** bucket, for example `onionflash-data` in NYC3
   if that globally unique name is available. Otherwise choose an available name
   you own and update both `archive.bucket` and `archive.public_origin` below.
   Keep bucket listing restricted/private. Cloudflare provides the public edge;
   enabling the Spaces CDN is unnecessary.
2. Grant a Spaces access key **Read/Write/Delete** for this bucket. A limited key
   can operate on an existing bucket but cannot create one. Bucket creation
   therefore needs the control panel or a suitable full-access credential.
3. Keep credentials in the existing protected dotenv file, or copy
   `archive-secrets.env.example` to `archive-secrets.env` and set mode 600.
   Use distinct environment names when the existing backup bucket has a
   different key. Do not commit credentials or put them in Worker variables.
4. Copy the complete `[archive]` section from `deployment.example.toml` and set:

```toml
[archive]
enabled = true
env_file = "archive-secrets.env"
endpoint_url = "https://nyc3.digitaloceanspaces.com"
region = "nyc3"
bucket = "onionflash-data"
public_prefix = "onionflash/public/"
private_prefix = "onionflash/private/"
public_origin = "https://onionflash-data.nyc3.digitaloceanspaces.com/onionflash/public/"
access_key_env = "ONIONFLASH_ARCHIVE_KEY"
secret_key_env = "ONIONFLASH_ARCHIVE_SECRET"
# Retain the bounded upload, manifest, timeout and retry settings from the example.
```

Only verified published objects and routing manifests receive a public-read
object ACL. Raw source bodies, model evidence, database backups and operational
receipts remain private. The publisher checks object hashes, sizes and ACLs;
unknown or corrupt objects stop publication rather than being overwritten.

## Migrate and publish

Use a core version that supports permanent publication and its current deployer.
Back up SQLite and configuration first. In the application config, set
`site.feed_size = 0`, keep `site.page_size` as the desired page length and remove
the retired `storage.max_stories` setting. `storage.render_batch_size` defaults
to 250 and controls work per build, not retention.

```sh
./deploy.sh check
./deploy.sh backup --force
./deploy.sh run
./deploy.sh status
```

`check` validates application settings, the installed toolchain, and enabled
archive settings/credential presence. It does not create a bucket or prove that
the credential has access to remote storage; the publication checks do that.

For a large initial migration, repeat runs while `articles_pending` or archive
pending counts remain. Existing article routes are not switched until previously
published pages have verified permanent copies. A partial build, generation
change, failed upload or exhausted work budget retains the existing publication.

On ordinary runs, only changed objects and affected manifest shards upload.
Unchanged articles are reused. Feeds are streamed with bounded memory. A hot-only
front-page change reuses the permanent manifest. Asset, Worker-code and runtime
binding changes all participate in deployment identity.

The publisher uses one linear upload, preview-check, activation and live-check
flow. Small per-version receipts retain the immutable root for rollback; there
is no transition journal or `reconcile` command. An interrupted attempt clears
the unchanged-content shortcut. Pre-activation failures retry normally; an
unrecorded live-version change stops for operator inspection so an external
deployment cannot be overwritten by an automatic retry.
Feed routing is verified by its immutable hash/ETag without buffering
an entire growing feed again. The upload itself is read back and hash-checked.

## Private evidence and receipt retention

Optional `[retention]` defaults keep 90 days of source bodies/analysis cache and
30 days of publication receipts locally. Completed sweeps run daily; full
batches continue on subsequent scheduled runs until the backlog drains.
Enable it only after `[archive]` is
working. The job first writes a compressed private bundle and verifies its bytes
and private ACL. It then conditionally compacts unchanged completed rows/files.
Pending items and concurrent edits are preserved.

Item IDs, source URLs, content hashes, timestamps, story mappings, retry state,
published stories, last-good public article JSON and incident deduplication state
are retained in SQLite. `private_archive_refs` records the original bucket,
endpoint, region, object key and hash so archived material remains findable
after storage settings change. Future analysis rows preserve the collected source snapshot;
legacy rows are archived as they exist, without fabricating missing inputs.

Current/previous publication receipts and receipts referenced by holds or
rollback state are pinned. Legacy transition records from older installations
are also honored when deciding what to retain. Remote private evidence, immutable public
objects and article history have no automatic expiry.

```sh
./deploy.sh retention   # Run one bounded private-retention batch explicitly.
```

Routine private backups contain only a gzip-compressed SQLite snapshot, including
the permanent publication catalog and last-good public story JSON. An unsigned
request must confirm the backup object is private. They do not repeatedly copy
generated pages or the public object archive. Keep deployment configuration and
publication receipts securely alongside the separately managed credentials. A
database restore can rebuild article files from the last-good public snapshots
with `onionflash build --config onionflash.toml --force`; repeat while the bounded
rebuild reports `articles_pending`. No LLM calls are required for reconstruction.

## Limits and failures

Per-run object/byte/time limits and parser/manifest limits protect resources.
They do not delete published news. If a sharded manifest becomes too large,
increase `shard_prefix_chars` within its supported range and prepare a new
manifest; older roots remain available. `archive-sync.json`, `retention.json`,
the normal run receipt and system logs expose incomplete or failed work.

RSS responses disable scripts while allowing the browser XML viewer's inline
styling. HTML keeps the application's existing CSP. The canonical browser cache
TTL is short; immutable origin objects and manifests can be cached for much longer.
