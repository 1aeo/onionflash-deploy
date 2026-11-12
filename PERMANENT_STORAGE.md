# Permanent storage behind Cloudflare

Published articles have no age or count expiry; the application's
[permanent publication](https://github.com/1aeo/onionflash/blob/main/docs/PERMANENT_HISTORY.md)
guide describes its pages and feeds.

Each article page and its public JSON (`news/<id>/` and `news/<id>/story.json`)
is uploaded once to an immutable object in Spaces, and a small sharded manifest
maps those paths to content hashes. Every other public file, from the front page,
numbered history and topic pages to the feeds and shared assets, ships in the
Worker's own deployment bundle. A Worker version binds its bundle and one
immutable root manifest, so rollback restores the same listings and selects the
same historical articles rather than reading files overwritten by a later run.

The Worker routes only article paths to storage. It never receives Spaces
credentials or exposes the private prefix, and it serves an object only if its
bytes match the manifest's hash. Browser URLs such as
`https://example.com/onionflash/news/<id>/` do not change.

## Set up a bucket

1. Create a **Standard Storage** bucket, for example `onionflash-data` in NYC3
   if that globally unique name is available. Otherwise choose an available name
   you own and set both `bucket` and `public_origin` in the `[archive]` section.
   Keep bucket listing restricted/private. Cloudflare provides the public edge;
   enabling the Spaces CDN is unnecessary.
2. Grant a Spaces access key **Read/Write/Delete** for this bucket. A limited key
   can operate on an existing bucket but cannot create one. Bucket creation
   therefore needs the control panel or a suitable full-access credential.
3. Keep the key in `secrets.env`. The `[archive]` section names the backup key's
   `DO_SPACES_KEY` and `DO_SPACES_SECRET`; for a dedicated key, add its two
   variables under other names, such as `ONIONFLASH_ARCHIVE_KEY` and
   `ONIONFLASH_ARCHIVE_SECRET`, and set `access_key_env` and `secret_key_env` to
   them. Do not commit credentials or put them in Worker variables.
4. Copy the `[archive]` section from `deployment.example.toml`, adjust it as
   above, and set `enabled = true`.

Only verified published objects and routing manifests receive a public-read
object ACL. Raw source bodies, model evidence and database backups remain
private: an unsigned request for any such object must be refused
(see [backups](README.md#digitalocean-spaces)). Storage checks every
upload against its Content-MD5, and an object that already exists must match its
size, sha256 metadata and ETag; unknown or corrupt objects stop publication
rather than being overwritten. When a public object or manifest is uploaded, or
an upload finds it already stored, it must also answer an unsigned request at
`public_origin`, or publication stops: check that no bucket policy denies
anonymous reads and that `public_origin` names this bucket and prefix. Objects
kept from earlier runs are not checked again, and the publisher never changes the
access of an object that already exists.

## Migrate and publish

Use a core version that supports permanent publication and its current deployer.
Back up first, and keep a private copy of the configuration too.

```sh
./deploy.sh check
./deploy.sh backup --force   # SQLite only; needs spaces.enabled = true, otherwise back up the database yourself
./deploy.sh run
./deploy.sh status
```

`check` validates application settings, the `[retention]` keys, the installed
toolchain, and enabled archive settings/credential presence. It does not create
a bucket or prove that the credential has access to remote storage; the
publication checks do that.

For a large initial migration, repeat runs while `articles_pending` or archive
pending counts remain. Existing article routes are not switched until previously
published pages have verified permanent copies. A partial build, generation
change, failed upload or exhausted work budget retains the existing publication.

Each run uploads only new or changed article files and the manifest shards that
list them, so permanent storage grows with the articles themselves. Listings and
feeds change with every story; they travel in the Worker bundle, which keeps no
permanent copies. Asset, Worker-code and runtime binding changes all participate
in deployment identity. The preview and live checks also fetch one article
through the Worker, so a wrong origin or root fails them.

Publication, rollback and recovery work as described in the
[README](README.md#publication).

## Private evidence retention

Optional `[retention]` moves two kinds of private data to private storage after
90 days: analysis cache rows, and the source bodies of ignored items, other than
GitLab issues, that no story uses. Every other item, including published ones,
keeps its source body. Enable it only after `[archive]` is working. Scheduled runs
sweep once a day; a batch cut short by its row or byte limit continues on the
next run until the backlog drains. A sweep first writes a compressed private
bundle, reads it back and checks its privacy as above. It then compacts only
rows that have not changed meanwhile, so concurrent edits are preserved. A final
review whose source survives only as an analysis snapshot older than 90 days
holds its update as missing source material.

Item IDs, source URLs, content hashes, timestamps, story mappings, retry state,
published stories, last-good public article JSON and incident deduplication state
are retained in SQLite. `private_archive_refs` records the original bucket,
endpoint, region, object key and hash so archived material remains findable
after storage settings change. Future analysis rows preserve the collected source snapshot;
legacy rows are archived as they exist, without fabricating missing inputs.

Publication receipts stay in `state/receipts/`. Remote private evidence,
immutable public objects and article history have no automatic expiry.

Routine private backups contain only a gzip-compressed SQLite snapshot, including
the permanent publication catalog and last-good public story JSON. They do not
repeatedly copy generated pages or the public object archive. Keep deployment
configuration, publication receipts and `state/permalinks.json` securely
alongside the separately managed credentials. After a database restore, article
files are rebuilt from the last-good public snapshots, as the README's
[recovery](README.md#digitalocean-spaces) steps describe.

## Limits and failures

Per-run object, byte and time limits and the manifest size limit protect
resources. They do not delete published news. `archive-sync.json`,
`retention.json`, the normal run receipt and system logs expose incomplete or
failed work. An object replaced in storage gets a 503 until it is restored and
its edge-cached copy expires or is purged.

The canonical browser cache TTL is short; immutable origin objects and manifests
can be cached for much longer.
