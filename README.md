# Onion Flash deployment

Install and publish [Onion Flash](https://github.com/1aeo/onionflash), a small Tor
news aggregator. A scheduled Python job collects sources and calls your LLM;
Cloudflare Workers Static Assets serves the generated website and RSS. Private
DigitalOcean Spaces backups are optional. Visitors never call the LLM.

Production: **https://1aeo.com/onionflash/** · [RSS](https://1aeo.com/onionflash/feed.xml)

## Quick start

Requirements: Linux or macOS, Python **3.11+** with `venv`/pip, Git, and npm.
The installer downloads a repository-local, pinned Node.js/Wrangler toolchain.
Cron is needed only for the optional schedule. Run everything as an ordinary user.

```sh
git clone https://github.com/1aeo/onionflash.git
git clone https://github.com/1aeo/onionflash-deploy.git
cd onionflash-deploy
./install.sh ../onionflash
```

Edit the three generated files (all ignored by Git):

1. **`onionflash.toml`**: website URL, sources, LLM endpoint/model, limits, retries,
   fallback URLs, and SQLite/output paths. For a LAN LLM, explicitly allow its
   exact addresses in `llm.allowed_networks` and set `allow_http = true` only if
   that endpoint uses HTTP. Source fetching remains restricted to public HTTPS.
2. **`secrets.env`**: LLM key, Cloudflare account/token, and optionally Spaces keys.
   Keep mode **600**. Literal `KEY=value` or quoted values are supported; shell
   commands and variable expansion are never executed. Set
   `ONIONFLASH_LLM_BASE_URL` if using the example application's environment reference.
3. **`deployment.toml`**: Worker name, deployment limits, schedule and optional
   Spaces destination. Paths are relative to this file. `spaces.enabled = false`
   is the default. Change the worker name and URL when installing your own copy.

```sh
./deploy.sh check       # Validate configuration and installed tools
./deploy.sh run         # Collect, analyze, build, verify preview, publish
./deploy.sh routes      # Attach only this application's path to your hostname
./deploy.sh schedule    # Install/update this application's cron block
./deploy.sh status      # Show publication, backup and last-run receipts
```

The first run may publish only some stories while its durable analysis queue is
processed. Each later run continues the queue. Processing limits and the run time
budget are configurable. `run` does not attach hostname routes; the initial
publication can be inspected at the origin URL printed by `status` first.

## Cloudflare setup

Use an active, proxied hostname in your Cloudflare account. The API credential
needs Workers Scripts editing and access to the account's Workers subdomain.
`routes` additionally needs the zone ID and a token with Workers Routes editing
and read access. A separate zone token is supported. Existing routes owned by a
different Worker are never replaced.

At `https://example.com/onionflash/`, the assets are mounted at `/onionflash`.
The installed route is `example.com/onionflash*`, because Cloudflare route
matching includes query strings. The tiny Worker redirects `/onionflash` to
`/onionflash/` while preserving its query. It serves the application's subtree,
and **passes sibling paths such as `/onionflash-other` to their original origin**.
Its code and tests explicitly enforce that boundary. For a whole-host deployment,
use a root `site.base_url`, such as `https://news.example.com/`.

Publication uploads an immutable version, checks preview hashes, activates it,
and verifies the origin and (after routing) production hostname. A failed
post-activation check restores the preceding version. Repeated content is not
uploaded again. An unexpectedly changed active version stops publication instead
of overwriting someone else's deployment.

The public package is an exact allowlist of generated pages, feeds and three
assets. SQLite, source evidence, credentials, logs and release receipts stay
outside it. Source timestamps and original first-publication times are preserved.

## DigitalOcean Spaces

Set `spaces.enabled = true`, an HTTPS endpoint, region, bucket and private prefix
in `deployment.toml`. The corresponding environment variable names are configurable.
Use a dedicated bucket or an isolated prefix such as `onionflash/backups/`.
Set `create_bucket = true` only when the credential is authorized to create that
bucket; otherwise create it privately first. Bucket creation is checked before use.

```sh
./deploy.sh backup
```

Backups use SQLite's consistent backup API, validate the database, and package it
with the generated public files and publication receipt. Every object is uploaded
with a **private ACL**, checked for size/hash metadata, and checked for accidental
public/group grants. Defaults are one backup per day and 30 days of retention.
Retention only touches this application's strictly named objects under its backup
prefix; the newest upload is always retained. The SDK uses bounded retries and
timeouts. A backup outage is recorded and retried later without blocking news
publication unless `spaces.required = true`.

For recovery, hold publication, download a selected private archive using an S3
client, compare its SHA-256 with object metadata, and extract it into a new private
directory. Validate the recovered database using `PRAGMA quick_check`. Replace the
database only with the scheduled job stopped and the application lock held, then
run the application's offline `build` before resuming publication. Keep the old
database until recovery is verified. Credentials are never included in backups.

## Operations

```sh
./deploy.sh publish    # Publish already generated output; no LLM call
./deploy.sh hold       # Pause publication while collection can continue
./deploy.sh rollback   # Restore and verify the preceding published version; hold
./deploy.sh resume     # Explicitly release a publication hold
./deploy.sh status
tail -n 40 state/run.log
```

Rollback requires a verified earlier publication; it does not roll the first
publication back to the temporary bootstrap placeholder. The durable hold ensures
the next scheduled run cannot undo a rollback. Correct the content/code before
resuming. `state/receipts/` records version identities and exact file hashes.

The installer and scheduled job share a lock. A concurrent scheduled run exits
without overlapping work. Credentials and state are private; logs rotate at 1 MiB
with three backups. The job never pulls new Git commits automatically. To update,
hold publication, update the two checkouts to reviewed commits, run `install.sh`
again, verify with `check`, and explicitly resume. Keep previous Git SHAs for code
rollback. Production installations should use reviewed tags or commit SHAs.

All network limits are documented in `deployment.example.toml` and the application
configuration. Reads retry transient failures with exponential backoff, jitter and
bounded `Retry-After`; permanent authorization errors fail promptly. Wrangler's
transient network failures are retried with the same bounded policy. LLM and source
fallbacks are configured in the application, not hardcoded in deployment scripts.

## Tests

```sh
python3 -m unittest discover -s tests -v
node --test tests/test_worker.mjs
```

The deployment tests exercise publication boundaries, credential handling,
backoff, partial route failure, deployment drift, holds and backup retention.
The application repository has separate transport, collection, analysis, database
and renderer tests. Test fixtures are never production news.

## License

Apache License 2.0, matching the application. GitHub project owner: **1aeo**.
