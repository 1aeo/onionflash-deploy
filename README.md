# Onion Flash deployment

Install and publish [Onion Flash](https://github.com/1aeo/onionflash), a small Tor
news aggregator. A scheduled Python job collects sources and calls your LLM;
Cloudflare Workers Static Assets serves the generated website and RSS. Private
DigitalOcean Spaces backups are optional. Visitors never call the LLM.

With `[archive].enabled = true`, Spaces durably stores article pages, public JSON,
numbered history pages and full-history feeds. Cloudflare serves them under the
same URLs and retains only the front page and small shared assets in its
deployment bundle. See [permanent storage setup](PERMANENT_STORAGE.md).

Production: **https://1aeo.com/onionflash/** · [RSS](https://1aeo.com/onionflash/feed.xml)

Alias: **https://onionflash.1aeo.com/** redirects to the canonical site above.

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
   Keep mode **600**. `KEY=value` lines use shell-style quoting (a `#` inside a value is
   kept, so put comments on their own line; `KEY=abc # note` is rejected); shell
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
./deploy.sh status      # Show publication, hold, backup and last-run records
```

The first run may publish only some stories while its durable analysis queue is
processed. Each later run continues the queue. Processing limits and the run time
budget are configurable. `run` does not attach hostname routes; inspect the initial
publication first at the Worker's `workers.dev` origin (`https://<worker_name>.<account
subdomain>.workers.dev` plus the site's base path).

## Cloudflare setup

Use an active, proxied hostname in your Cloudflare account. The API credential
needs Workers Scripts editing and access to the account's Workers subdomain.
`routes` additionally needs the zone ID and a token with Workers Routes editing
and read access. A separate zone token is supported. Existing routes owned by a
different Worker are never replaced.

At `https://example.com/onionflash/`, the assets are mounted at `/onionflash`.
The publisher packages ordinary asset paths beneath that prefix, so it does not
require Cloudflare's account-gated `assets.base_path` feature.
The installed route is `example.com/onionflash*`, because Cloudflare route
matching includes query strings. The tiny Worker redirects `/onionflash` to
`/onionflash/` while preserving its query. It serves the application's subtree,
and **passes sibling paths such as `/onionflash-other` to their original origin**.
Its code and tests explicitly enforce that boundary. For a whole-host deployment,
use a root `site.base_url`, such as `https://news.example.com/`.

### Optional hostname alias

`redirect-worker.mjs` is a separate, small Worker that sends HTTP 308 redirects.
Production uses `onionflash-redirect` on `onionflash.1aeo.com/*`, with a proxied
CNAME from `onionflash.1aeo.com` to `1aeo.com`. Its `CANONICAL_URL` variable is
`https://1aeo.com/onionflash/`. Article/feed paths and query strings are preserved;
an already-present `/onionflash` prefix is not duplicated.

To configure another installation, copy `redirect.example.toml` to
`redirect.toml`, edit the Worker name and canonical URL, and deploy that file
with Wrangler using the existing account credential. Create the alias's proxied
DNS record and attach only `<alias-hostname>/*` to the redirect Worker. The
regular `deploy.sh routes` command manages the canonical site's route; it does
not replace this separate alias route. Keep DNS/routing receipts privately.

The separate Worker guarantees hostname redirects even when an incoming path
matches a static asset. The canonical site keeps its existing asset delivery,
and the redirect itself requires no browser JavaScript.

Publication uploads an immutable version, checks its preview hashes, activates
it, and verifies the live site: the production hostname once routed, otherwise the
`workers.dev` origin. A failed live check reactivates the preceding version and
fails that run; the next scheduled run tries again. Unchanged content is not
uploaded, but the active version is checked before taking that shortcut. A new Worker is deployed directly, since it
has no live version to protect. An active version that differs from the recorded
publication, or changes during preview checks, stops publication for inspection.

Permanent-storage deployments use the same linear flow. Small per-version
receipts preserve the immutable archive root and verification hashes for rollback
and private history retention; there is no transition journal or `reconcile`
command. An interrupted attempt clears the content shortcut. If activation did
not occur, the next run uploads and verifies again. If the live version changed
without a completed receipt, publication stops for operator inspection rather
than guessing whether that change belongs to this process. An external version
change during verification is left intact rather than rolled back.
Manual rollback also checks the recorded active version before preparation and
again before activation, so it will not replace a concurrent external deployment.

Failures inside a run are recorded in `state/run.log` and `state/last-run.json`;
errors raised before the run starts, such as an invalid configuration or credential
file, go to the console or, for scheduled runs, the system log.

The public package is an exact allowlist of generated pages, feeds and three
assets. SQLite, source evidence, credentials, logs and publication records stay
outside it. Source timestamps and original first-publication times are preserved.

## DigitalOcean Spaces

Set `spaces.enabled = true`, an HTTPS endpoint, region, bucket and private prefix
in `deployment.toml`. The corresponding environment variable names are configurable.
Use a dedicated bucket or an isolated prefix such as `onionflash/backups/`, and
create the bucket privately first.

```sh
./deploy.sh backup
./deploy.sh backup --force   # Make a fresh backup immediately
```

A backup is a gzip-compressed SQLite snapshot taken under the application's
database lock; the public site is rebuilt from it with `onionflash build`. Each
object is uploaded with a **private ACL**, then an unsigned request for it must be
refused with 403. That checks the actual exposure path, whatever ACLs or bucket
policies say; a publicly readable object is deleted and the backup fails.
Defaults are one backup per day and 30 days of retention; changing the endpoint,
bucket or prefix triggers a backup on the next run. Retention only deletes
this application's strictly named objects under its backup prefix and never the
newest upload. A backup outage is recorded and retried later without blocking
news publication unless `spaces.required = true`; optional failures appear as
degraded warnings in job status. Scheduled backups are attempted even when
collection or publication fails, or publication is held. A retention failure does
not advance the interval marker and is retried on a later run.

For recovery, hold publication, download a selected object with an S3 client,
decompress it into a new private directory and check it with `PRAGMA quick_check`.
Replace the database only with the scheduled job stopped and the application lock
held, then run the application's offline `build` before resuming publication.
Keep the old database until recovery is verified. Credentials are never included
in backups.

## Operations

```sh
./deploy.sh publish    # Publish already generated output; no LLM call
./deploy.sh hold       # Pause publication while collection can continue
./deploy.sh rollback   # Reactivate the preceding version and hold publication
./deploy.sh resume     # Explicitly release a publication hold
./deploy.sh status
tail -n 40 state/run.log
```

Rollback reactivates the version recorded as the previous publication and holds
publication, so the next scheduled run cannot undo it. Correct the content or code
before resuming; the first publication after resuming uploads again.
`state/publication.json` records the live version, its predecessor and the exact
file hashes.

The installer and scheduled job share a lock. A concurrent scheduled run exits
without overlapping work. A busy administrative command exits **75** with a clear
"command not performed" message, so retry it after the running job finishes.
`status` stays available during a run and does not open or rotate the shared log.
Credentials and state are private; `state/run.log` rotates at 1 MiB with three
backups and records each run's report and any application error. The scheduled
line sends console output to the system log (`logger -t onionflash`), so errors
raised before `run.log` opens are visible too. Wrangler keeps one log file per run
in `state/wrangler-logs/` and prunes old ones. The job never pulls new Git commits automatically. To update,
hold publication, update the two checkouts to reviewed commits, run `install.sh`
again, verify with `check`, and explicitly resume. Keep previous Git SHAs for code
rollback. Production installations should use reviewed tags or commit SHAs.

Network limits are in `deployment.example.toml` and the application configuration.
Publication checks retry transient failures and not-yet-propagated files with
exponential backoff; other client errors fail promptly. Wrangler retries its own API
calls. LLM and source fallbacks are configured in the application, not hardcoded in
deployment scripts.

## Tests

```sh
python3 -m unittest discover -s tests -v
node --test tests/test_worker.mjs
```

The deployment tests exercise publication boundaries, credential handling,
verification retries, rollback after a failed check, partial route failure,
administrative lock contention, holds, backup privacy cleanup and retention.
The application repository has separate transport, collection, analysis, database
and renderer tests.

## License

Apache License 2.0, matching the application. GitHub project owner: **1aeo**.
