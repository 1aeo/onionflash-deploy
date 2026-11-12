# Onion Flash deployment

Install and publish [Onion Flash](https://github.com/1aeo/onionflash), a small Tor
news aggregator. A scheduled Python job collects sources and calls your LLM;
Cloudflare Workers Static Assets serves the generated website and RSS. Private
DigitalOcean Spaces backups are optional. Visitors never call the LLM.
The website uses ordinary links, static topic pages and a native menu disclosure;
it ships no browser JavaScript. Cloudflare's routing Worker runs only on the server.

With `[archive].enabled = true`, Spaces durably stores article pages and their
public JSON, and Cloudflare serves them under the same URLs; every other file stays
in the Worker's deployment bundle. See [permanent storage setup](PERMANENT_STORAGE.md).

Production: **https://1aeo.com/onionflash/** · [RSS](https://1aeo.com/onionflash/feed.xml)

Alias: **https://onionflash.1aeo.com/** redirects to the canonical site above.

## Quick start

Requirements: Linux or macOS, Python **3.11+** with `venv`/pip, Git, and npm.
The installer runs `npm ci`, which installs the locked Wrangler release and Node.js
24.21.0 into the repository. The `node` package downloads that binary in its own
install step, so the lockfile pins its version but does not verify the download.
Cron is needed only for the optional schedule. Run everything as an ordinary user.

```sh
git clone https://github.com/1aeo/onionflash.git
git clone https://github.com/1aeo/onionflash-deploy.git
cd onionflash-deploy
./install.sh ../onionflash
```

The installer builds the application from its checked-out commit only, so
untracked files and build caches in the checkout are never installed. It refuses
a checkout with uncommitted changes to tracked files.

Edit the three generated files (all ignored by Git):

1. **`onionflash.toml`**: website URL, sources, LLM endpoint/model and timeout,
   per-run limits, fallback URLs, and SQLite/output paths. For a LAN LLM,
   explicitly allow its exact addresses in `llm.allowed_networks` and set
   `allow_http = true` only if that endpoint uses HTTP. Source fetching remains
   restricted to public HTTPS.
2. **`secrets.env`**: LLM key, `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN`
   (plus `CLOUDFLARE_ZONE_TOKEN`/`CLOUDFLARE_ZONE_ID` for `routes`), and optionally
   `DO_SPACES_KEY`/`DO_SPACES_SECRET`. Keep mode **600**. `KEY=value` lines use shell-style quoting (a `#` inside a value is
   kept, so put comments on their own line; `KEY=abc # note` is rejected); shell
   commands and variable expansion are never executed. Set
   `ONIONFLASH_LLM_BASE_URL` if using the example application's environment reference.
3. **`deployment.toml`**: Worker name, compatibility date, schedule and optional
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
processed. Each later run continues the queue; `llm.max_items_per_run` sets how
many items one run analyzes, and the run time budget is fixed (see the
application's [configuration](https://github.com/1aeo/onionflash#configuration)).
`run` does not attach hostname routes; inspect the initial
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

## Publication

Publication uploads an immutable version, checks its preview hashes, activates
it, and verifies the live site: the production hostname once routed, otherwise the
`workers.dev` origin. A failed live check reactivates the preceding version and
fails that run; if that restore succeeds, the next scheduled run tries again. A new
Worker is deployed directly, since it has no live version to protect; it is recorded
even if that deployment or its live check fails, so the next run uploads and checks
again. Unchanged content is not uploaded, but the active version is checked before
taking that shortcut, and an interrupted attempt clears it. An active version that
differs from the recorded publication, or changes during preview or live checks,
stops publication for inspection (see [Operations](#operations)).

Failures inside a run are recorded in `state/run.log` and `state/last-run.json`;
errors raised before the run starts, such as an invalid configuration or credential
file, go to the console or, for scheduled runs, the system log.

The public package is an exact allowlist of generated pages, feeds and two
assets (`style.css` and `mark.svg`). SQLite, source evidence, credentials, logs and publication records stay
outside it. Source timestamps and original first-publication times are preserved.

Topic pages use `/topics/<topic>/` and `/topics/<topic>/page/<number>/`; the
existing `/tags/<topic>.xml` subscriptions remain available. HTML responses
disable browser scripts and connections with `script-src 'none'` and
`connect-src 'none'`.

An upgrade from a release that shipped browser JavaScript rebuilds the articles in
bounded batches on scheduled runs. Until that rebuild retires the old `app.js`,
publication refuses it and the live site stays unchanged; see the application's
[upgrade notes](https://github.com/1aeo/onionflash/blob/main/docs/PERMANENT_HISTORY.md#upgrade-from-the-browser-script-version).

## DigitalOcean Spaces

Set `spaces.enabled = true`, an HTTPS endpoint, region, bucket and private prefix
in `deployment.toml`, and the two Spaces keys in `secrets.env`.
Use a dedicated bucket or an isolated prefix such as `onionflash/backups/`, and
create the bucket privately first.

```sh
./deploy.sh backup
./deploy.sh backup --force   # Make a fresh backup immediately
```

A backup is a gzip-compressed SQLite snapshot taken under the application's
database lock; recovery (below) rebuilds the public site from it. Each
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
held, then run `./deploy.sh app build --force`, repeating it while it reports
`articles_pending` (no LLM calls are needed), before resuming publication.
Publication refuses a database that lacks an article this deployment has
published (see [Operations](#operations)); restore a newer copy instead. Keep the
old database until recovery is verified. Credentials are never included in backups.

## Operations

Use `./deploy.sh app` for offline application operations. It loads the protected
dotenv file and the application config named in `deployment.toml`, so environment
references such as `${ONIONFLASH_LLM_BASE_URL}` work without sourcing a shell file.
Only `story`, `rejected`, `decisions`, `build`, and `status` are available; do not
supply an application `--config`. Cloudflare, Spaces, and LLM keys are removed from the
application environment after configuration is resolved. Inspection remains
available during scheduled work; mutations take the deployment lock and the
application's database lock, returning **75** if either is busy.

```sh
./deploy.sh app status
./deploy.sh app rejected list --limit 20
./deploy.sh app decisions summary --since 7d
./deploy.sh app story show --story STORY_ID
./deploy.sh app build      # Render saved stories after an editorial change
./deploy.sh publish        # Publish the resulting files, or let the next cron run do it
```

`rejected approve` and `story merge` take the run lock;
`decisions summary|list` are read-only.
The `story` and `rejected` action flags match the application's
[editorial](https://github.com/1aeo/onionflash/blob/main/docs/EDITORIAL.md) and
[rejected-analysis](https://github.com/1aeo/onionflash/blob/main/docs/REJECTED_ANALYSES.md)
guides. These application commands make no source, model, or publication requests.
Corrections and retractions require a subsequent build and publication to appear
on the website; retrying a rejection only queues it for a later scheduled analysis.

After each run, when `state/weekly/<YYYY>-W<WW>.md` for the current ISO week does
not exist yet, the publisher writes the JSON of the application's
`decisions summary --since 7d`, indented, there and to `state/weekly/latest.md`,
both with mode 600. The [summary](https://github.com/1aeo/onionflash/blob/main/docs/DECISIONS.md#commands)
covers the last seven days and includes the size of `feed.xml`. Nothing is sent;
read the files on the server. A failure is a degraded warning, never a failed run,
and the next run tries again.

```sh
./deploy.sh publish    # Publish already generated output; no LLM call
./deploy.sh hold       # Pause publication while collection can continue
./deploy.sh rollback   # Reactivate the preceding version and hold publication
./deploy.sh resume     # Explicitly release a publication hold
./deploy.sh status
tail -n 40 state/run.log
```

Rollback reactivates the version recorded as the previous publication and holds
publication, so the next scheduled run cannot undo it. It refuses to run when the
active version differs from the recorded publication. Correct the content or code
before resuming; the first publication after resuming uploads again.
`state/publication.json` records the live version, its predecessor and the exact
file hashes. `state/receipts/` keeps that record for the live and previous
versions, which rollback restores; with permanent storage it keeps one for every
version, including its root. `state/permalinks.json` lists every article this
deployment has published and only grows: rollback leaves it alone, and a
publication that lacks one of its articles is refused whatever the receipts say.
To withdraw a story, retract it.

A run that fails with "Active Worker version differs from the publication receipt"
names the live and recorded versions. A failed restore, a run interrupted after
activation, a lost `state/` directory or a deployment made outside this publisher
can cause it, and neither `rollback` nor `resume` clears it. Hold publication and
inspect the live version. To discard it, deploy the recorded version again from the
Worker's deployments in the Cloudflare dashboard (or with `wrangler versions deploy`).
To keep it, replace `state/publication.json`: if `state/receipts/` has a record
for that version, use the record with its `content_hash` field deleted; otherwise
write `{"version_id": "<live version>"}`. Either way the next run uploads and
checks a new version. A version-only receipt lists no articles, and
`state/permalinks.json` lists only articles whose publication passed its live
check, so use such a receipt only with a database that has every published story,
such as the one the live version was built from, and restore
`state/permalinks.json` if it was lost too. If `state/routes.json` was lost too,
restore it. Otherwise the next publication is
checked at the workers.dev origin, and `./deploy.sh routes` restores the check
against the public hostname once that publication has written a full receipt.
Then resume.

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

The publisher's and the Worker's timeouts, retries and size limits are constants
in the code.
Publication checks retry transient failures and not-yet-propagated files with
exponential backoff for about two minutes per file; other client errors fail
promptly. Wrangler retries its own API calls. LLM and source fallbacks are
configured in the application, not hardcoded in deployment scripts.

## Tests

To run the Python and Node suites that CI runs, use the commands in
[AGENTS.md](AGENTS.md#code).

The deployment tests exercise publication boundaries, credential handling,
verification retries, rollback after a failed check, partial route failure,
administrative lock contention, holds, backup privacy cleanup and retention.
The application repository has separate transport, collection, analysis, database
and renderer tests.

## License

Apache License 2.0, matching the application. GitHub project owner: **1aeo**.
