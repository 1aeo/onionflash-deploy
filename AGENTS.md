# Contributor rules

These rules apply to every change, whether a person or a coding agent makes it.

## Public text

This repository is public, and GitHub keeps every revision of PR and issue text.
Commit messages, PR titles and descriptions, issues, code comments and docs must
not contain:

- host names, IP addresses, bucket names, or paths of a real deployment;
- schedules or time zones of a real deployment;
- production sizes or counts (stories, files, objects, bytes), costs, or rollout
  state ("enabled in production", "waiting for access");
- names of internal tools, model routes or review services;
- security exposures that are not fixed yet.

Describe tests, rehearsals and reviews generically. Rollout and operations steps
go to the operator, not into PR text.

## Settings

`deployment.toml` holds only operator choices: paths, the schedule, the Worker
name and compatibility date, and storage destinations. Budgets, limits, timeouts
and retries are constants in the module that uses them. A removed setting stays
listed in `publish.RETIRED` so older files still load. Credentials use the
standard environment variable names listed in the README.

## The application boundary

This repository uses the application only through its public interfaces:

- `onionflash.config.load_config`, `onionflash.cli.main` and the commands' JSON
  output;
- `onionflash.store.backup_database`, and the layout constants
  `onionflash.render.ASSET_FILES` and `onionflash.schema.CATEGORIES`;
- the rendered output, and the publication tables and metadata documented for
  publishers: `public_artifacts`, `public_catalog`, `public_render_queue` and the
  `publication_*` metadata keys.

Do not read other application tables or parse its private JSON; ask the
application to expose what is needed instead. The current exception is
`retention.py`, which compacts old `items` and `analyses` rows; it is to move
behind an application interface.

## Code

- Make the smallest change that solves the problem, and delete code that loses
  its last caller.
- Keep each fact in one document and link to it instead of restating it.
- Never weaken the permalink check or add a way to drop a published article; a
  story is corrected or retracted in the application.
- Tests pin behavior, not implementation details. After `install.sh`, run
  `.venv/bin/python -m unittest discover -s tests` and
  `node_modules/node/bin/node --test tests/*.mjs` before every push.
