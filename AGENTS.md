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
and retries are constants in the module that uses them. The `[archive]` and
`[retention]` sections still hold some such values; remove one only together with
the code that reads it. Credentials use the standard environment variable names
listed in the README.

## The application boundary

This repository uses the application only through its public interfaces:
`onionflash.config.load_config`, its commands and their JSON output, the rendered
output, and the publication tables and metadata it documents for publishers. Do
not read other application tables or parse its private JSON; ask the application
to expose what is needed instead. The one current exception is retention's
protection of unfinished review evidence, which is to move behind an application
interface.

## Code

- Make the smallest change that solves the problem, and delete code that loses
  its last caller.
- Keep each fact in one document and link to it instead of restating it.
- Tests pin behavior, not implementation details. After `install.sh`, run
  `.venv/bin/python -m unittest discover -s tests` and
  `node_modules/node/bin/node --test tests/*.mjs` before every push.
