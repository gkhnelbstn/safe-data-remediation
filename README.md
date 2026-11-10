# safe-data-remediation

Python CLI for safe bulk fixes in production databases: backups, generated rollback SQL, count gates, audited transactions.

**Status: planned.** This repo is a work in progress; the checklist lives in the epic issue and on the [Portfolio build-out](https://github.com/users/gkhnelbstn/projects/6) board.

## Plan

- What: a small Python CLI for bulk fixes in a production database: row-level backup export, generated rollback SQL, count gates, single audited transaction, automatic rollback on mismatch. The public version of your 652,513-record fix.
- Must have: dry-run mode, tests against a Postgres container, an example fix with its generated rollback script.
- Done when: the README walks through one fix and its rollback end to end.
