# Release readiness audit — 2026-10-10

## Decision

A stable release claiming bounded metadata storage is not ready. Functional deletion and command-bot acceptance have passed for the tested single-process Synapse 1.162.0 deployment. Do not repeat the completed Element X acceptance tests just to fill older checklist entries.

## Verified evidence

- UI commit: e631cb6424c10049faf24e975c8287414ed15230.
- GitHub Actions run 38039493281: 132 tests passed, no skips; Ruff, retention worker image, Synapse image, actual-image room limiter check, command-bot image and Compose smoke test passed.
- Metadata compaction commit: 59dd70b623de6f6ea61988e3bd12ccd55a7f2a09; Actions run 38037621437 passed. This separate branch is not included in the UI commit and deployment has not been confirmed.
- Existing acceptance records cover plain/E2EE DM, foreground/background/offline cache reset, permissions, moderator fallback, federation and service restarts.
- Live command-bot: encrypted DM, selection by room name, rejection of equal minimum/maximum, successful policy change, readable formatted status with room link. Latest status: maximum five minutes, minimum thirty seconds. Duplicate commands in the screenshot were deliberately sent twice.
- Bot selection persistence across restart is covered by integration tests. The live screenshot selected the room again after deployment and does not independently prove this scenario.
- Live 10 msg/s × 300 s: 3000 additional redactions done, zero additional missed, all invalidations complete.
- Live 25 msg/s × 300 s: 7500 accepted, zero HTTP errors, reported 23.71 msg/s; 4788 additional redactions done, 2712 additional EVENT_PURGED misses, all 7500 additional invalidations complete.
- Live 50 msg/s × 300 s: 15000 accepted, zero HTTP errors, reported 23.7 msg/s; 4458 additional redactions done, 10542 additional EVENT_PURGED misses, all 15000 additional invalidations complete. Actual sending took approximately 633 s and scheduling lag p95 was 307668 ms.
- Later SQL confirms no original message events, matching reset generations, no pending reset, and completed reset records for all ten local members after both sustained 25 and 50 stages. This is server-side confirmation; it is not a fresh observation on ten physical clients.
- Fixed load room: 54753 history rows and 54753 snapshots, tips_max=1, rows_per_snapshot=1.00. Old two rooms account for approximately 12.5 million historical rows. Current tips do not erase older history.

## Checklist and scope

| Status | Item | Remaining action |
| --- | --- | --- |
| Passed | Automated functionality, permissions, queues, E2EE, recovery, bot UI | Re-run CI once on the final combined release commit. |
| Passed | Existing Element X foreground/background/offline acceptance | No repeat required for current UI-only change. |
| Passed | Eventual original-message purge and completion of invalidation/reset jobs under tested load | Do not claim redaction of every event before native purge or achieved 50/100 msg/s throughput. |
| Passed | DAG amplification correction in tested load room | Document native thirty-day history cleanup and existing historical data separately. |
| Passed | Live command-bot name selection, policy changes, validation and formatted status | No additional manual UI acceptance required. |
| Covered in CI | Bot room links/aliases, duplicate names, permissions loss, group privacy and encrypted restart | Optional live spot checks; not blockers for the tested configuration. |
| Not confirmed by this audit | Complete live content-type matrix | Older checklist remains unchecked; do not manufacture a pass. This does not establish physical media deletion. |
| Outside tested scope | Synapse external workers/multiple processes and iOS client | Test before claiming those deployments/clients as accepted. |
| Separate scope | Media repository lifecycle and thumbnails | No guarantee that redaction or event purge deletes media bytes; define and test a separate media policy if promised. |
| Not required for stated goal | Exact 100 msg/s and strict retention deadline | User accepts delayed eventual removal. More tests at that target are not needed for this release decision. |
| Blocker | Terminal bot metadata compaction including missed | Integrate the separate fix and validate final combined CI; verify installed code when deployed. |
| Blocker | Synapse event-invalidation idempotency metadata lifecycle | Implement safe bounded cleanup covering redelivery/recovery; test aging and duplicate replay. No routine truncation. |
| Pending | Unified release commit and install/upgrade/operations documentation | Prepare after storage lifecycle is resolved; include cleanup windows, resource limits and unsupported scope. |

## Necessary remaining tests

Test cleanup by controlled timestamps on disposable databases rather than waiting seven or thirty real days. Preserve pending work and idempotency while expiring eligible terminal metadata; verify late duplicate delivery and recovery. Existing compaction tests already cover the bot-side cutoff; new tests must target the Synapse-side lifecycle.

After combining changes, run the full CI suite once. A repeated long flood or another round of phone deletion tests is not justified unless deletion/reset code changes or the final CI finds a regression.
