# ADR 0007: Unattended Databricks auth for scheduled and CI runs

**Status:** accepted

## Context
The nightly refresh (`refresh.yml`) and the bundle deploys (`deploy.yml`) run on GitHub-hosted
runners without a person present. The first deployment used a personal access token with a 14-day
lifetime, so it would silently stop working within two weeks. Free Edition constraints narrow the
options:
- No account console and no account-level APIs. GitHub OIDC token federation needs a service
  principal federation policy, which is an account-level object, so it isn't available.
- Workspace admins can create service principals and OAuth secrets (lifetime up to 730 days).
- PATs are capped by `maxTokenLifetimeDays`, which is 730 in this workspace.

## Decision
1. **Pluggable auth by precedence.** `fooddelivery.refresh.auth_mode` uses GitHub OIDC federation
   (a client ID with no secret), then OAuth M2M (a client ID and secret), then a PAT. The workflows
   export exactly one credential (databricks-sdk rejects ambiguous configuration) and set
   `DATABRICKS_AUTH_TYPE` explicitly. When federation becomes available, adding the policy and
   deleting the secret is enough; no code changes.
2. **The refresh runs as a dedicated, least-privilege service principal.** It can run the job, write
   to the landing volume, read gold and use the warehouse. The job still runs as its owner, so the
   principal needs no pipeline or table-write rights.
3. **Deploys stay on a long-lived PAT unless opted in** (`FD_DEPLOY_AUTH=service-principal`). Changing
   the deploying identity changes dev resource names and the bundle state location, so it is a
   deliberate migration, not a silent side effect of adding a variable.
4. **Fail early, loudly and with a date.** A `credential health` job runs before any data is touched.
   It authenticates (`current_user.me()`) and judges the remaining lifetime:
   - a PAT through `/api/2.0/token/list`, pinned with `DATABRICKS_TOKEN_ID` (if unpinned, the
     soonest-expiring valid token is judged, which can raise a false alarm but never misses one);
   - an OAuth secret through the recorded `DATABRICKS_CLIENT_SECRET_EXPIRES`, because its expiry
     can't be read without account APIs.

   Within 14 days of expiry the run fails and opens (or comments on) one GitHub issue, which also
   triggers GitHub's failure email.

## Consequences
- With B in place, the only recurring chore is renewing an OAuth secret every two years. The deploy
  PAT, renewed every two years, is the second.
- A credential nearing expiry stops the refresh for up to 14 days before the hard expiry. That is
  intentional: a gap in the synthetic feed is cheap, and a forgotten credential is not.
- The issue-based alerting needs only the built-in `GITHUB_TOKEN` (`issues: write`).
