# Databricks credentials for GitHub Actions

Two workflows talk to the workspace:

| Workflow | When | Uses |
|---|---|---|
| `refresh.yml` | nightly + manual | the service principal if `DATABRICKS_CLIENT_ID` is set, else the PAT |
| `deploy.yml` | push to `main` (prod), PRs (dev), manual | the PAT, unless `FD_DEPLOY_AUTH=service-principal` |

Both read the workspace URL from the variable `DATABRICKS_HOST`, or the secret of the same name.
[ADR 0007](adr/0007-unattended-auth.md) explains the choices.

## Which option Free Edition supports

| Option | Stored secret | Expires | Free Edition |
|---|---|---|---|
| A. GitHub OIDC → Databricks token federation | none | never | **Not available.** Federation policies are created with the account-level API (`databricks account service-principal-federation-policy create`), and Free Edition has "no access to the account console or account-level APIs". |
| B. Service principal + OAuth secret (M2M) | client secret | up to **730 days** | **Available.** Service principals and their OAuth secrets are managed in workspace settings by a workspace admin, which you are. |
| C. Personal access token | PAT | up to **730 days** (this workspace's `maxTokenLifetimeDays`) | Available. It's what runs today: `NewToken`, which **expires 2026-10-21**. |

**Recommended:** B for the nightly refresh, and C with the full 730-day lifetime for deploys.
`refresh.yml` already supports A. If the workspace is ever upgraded to an edition with an account
console, adding the federation policy and deleting the client secret is enough.

The `credential health` job checks the configured credential before every refresh. It fails, and
opens a "Databricks credential needs attention" issue, when the credential is rejected or expires
within 14 days. The window is set by the variable `FD_CREDENTIAL_WARN_DAYS`.

---

## B. Service principal with an OAuth secret (about 10 minutes)

### 1. Create the service principal
1. In the workspace, click your username (top right) and choose **Settings**.
2. Open the **Identity and access** tab. Next to **Service principals**, click **Manage**.
3. Click **Add service principal**, then **Add new**. Name it `gh-actions-food-delivery` and click **Add**.
4. Open it. On the **Configuration** tab, keep **Workspace access** and **Databricks SQL access**
   ticked. Leave **Allow cluster creation** and every admin role off.
5. Copy the **Application ID** (a UUID). This is the client ID.

### 2. Generate the OAuth secret
1. Still on the service principal, open the **Secrets** tab and click **Generate secret**.
2. Set **Lifetime (days)** to `730`.
3. **Scopes:** choose **All APIs**. The secret's real power is limited by the narrow grants in
   step 3, and the refresh calls Jobs, SQL, Files, Unity Catalog and identity APIs.
4. Click **Generate**, then copy the **Secret**. It is shown once. Note the expiry date
   (today + 730 days).

### 3. Grant only what the refresh needs
The job runs as its owner, so the service principal needs to start runs, write landing files and read
gold. It needs nothing else.

| Where (in the workspace) | Object | Grant to `gh-actions-food-delivery` |
|---|---|---|
| Catalog Explorer → `workspace` → **Permissions** → Grant | catalog `workspace` | `USE CATALOG` |
| Catalog Explorer → `dev_rohit91jacob_fooddelivery` → **Permissions** | schema | `USE SCHEMA` |
| Catalog Explorer → `dev_rohit91jacob_fooddelivery` → `landing` (volume) → **Permissions** | volume | `READ VOLUME`, `WRITE VOLUME` |
| Catalog Explorer → `dev_rohit91jacob_fooddelivery_gold` → **Permissions** | schema | `USE SCHEMA`, `SELECT` |
| Jobs & Pipelines → `[dev rohit91jacob] fooddelivery_daily` → ⋮ → **Edit permissions** | job | `Can Manage Run` |
| SQL Warehouses → `Serverless Starter Warehouse` → **Permissions** | warehouse | `Can use` |

### 4. Configure the repository
In GitHub → **Settings → Secrets and variables → Actions**:

| Kind | Name | Value |
|---|---|---|
| Secret | `DATABRICKS_CLIENT_SECRET` | the secret from step 2 |
| Variable | `DATABRICKS_CLIENT_ID` | the Application ID from step 1 |
| Variable | `DATABRICKS_CLIENT_SECRET_EXPIRES` | the secret's expiry date, `YYYY-MM-DD` |
| Variable | `DATABRICKS_HOST` | `https://dbc-53816a96-ab86.cloud.databricks.com` (optional; a secret of the same name also works) |

As soon as `DATABRICKS_CLIENT_ID` exists, the refresh switches to the service principal. Each run
logs `"auth_mode": "oauth-m2m"`.

### 5. Prove it
In **Actions → Scheduled refresh → Run workflow**, leave the inputs empty and run. The
`credential health` job should report `oauth-m2m` and `gh-actions-food-delivery`, and the refresh
should end with `reconciled 5 cities x 26 metrics`. If a step fails with `PERMISSION_DENIED`, the
error names the missing grant from step 3.

### 6. Renewal (every two years)
Generate a second secret on the same service principal (up to five can coexist), update
`DATABRICKS_CLIENT_SECRET` and `DATABRICKS_CLIENT_SECRET_EXPIRES`, run the refresh once, then
delete the old secret. The 14-day warning opens an issue in good time.

---

## C. A personal access token with the maximum lifetime (for deploys, or as a fallback)

1. Click your username → **Settings** → **Developer** → **Access tokens** → **Manage**, then
   **Generate new token**.
2. Comment: `github-actions-food-delivery`. Lifetime: `730` days, the maximum this workspace
   allows. Click **Generate** and copy the token.
3. In GitHub, update the secret `DATABRICKS_TOKEN`.
4. Optional but recommended: set the variable `DATABRICKS_TOKEN_ID` to the new token's ID, so the
   expiry check judges exactly that token instead of the soonest-expiring of all your tokens. The
   ID is shown by `databricks tokens list`.
5. Run **Deploy bundle** (or the refresh) once, then revoke the old token (`NewToken`) on the same
   page.

## Deploying as the service principal (optional, advanced)

Setting the variable `FD_DEPLOY_AUTH=service-principal` makes `deploy.yml` use B as well. Do this
deliberately. In development mode, resource names (`[dev <user>] …`, `dev_<user>_…`) and the
bundle's state location depend on the deploying identity, and the prod root path is per user. A
switch therefore creates a second copy rather than updating the first. The service principal also
needs ownership (or `CAN_MANAGE`) of the deployed resources and `CREATE` rights on the catalog. Until
that migration is wanted, keep deploys on the long-lived PAT from C.
