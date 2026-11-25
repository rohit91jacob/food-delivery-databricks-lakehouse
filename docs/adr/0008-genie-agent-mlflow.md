# ADR 0008: A Genie agent over gold, evaluated against generator ground truth and monitored with MLflow

**Status:** accepted

## Context
The gold marts answer business questions only for people who write SQL. Genie can turn a
question into SQL over Unity Catalog tables, but by itself it can't show how often it is
right, why an answer was wrong, or whether quality drifts as data and questions change.
The platform already has something most text-to-SQL demos don't: a generator manifest that
holds the exact answer to every daily KPI question. `gold_daily_kpis` reconciles with it to the
paisa every day.

## Decision
1. **The Genie space is code.** `fooddelivery.agent.space` declares the tables, the one text
   instruction, the example SQL and the sample questions. `fd agent space` creates the space, or
   updates the one with the same title. IDs come from uuid5 of a stable key, so the Genie API's
   sorted-hex-ID rules hold and re-runs are idempotent. Space content is reviewed in pull
   requests like any other code.
2. **The agent is a thin MLflow `ResponsesAgent` around the Conversation API.** It doesn't wrap
   Genie in a second LLM: Genie's text, its result table and its SQL are the answer. The
   `predict` method is traced automatically as the AGENT span. Two TOOL spans cover the parts
   MLflow can't see: `genie.ask` (prompt, status, SQL, Genie's text) and `genie.query_result`
   (rows). The trace session is the Genie conversation ID, so follow-up questions group together.
   The file imports only mlflow and databricks-sdk, so it is logged unchanged as models-from-code.
3. **The evaluation's answer key comes from the generator, not from gold.** `fooddelivery.agent.evalset`
   regenerates the manifests for a date range and asks point lookups, an aggregate across
   cities, a ranking, a 7-day window and an out-of-scope question. The deterministic
   `numeric_match` scorer requires the exact value: counts exactly, money to the paisa, and rates
   also accepted as percentages. LLM judges (`Correctness`, `RelevanceToQuery`, `Safety`,
   `Guidelines`) cover what code can't check.
4. **Production monitoring reuses the cheap checks at 100% and samples the judges.** Monitoring
   has no ground truth. It checks that Genie completed with a result table, that it generated SQL,
   and how long the answer took, all on every trace. Safety also runs on every trace. Relevance
   and the answer guidelines run on 50% of traces. Custom scorers keep their imports inside the
   function because registered scorers are serialised by source.
5. **Traces go to the experiment by default.** `FD_AGENT_TRACE_SCHEMA=catalog.schema` moves them
   to Unity Catalog tables. That is opt-in because binding an experiment to a UC location is
   permanent, and Free Edition's limit of 100 tables per schema is already managed carefully
   ([ADR 0001](0001-free-edition-constraints.md)).

## Consequences
- A wrong answer can be diagnosed from a single trace: the SQL Genie generated, the rows it
  returned, and which instruction or example it leaned on.
- Space changes (instructions, examples) can be A/B tested by running `fd agent eval` before and
  after, against the same deterministic answer key.
- Genie's Conversation API is throughput-limited per workspace. The agent backs off on 429s and
  evaluation runs at most 2 workers, so a 26-question evaluation takes minutes, not seconds.
- Genie sometimes copies an example SQL's description verbatim, which can name the wrong city.
  The description is kept in the span but left out of the answer text.
