"""An MLflow ``ResponsesAgent`` that answers questions through a Genie space.

Self-contained on purpose (mlflow + databricks-sdk only, no ``fooddelivery`` imports), so the same file
runs locally, inside ``mlflow.genai.evaluate`` and as a models-from-code model on Model Serving.

Tracing: ``ResponsesAgent`` traces ``predict`` as the root AGENT span automatically. Two child spans
cover the work MLflow cannot see: ``genie.ask`` (the Conversation API round trip: prompt, status,
generated SQL, Genie's text) and ``genie.query_result`` (the rows the SQL returned). The trace's
session is the Genie conversation, so follow-up questions group together in the MLflow UI.

Configuration (environment, or ``model_config`` when logged): ``FD_GENIE_SPACE_ID`` (required),
``FD_GENIE_MAX_ROWS`` (rows rendered into the answer, default 20).
"""

from __future__ import annotations

import os
from collections.abc import Generator
from datetime import timedelta
from typing import Any

import mlflow
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

DEFAULT_MAX_ROWS = 20
GENIE_TIMEOUT = timedelta(minutes=5)


def _logged_config() -> dict:
    """The ``model_config`` the model was logged with.

    MLflow exposes it to ``ModelConfig`` only while it imports this file, so it must be read at
    import time; read lazily inside ``predict`` it is already gone and the agent would fall back to
    an unset environment variable.
    """
    try:
        return dict(mlflow.models.ModelConfig(development_config={}).to_dict() or {})
    except Exception:
        return {}


_LOGGED_CONFIG = _logged_config()


def _config(key: str, default: str | None = None) -> str | None:
    return os.environ.get(key) or _LOGGED_CONFIG.get(key) or default


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):  # [{"type": "input_text", "text": ...}, ...]
        return "".join(c.get("text", "") if isinstance(c, dict) else str(c) for c in content)
    return str(content or "")


def last_user_message(request: ResponsesAgentRequest) -> str:
    for item in reversed(request.input):
        item = item.model_dump() if hasattr(item, "model_dump") else dict(item)
        if item.get("role") == "user":
            return _text(item.get("content")).strip()
    raise ValueError("the request has no user message")


def markdown_table(columns: list[str], rows: list[list[Any]], max_rows: int) -> str:
    if not columns:
        return ""
    shown = rows[:max_rows]
    out = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in shown]
    if len(rows) > len(shown):
        out.append(f"\n_{len(rows) - len(shown)} more rows not shown._")
    return "\n".join(out)


class GenieAgent(ResponsesAgent):
    def __init__(self, workspace_client=None, space_id: str | None = None, max_rows: int | None = None):
        self._w = workspace_client
        self._space_id = space_id
        self._max_rows = max_rows

    # Resolved lazily: Model Serving loads the model before credentials/env are guaranteed usable.
    @property
    def w(self):
        if self._w is None:
            from databricks.sdk import WorkspaceClient

            self._w = WorkspaceClient()
        return self._w

    @property
    def space_id(self) -> str:
        sid = self._space_id or _config("FD_GENIE_SPACE_ID")
        if not sid:
            raise RuntimeError("FD_GENIE_SPACE_ID is not set: run `fd agent space` and export the id it prints")
        return sid

    @property
    def max_rows(self) -> int:
        return self._max_rows or int(_config("FD_GENIE_MAX_ROWS", str(DEFAULT_MAX_ROWS)))

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        question = last_user_message(request)
        custom = dict(request.custom_inputs or {})
        conversation_id = custom.get("conversation_id")

        answer = self._ask(question, conversation_id)
        mlflow.update_current_trace(
            tags={"genie.space_id": self.space_id, "genie.status": answer["status"]},
            session_id=answer["conversation_id"] or None,
            user=str(custom["user_id"]) if custom.get("user_id") else None,
        )

        item = self.create_text_output_item(text=answer["text"], id=answer["message_id"] or "genie")
        return ResponsesAgentResponse(
            output=[item],
            custom_outputs={
                "conversation_id": answer["conversation_id"],
                "message_id": answer["message_id"],
                "status": answer["status"],
                "sql": answer["sql"],
                "row_count": answer["row_count"],
            },
        )

    def predict_stream(self, request: ResponsesAgentRequest) -> Generator[ResponsesAgentStreamEvent, None, None]:
        # Genie returns a complete answer, so the stream is a single "done" event.
        response = self.predict(request)
        for item in response.output:
            yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)

    @mlflow.trace(name="genie.ask", span_type=SpanType.TOOL)
    def _ask(self, question: str, conversation_id: str | None) -> dict:
        if conversation_id:
            msg = self._with_backoff(
                lambda: self.w.genie.create_message_and_wait(
                    self.space_id, conversation_id, question, timeout=GENIE_TIMEOUT
                )
            )
        else:
            msg = self._with_backoff(
                lambda: self.w.genie.start_conversation_and_wait(self.space_id, question, timeout=GENIE_TIMEOUT)
            )

        status = getattr(msg.status, "value", str(msg.status))
        message_id = msg.message_id or msg.id
        out = {
            "conversation_id": msg.conversation_id,
            "message_id": message_id,
            "status": status,
            "genie_text": None,
            "sql": None,
            "query_description": None,
            "row_count": None,
            "suggested_questions": [],
            "error": getattr(msg.error, "error", None) if msg.error else None,
        }
        columns, rows = [], []
        for att in msg.attachments or []:
            if att.text and att.text.content:
                out["genie_text"] = att.text.content
            if att.suggested_questions and att.suggested_questions.questions:
                out["suggested_questions"] = list(att.suggested_questions.questions)
            if att.query and att.query.query:
                out["sql"] = att.query.query
                out["query_description"] = att.query.description
                columns, rows = self._query_result(msg.conversation_id, message_id, att.attachment_id)
                out["row_count"] = len(rows)

        out["text"] = self._compose(out, columns, rows)
        return out

    @staticmethod
    def _with_backoff(call, attempts: int = 6):
        """Genie's Conversation API is throughput-limited per workspace; back off on 429."""
        import time

        from databricks.sdk.errors import ResourceExhausted, TooManyRequests

        for i in range(attempts):
            try:
                return call()
            except (TooManyRequests, ResourceExhausted):
                if i == attempts - 1:
                    raise
                time.sleep(min(60, 5 * 2**i))
        raise AssertionError("unreachable")

    @mlflow.trace(name="genie.query_result", span_type=SpanType.TOOL)
    def _query_result(self, conversation_id: str, message_id: str, attachment_id: str) -> tuple[list, list]:
        res = self.w.genie.get_message_attachment_query_result(
            self.space_id, conversation_id, message_id, attachment_id
        )
        stmt = res.statement_response
        if stmt is None:
            return [], []
        columns = [c.name for c in (stmt.manifest.schema.columns or [])] if stmt.manifest else []
        rows = (stmt.result.data_array or []) if stmt.result else []
        return columns, rows

    def _compose(self, out: dict, columns: list[str], rows: list[list[Any]]) -> str:
        if out["status"] != "COMPLETED":
            detail = f": {out['error']}" if out["error"] else "."
            return f"Genie could not answer this question (status {out['status']}){detail}"
        parts = []
        if out["genie_text"]:
            parts.append(out["genie_text"])
        # query_description stays in the genie.ask span only: Genie sometimes copies it from the closest
        # example SQL, so it can name a different city or date than the question.
        if columns:
            parts.append(markdown_table(columns, rows, self.max_rows))
        if out["sql"]:
            parts.append(f"```sql\n{out['sql']}\n```")
        return "\n\n".join(parts) or "Genie returned no answer."


AGENT = GenieAgent()
mlflow.models.set_model(AGENT)
