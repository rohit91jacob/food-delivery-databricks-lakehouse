"""Genie agent scorers and the traced agent against a fake Genie API (dependency group `agent`)."""

from __future__ import annotations

from types import SimpleNamespace as NS

import pytest

mlflow = pytest.importorskip("mlflow", reason="dependency group `agent`")


def _response(text, status="COMPLETED", rows=3):
    return {
        "output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}],
        "custom_outputs": {"status": status, "row_count": rows},
    }


@pytest.mark.agent
@pytest.mark.parametrize(
    ("text", "expectations", "ok"),
    [
        ("| gmv |\n|---|\n| 227202.04 |", {"expected_value": 227202.04}, True),
        ("GMV was ₹2,27,202.04 in Bangalore", {"expected_value": 227202.04}, True),
        ("GMV was 227202.10", {"expected_value": 227202.04}, False),
        ("868 orders", {"expected_value": 868}, True),
        ("on-time rate 91.3%", {"expected_value": 0.9134}, True),
        ("Mumbai had the highest GMV", {"expected_text": "Mumbai"}, True),
        ("Delhi had the highest GMV", {"expected_text": "Mumbai"}, False),
    ],
)
def test_numeric_match(text, expectations, ok):
    from fooddelivery.agent.scorers import numeric_match

    assert numeric_match(outputs=_response(text), expectations=expectations).value is ok


@pytest.mark.agent
def test_numeric_match_not_applicable_without_ground_truth():
    from fooddelivery.agent.scorers import numeric_match

    assert numeric_match(outputs=_response("n/a"), expectations={"expected_facts": ["x"]}) is None


@pytest.mark.agent
def test_genie_answered():
    from fooddelivery.agent.scorers import genie_answered

    assert genie_answered(outputs=_response("x")).value is True
    assert genie_answered(outputs=_response("x", status="FAILED", rows=None)).value is False
    refusal = _response("I can't answer that", rows=None)
    assert genie_answered(outputs=refusal, expectations={"expect_refusal": True}).value is True
    assert genie_answered(outputs=_response("x"), expectations={"expect_refusal": True}).value is False


class FakeGenie:
    def __init__(self, status="COMPLETED"):
        self.status = status
        self.calls = []

    def _msg(self, conversation_id):
        att = [
            NS(
                attachment_id="att1",
                text=NS(content="Bangalore placed 868 orders."),
                query=None,
                suggested_questions=None,
            ),
            NS(
                attachment_id="att2",
                text=None,
                suggested_questions=None,
                query=NS(query="SELECT orders_placed FROM k WHERE city = 'Bangalore'", description="Orders placed"),
            ),
        ]
        return NS(
            conversation_id=conversation_id,
            message_id="m1",
            id="m1",
            status=NS(value=self.status),
            attachments=att if self.status == "COMPLETED" else [],
            error=None if self.status == "COMPLETED" else NS(error="boom"),
        )

    def start_conversation_and_wait(self, space_id, content, timeout=None):
        self.calls.append(("start", space_id, content))
        return self._msg("c1")

    def create_message_and_wait(self, space_id, conversation_id, content, timeout=None):
        self.calls.append(("follow_up", space_id, conversation_id, content))
        return self._msg(conversation_id)

    def get_message_attachment_query_result(self, space_id, conversation_id, message_id, attachment_id):
        assert attachment_id == "att2"
        cols = NS(columns=[NS(name="orders_placed")])
        return NS(statement_response=NS(manifest=NS(schema=cols), result=NS(data_array=[["868"]])))


@pytest.fixture
def local_mlflow(tmp_path, monkeypatch):
    monkeypatch.delenv("FD_GENIE_SPACE_ID", raising=False)
    mlflow.set_tracking_uri(f"sqlite:///{tmp_path / 'mlflow.db'}")
    mlflow.set_experiment("genie-agent-test")
    yield
    mlflow.set_tracking_uri(None)


@pytest.mark.agent
def test_agent_answers_and_traces(local_mlflow):
    from fooddelivery.agent.genie_agent import GenieAgent
    from fooddelivery.agent.scorers import sql_generated

    genie = FakeGenie()
    agent = GenieAgent(workspace_client=NS(genie=genie), space_id="space1")
    resp = agent.predict({"input": [{"role": "user", "content": "How many orders in Bangalore on 2026-09-05?"}]})

    text = resp.model_dump()["output"][0]["content"][0]["text"]
    assert "Bangalore placed 868 orders." in text
    assert "| 868 |" in text
    assert "```sql" in text
    assert resp.custom_outputs["conversation_id"] == "c1"
    assert resp.custom_outputs["row_count"] == 1
    assert genie.calls[0] == ("start", "space1", "How many orders in Bangalore on 2026-09-05?")

    trace_id = mlflow.get_last_active_trace_id()
    mlflow.flush_trace_async_logging()
    trace = mlflow.get_trace(trace_id)
    names = {s.name for s in trace.data.spans}
    assert {"predict", "genie.ask", "genie.query_result"} <= names
    assert trace.info.tags["genie.status"] == "COMPLETED"
    assert sql_generated(trace=trace).value is True


@pytest.mark.agent
def test_agent_follow_up_reuses_conversation(local_mlflow):
    from fooddelivery.agent.genie_agent import GenieAgent

    genie = FakeGenie()
    agent = GenieAgent(workspace_client=NS(genie=genie), space_id="space1")
    agent.predict({"input": [{"role": "user", "content": "and Mumbai?"}], "custom_inputs": {"conversation_id": "c9"}})
    assert genie.calls[0][:3] == ("follow_up", "space1", "c9")


@pytest.mark.agent
def test_agent_reports_genie_failure(local_mlflow):
    from fooddelivery.agent.genie_agent import GenieAgent
    from fooddelivery.agent.scorers import genie_answered

    agent = GenieAgent(workspace_client=NS(genie=FakeGenie(status="FAILED")), space_id="space1")
    resp = agent.predict({"input": [{"role": "user", "content": "?"}]})
    assert "could not answer" in resp.model_dump()["output"][0]["content"][0]["text"]
    assert genie_answered(outputs=resp.model_dump()).value is False


@pytest.mark.agent
def test_agent_requires_space_id(local_mlflow):
    from fooddelivery.agent.genie_agent import GenieAgent

    with pytest.raises(RuntimeError, match="FD_GENIE_SPACE_ID"):
        _ = GenieAgent(workspace_client=NS(genie=FakeGenie())).space_id
