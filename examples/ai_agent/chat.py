"""The chat routes. /chat validates its body with pydantic, which can't load in a
sub-interpreter, and /stream returns a Stream, which needs the main interpreter; both keep
the session history in app.state, which a worker process can't reach. So they run on
runtime="main", and only the main interpreter imports this module."""

import json
import time
from collections.abc import Iterator

from pydantic import BaseModel

from mipyre import EventSourceResponse, Router, ServerSentEvent, SharedState, Stream

from ai_agent.memory import remember

router = Router(runtime="main")

TOKEN_DELAY_SECONDS = 0.03


class ChatRequest(BaseModel):
    prompt: str
    session_id: str = "default"
    max_tokens: int = 50


def fake_llm_generate(prompt: str, max_tokens: int = 50) -> Iterator[str]:
    """Yield the words of a canned answer, one every TOKEN_DELAY_SECONDS. Replace it with
    a real model client."""
    answer = (
        f"I received your question about '{prompt}'. Here is a thoughtful response that "
        "demonstrates the streaming capabilities of the Mipyre framework. Each token is "
        "sent individually with realistic delays."
    )
    for word in answer.split()[:max_tokens]:
        time.sleep(TOKEN_DELAY_SECONDS)
        yield word + " "


def _state() -> SharedState:
    # ai_agent.main imports this module while it builds the app, so the app is looked up
    # when a request comes, not at import.
    from ai_agent.main import app

    return app.state


@router.post("/chat", body=ChatRequest)
def chat(body: ChatRequest):
    tokens = list(fake_llm_generate(body.prompt, body.max_tokens))
    answer = "".join(tokens)
    remember(_state(), body.session_id, body.prompt, answer)
    return {"response": answer, "session_id": body.session_id, "tokens": len(tokens)}


def answer_events(state: SharedState, prompt: str, session_id: str) -> Iterator[ServerSentEvent]:
    """Yield the answer's tokens, then a ``done`` event, then remember the answer. A client
    that leaves closes the generator at a ``yield``, so an answer it didn't read is not
    remembered."""
    tokens = []
    for token in fake_llm_generate(prompt):
        yield ServerSentEvent(token, event="token")
        tokens.append(token)
    yield ServerSentEvent(json.dumps({"done": True, "tokens": len(tokens)}), event="done")
    remember(state, session_id, prompt, "".join(tokens))


@router.get("/stream")
def stream_chat(req) -> Stream:
    prompt = req.query_params.get("prompt", "hello")
    session_id = req.query_params.get("session_id", "default")
    return EventSourceResponse(answer_events(_state(), prompt, session_id))
