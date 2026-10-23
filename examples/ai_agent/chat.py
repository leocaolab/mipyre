"""The chat routes. /chat validates its body with pydantic, which can't load in a
sub-interpreter, and /stream returns a Stream, which needs the main interpreter; both keep
the session history in app.state, which a worker process can't reach. So they run on
runtime="main", and only the main interpreter imports this module."""

import json
import threading
import time
from collections.abc import Iterator

from pydantic import BaseModel

from mipyre import Router, SharedState, Stream

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


def send_answer(stream: Stream, state: SharedState, prompt: str, session_id: str) -> None:
    tokens = []
    try:
        for token in fake_llm_generate(prompt):
            stream.send_event(token, event="token")
            tokens.append(token)
        stream.send_event(json.dumps({"done": True, "tokens": len(tokens)}), event="done")
    except ConnectionError:
        return  # the client went away; the answer it didn't read is not remembered
    remember(state, session_id, prompt, "".join(tokens))
    stream.close()


@router.get("/stream")
def stream_chat(req):
    prompt = req.query_params.get("prompt", "hello")
    session_id = req.query_params.get("session_id", "default")
    stream = Stream()
    threading.Thread(target=send_answer, args=(stream, _state(), prompt, session_id), daemon=True).start()
    return stream
