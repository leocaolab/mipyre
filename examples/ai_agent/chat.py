"""The chat routes and the session memory they keep. /chat validates its body with
pydantic, which can't load in a sub-interpreter, and /stream returns a Stream, so they run
on the main interpreter and only the main interpreter imports this module."""

import json
import threading
import time

from pydantic import BaseModel

from mipyre import Router, SharedState, Stream

router = Router(runtime="main")


def _state() -> SharedState:
    # ai_agent.main imports this module while it builds its app, so the app is looked up
    # when a request comes, not at import.
    from ai_agent.main import app

    return app.state


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    prompt: str
    session_id: str = "default"
    max_tokens: int = 50


# ---------------------------------------------------------------------------
# Simulated LLM (replace with a real LLM client)
# ---------------------------------------------------------------------------

def fake_llm_generate(prompt: str, max_tokens: int = 50):
    """Simulate LLM token generation with realistic latency."""
    response = f"I received your question about '{prompt}'. "
    response += "Here is a thoughtful response that demonstrates "
    response += "streaming capabilities of the Mipyre framework. "
    response += "Each token is sent individually with realistic delays."

    words = response.split()
    for word in words[:max_tokens]:
        time.sleep(0.03)  # ~30ms per token, realistic for LLM
        yield word + " "


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/chat", model=ChatRequest)
def chat(req, request: ChatRequest):
    """Synchronous chat — returns full response at once."""
    state = _state()
    # Build response from simulated LLM
    tokens = list(fake_llm_generate(request.prompt, request.max_tokens))
    response = "".join(tokens)

    # Store in session memory (SharedState — cross-worker, nanosecond)
    history = json.loads(state.get(f"session:{request.session_id}") or "[]")
    history.append({"role": "user", "content": request.prompt})
    history.append({"role": "assistant", "content": response})
    state[f"session:{request.session_id}"] = json.dumps(history[-20:])  # Keep last 20

    return {
        "response": response,
        "session_id": request.session_id,
        "tokens": len(tokens),
    }


@router.get("/stream")
def stream_chat(req):
    """SSE streaming — sends tokens one by one (like ChatGPT)."""
    state = _state()
    prompt = req.query_params.get("prompt", "hello")
    session_id = req.query_params.get("session_id", "default")

    stream = Stream()

    def generate():
        full_response = []
        for token in fake_llm_generate(prompt):
            stream.send_event(token, event="token")
            full_response.append(token)

        # Store in memory
        response_text = "".join(full_response)
        history = json.loads(state.get(f"session:{session_id}") or "[]")
        history.append({"role": "user", "content": prompt})
        history.append({"role": "assistant", "content": response_text})
        state[f"session:{session_id}"] = json.dumps(history[-20:])

        stream.send_event(json.dumps({"done": True, "tokens": len(full_response)}), event="done")
        stream.close()

    threading.Thread(target=generate, daemon=True).start()
    return stream


@router.get("/memory/{session_id}")
def get_memory(req):
    """View conversation history for a session."""
    session_id = req.params["session_id"]
    history = json.loads(_state().get(f"session:{session_id}") or "[]")
    return {"session_id": session_id, "messages": history, "count": len(history)}
