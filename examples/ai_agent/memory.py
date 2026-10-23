"""A session's conversation history, kept in the app's SharedState as one JSON list per
session so every interpreter sees the same history."""

import json

from mipyre import SharedState

SESSION_PREFIX = "session:"
HISTORY_LIMIT = 20


def history(state: SharedState, session_id: str) -> list[dict]:
    return json.loads(state.get(SESSION_PREFIX + session_id) or "[]")


def remember(state: SharedState, session_id: str, prompt: str, answer: str) -> None:
    """Append one exchange and keep the last HISTORY_LIMIT messages. Two requests of the same
    session at once can lose one of the two exchanges: the read and the write are separate
    calls."""
    messages = history(state, session_id)
    messages += [{"role": "user", "content": prompt}, {"role": "assistant", "content": answer}]
    state[SESSION_PREFIX + session_id] = json.dumps(messages[-HISTORY_LIMIT:])
