"""Text-debounce batching for the WhatsApp adapter (issue #35301).

WhatsApp delivers rapid multi-message bursts (forwarded batches, paste-splits)
individually.  Without debounce each fragment triggers a separate agent
invocation, wasting tokens and flooding the user with reply fragments.  This
mirrors the Telegram/WeCom/Feishu pattern.

Batch delays are read from ``config.extra`` (config.yaml), not env vars.
"""

import asyncio

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import MessageEvent, MessageType
from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
from gateway.session import SessionSource


def _make_adapter(**extra):
    base = {"session_name": "test"}
    base.update(extra)
    return WhatsAppAdapter(PlatformConfig(enabled=True, extra=base))


def _event(text):
    src = SessionSource(
        platform=Platform.WHATSAPP,
        chat_id="chat123",
        chat_type="dm",
        user_id="user1",
        user_name="tester",
    )
    return MessageEvent(text=text, message_type=MessageType.TEXT, source=src)


def test_batch_delays_overridden_via_config_extra():
    adapter = _make_adapter(
        text_batch_delay_seconds="2.5",
        text_batch_split_delay_seconds=7,
    )
    assert adapter._text_batch_delay_seconds == 2.5
    assert adapter._text_batch_split_delay_seconds == 7.0


def test_invalid_config_value_falls_back_to_default():
    adapter = _make_adapter(
        text_batch_delay_seconds="garbage",
        text_batch_split_delay_seconds=-3,
    )
    assert adapter._text_batch_delay_seconds == 5.0
    assert adapter._text_batch_split_delay_seconds == 10.0




@pytest.mark.asyncio
async def test_text_flush_survives_followup_cancel():
    """A follow-up chunk cancels the in-flight flush task; the already-popped
    event must still be dispatched (shield), not silently lost.

    Regression: the flush popped the event then awaited handle_message
    unshielded, so the follow-up's cancel aborted the dispatch mid-flight.
    """
    adapter = _make_adapter(text_batch_delay_seconds="0.05")
    handled = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_handle(event):
        entered.set()
        await release.wait()
        handled.append(event.text)
        return True

    adapter.handle_message = slow_handle

    adapter._enqueue_text_event(_event("first part"))
    await asyncio.wait_for(entered.wait(), timeout=2)
    adapter._enqueue_text_event(_event("second part"))
    await asyncio.sleep(0.05)
    release.set()
    await asyncio.sleep(0.3)

    assert handled == ["first part", "second part"]
