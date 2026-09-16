import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from coursemanager.course_code import CourseCode
from coursemanager.course_service import CourseService


class _TypingContext:
    def __init__(self, enter_error: Exception | None = None) -> None:
        self.enter_error = enter_error
        self.entered = False
        self.exited = False
        self.exit_error = None

    async def __aenter__(self) -> None:
        if self.enter_error is not None:
            raise self.enter_error
        self.entered = True

    async def __aexit__(self, exc_type, exc, traceback) -> bool:
        self.exited = True
        self.exit_error = exc
        return False


def _http_exception() -> discord.HTTPException:
    response = MagicMock()
    response.status = 500
    response.reason = "Internal Server Error"
    return discord.DiscordServerError(
        response,
        {"code": 0, "message": "Internal Server Error"},
    )


class CourseTypingResilienceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.service = object.__new__(CourseService)
        self.course_obj = CourseCode("SOCWORK-2CC3")
        self.data = {"cached_course_data": [{"course_code": "SOCWORK-2CC3"}]}
        self.ctx = MagicMock()
        self.ctx.send = AsyncMock()
        self.service._lookup_course_data = AsyncMock(
            return_value=(self.course_obj, self.data),
        )

    def assert_lookup_once(self) -> None:
        self.service._lookup_course_data.assert_awaited_once_with(
            self.ctx,
            self.course_obj,
            already_resolved=True,
        )
        self.ctx.typing.assert_called_once_with()

    async def test_typing_failure_does_not_prevent_course_lookup(self) -> None:
        typing_context = _TypingContext(_http_exception())
        self.ctx.typing.return_value = typing_context

        with patch("coursemanager.course_service.log.warning") as warning:
            (
                result_obj,
                result_data,
            ) = await self.service._fetch_and_validate_course_data(
                self.ctx,
                self.course_obj,
            )

        self.assertIs(result_obj, self.course_obj)
        self.assertIs(result_data, self.data)
        self.assert_lookup_once()
        self.ctx.send.assert_not_awaited()
        self.assertFalse(typing_context.entered)
        self.assertFalse(typing_context.exited)
        warning.assert_called_once_with(
            "Failed to start typing indicator; continuing course lookup",
            exc_info=True,
        )

    async def test_lookup_http_exception_is_not_swallowed(self) -> None:
        lookup_error = _http_exception()
        typing_context = _TypingContext()
        self.ctx.typing.return_value = typing_context
        self.service._lookup_course_data.side_effect = lookup_error

        with patch("coursemanager.course_service.log.warning") as warning:
            with self.assertRaises(discord.HTTPException) as caught:
                await self.service._fetch_and_validate_course_data(
                    self.ctx,
                    self.course_obj,
                )

        self.assertIs(caught.exception, lookup_error)
        self.assertTrue(typing_context.entered)
        self.assertTrue(typing_context.exited)
        self.assertIs(typing_context.exit_error, lookup_error)
        self.assert_lookup_once()
        self.ctx.send.assert_not_awaited()
        warning.assert_not_called()

    async def test_typing_stays_active_during_successful_lookup(self) -> None:
        typing_context = _TypingContext()
        self.ctx.typing.return_value = typing_context

        async def lookup(*args, **kwargs):
            self.assertTrue(typing_context.entered)
            self.assertFalse(typing_context.exited)
            return self.course_obj, self.data

        self.service._lookup_course_data.side_effect = lookup
        with patch("coursemanager.course_service.log.warning") as warning:
            (
                result_obj,
                result_data,
            ) = await self.service._fetch_and_validate_course_data(
                self.ctx,
                self.course_obj,
            )

        self.assertIs(result_obj, self.course_obj)
        self.assertIs(result_data, self.data)
        self.assertTrue(typing_context.exited)
        self.assertIsNone(typing_context.exit_error)
        self.assert_lookup_once()
        self.ctx.send.assert_not_awaited()
        warning.assert_not_called()

    async def test_lookup_failure_after_typing_failure_still_propagates(self) -> None:
        self.ctx.typing.return_value = _TypingContext(_http_exception())
        lookup_error = _http_exception()
        self.service._lookup_course_data.side_effect = lookup_error

        with patch("coursemanager.course_service.log.warning") as warning:
            with self.assertRaises(discord.HTTPException) as caught:
                await self.service._fetch_and_validate_course_data(
                    self.ctx,
                    self.course_obj,
                )

        self.assertIs(caught.exception, lookup_error)
        self.assert_lookup_once()
        self.ctx.send.assert_not_awaited()
        warning.assert_called_once_with(
            "Failed to start typing indicator; continuing course lookup",
            exc_info=True,
        )


if __name__ == "__main__":
    unittest.main()
