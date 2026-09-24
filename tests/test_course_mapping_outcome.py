"""Category mapping outcome regressions against the real CourseManager modules."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from coursemanager.channel_service import ChannelService, MappingResult
from coursemanager.course_commands import CourseChannelCog
from coursemanager.course_service import CourseService


class MappingOutcomeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.guild = SimpleNamespace(id=1, name="test")
        self.config = SimpleNamespace(course_category=AsyncMock(return_value="COURSES"))
        self.service = ChannelService(object(), self.config)
        self.mapping = {"BIO-2AA3": "COURSES-1"}
        self.service._cleanup_categories = AsyncMock(return_value=True)
        self.service._reorder_course_categories = AsyncMock()

    async def test_category_creation_none_cannot_report_success(self):
        old_category = SimpleNamespace(name="COURSES")
        channel = SimpleNamespace(name="bio-2aa3", category=old_category, edit=AsyncMock())
        with patch("coursemanager.channel_service._iter_course_text_channels", return_value=[channel]), patch("coursemanager.channel_service.get_or_create_category", new_callable=AsyncMock, return_value=None):
            result = await self.service.apply_category_mapping(self.guild, self.mapping)
        self.assertEqual(result.name, "FAILED")
        channel.edit.assert_not_awaited()
        self.service._cleanup_categories.assert_not_awaited()

    async def test_successful_move_and_manual_success(self):
        channel = SimpleNamespace(name="bio-2aa3", category=SimpleNamespace(name="COURSES"), edit=AsyncMock())
        category = SimpleNamespace(name="COURSES-1")
        with patch("coursemanager.channel_service._iter_course_text_channels", return_value=[channel]), patch("coursemanager.channel_service.get_or_create_category", new_callable=AsyncMock, return_value=category), patch("coursemanager.channel_service._safe_sleep", new_callable=AsyncMock):
            result = await self.service.apply_category_mapping(self.guild, self.mapping)
        self.assertIs(result, MappingResult.SUCCESS)
        channel.edit.assert_awaited_once_with(category=category)
        self.assertIn("Reclustered and moved", await self._manual_response(result))

    async def test_one_failed_move_continues_and_reports_partial(self):
        first = SimpleNamespace(name="bio-2aa3", category=SimpleNamespace(name="COURSES"), edit=AsyncMock(side_effect=RuntimeError("move denied")))
        second = SimpleNamespace(name="mat-1aa3", category=SimpleNamespace(name="COURSES"), edit=AsyncMock())
        category = SimpleNamespace(name="COURSES-1")
        mapping = {"BIO-2AA3": "COURSES-1", "MAT-1AA3": "COURSES-1"}
        with patch("coursemanager.channel_service._iter_course_text_channels", return_value=[first, second]), patch("coursemanager.channel_service.get_or_create_category", new_callable=AsyncMock, return_value=category), patch("coursemanager.channel_service._safe_sleep", new_callable=AsyncMock):
            result = await self.service.apply_category_mapping(self.guild, mapping)
        self.assertIs(result, MappingResult.PARTIAL)
        second.edit.assert_awaited_once_with(category=category)
        self.service._cleanup_categories.assert_not_awaited()
        self.service._reorder_course_categories.assert_not_awaited()
        self.assertIn("Some course channels", await self._manual_response(result))

    async def test_missing_mapping_channel_is_not_a_false_success(self):
        with patch("coursemanager.channel_service._iter_course_text_channels", return_value=[]):
            result = await self.service.apply_category_mapping(self.guild, self.mapping)
        self.assertIs(result, MappingResult.FAILED)

    async def test_cleanup_failure_warns_after_required_moves(self):
        channel = SimpleNamespace(name="bio-2aa3", category=SimpleNamespace(name="COURSES-1"))
        self.service._cleanup_categories.return_value = False
        with patch("coursemanager.channel_service._iter_course_text_channels", return_value=[channel]):
            result = await self.service.apply_category_mapping(self.guild, self.mapping)
        self.assertIs(result, MappingResult.SUCCESS_WITH_WARNINGS)
        self.assertIn("cleanup or ordering was incomplete", await self._manual_response(result))

    async def test_reorder_failure_warns_but_does_not_undo_moves(self):
        channel = SimpleNamespace(name="bio-2aa3", category=SimpleNamespace(name="COURSES-1"))
        self.service._reorder_course_categories.side_effect = RuntimeError("reorder denied")
        with patch("coursemanager.channel_service._iter_course_text_channels", return_value=[channel]):
            result = await self.service.apply_category_mapping(self.guild, self.mapping)
        self.assertIs(result, MappingResult.SUCCESS_WITH_WARNINGS)
        self.assertIn("cleanup or ordering was incomplete", await self._manual_response(result))

    async def test_cancellation_propagates_from_move(self):
        channel = SimpleNamespace(name="bio-2aa3", category=SimpleNamespace(name="COURSES"), edit=AsyncMock(side_effect=asyncio.CancelledError))
        with patch("coursemanager.channel_service._iter_course_text_channels", return_value=[channel]), patch("coursemanager.channel_service.get_or_create_category", new_callable=AsyncMock, return_value=SimpleNamespace(name="COURSES-1")):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.apply_category_mapping(self.guild, self.mapping)

    async def test_cleanup_delete_failure_returns_warning(self):
        import discord

        category = SimpleNamespace(name="COURSES-OLD", channels=[], delete=AsyncMock(side_effect=discord.Forbidden(Mock(status=403, reason="Forbidden"), "denied")))
        with patch("coursemanager.channel_service.get_categories_by_prefix", return_value=[category]):
            self.assertFalse(await ChannelService._cleanup_categories(self.service, self.guild, self.mapping))

    async def test_automatic_clustering_logs_incomplete_outcomes(self):
        for outcome, level in ((MappingResult.PARTIAL, "warning"), (MappingResult.FAILED, "error"), (MappingResult.SUCCESS_WITH_WARNINGS, "warning")):
            with self.subTest(outcome=outcome):
                service = object.__new__(CourseService)
                service.bot = SimpleNamespace(wait_until_ready=AsyncMock(), is_closed=Mock(side_effect=[False, True]), guilds=[self.guild])
                service.config = SimpleNamespace(enabled_guilds=AsyncMock(return_value=[self.guild.id]), course_groups=SimpleNamespace(set=AsyncMock()))
                service.gather_course_user_data = AsyncMock(return_value={})
                channels = SimpleNamespace(apply_category_mapping=AsyncMock(return_value=outcome))
                clustering = SimpleNamespace(cluster_courses=Mock(return_value=self.mapping))
                with patch("coursemanager.course_service.asyncio.sleep", new_callable=AsyncMock), patch("coursemanager.course_service.log") as logger:
                    await service.auto_course_clustering(channels, clustering, interval=1)
                getattr(logger, level).assert_called()

    async def _manual_response(self, result):
        cog = object.__new__(CourseChannelCog)
        cog.course_service = SimpleNamespace(gather_course_user_data=AsyncMock(return_value={}))
        cog.clustering = SimpleNamespace(cluster_courses=Mock(return_value=self.mapping))
        cog.config = SimpleNamespace(course_groups=SimpleNamespace(set=AsyncMock()))
        cog.channel_service = SimpleNamespace(apply_category_mapping=AsyncMock(return_value=result))
        ctx = SimpleNamespace(guild=self.guild, send=AsyncMock())
        await CourseChannelCog.recluster.callback.__wrapped__(cog, ctx)
        return ctx.send.await_args.args[0]

    async def test_manual_recluster_reports_incomplete_when_mapping_fails(self):
        response = await self._manual_response(MappingResult.FAILED)
        self.assertIn("could not be reclustered", response)


if __name__ == "__main__":
    unittest.main()
