"""Focused CourseManager regressions for course data and channel operations."""

import asyncio
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import discord

from coursemanager.channel_service import ChannelService
from coursemanager.course_channel_clustering import CourseChannelClustering
from coursemanager.course_data_proxy import CourseDataProxy
from coursemanager.course_code import CourseCode
from coursemanager.course_service import CourseService


class ExistingFixTests(unittest.IsolatedAsyncioTestCase):
    async def test_term_scoped_miss_falls_through_to_next_term(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._now = lambda: datetime(2026, 1, 1, tzinfo=timezone.utc)
        proxy._determine_term_order_refined = AsyncMock(return_value=[("winter", 2026), ("spring", 2026)])
        proxy._determine_term_order_fallback = lambda: []
        proxy._should_skip_invalid = lambda pair: False
        proxy._get_term_id = AsyncMock(side_effect=[101, 202])
        proxy._build_url = lambda term_id, course: str(term_id)
        soup = object()
        proxy._fetch_with_backoff = AsyncMock(side_effect=[(None, "course not found"), (soup, None)])
        proxy._record_invalid = unittest.mock.Mock()

        result = await proxy._fetch_course_online("BIO-2AA3")

        self.assertEqual(result, (soup, None))
        self.assertEqual([call.args[0] for call in proxy._fetch_with_backoff.await_args_list], ["101", "202"])
        proxy._record_invalid.assert_called_with(("BIO-2AA3", "winter-2026"))

    async def test_all_term_scoped_misses_return_not_found_after_fallback(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._now = lambda: datetime(2026, 1, 1, tzinfo=timezone.utc)
        proxy._determine_term_order_refined = AsyncMock(return_value=[("winter", 2026), ("spring", 2026)])
        proxy._determine_term_order_fallback = lambda: [("winter", 2026)]
        proxy._should_skip_invalid = lambda pair: False
        proxy._get_term_id = AsyncMock(side_effect=[101, 202])
        proxy._build_url = lambda term_id, course: str(term_id)
        proxy._fetch_with_backoff = AsyncMock(return_value=(None, "course not found"))
        proxy._record_invalid = unittest.mock.Mock()

        self.assertEqual(await proxy._fetch_course_online("BIO-2AA3"), (None, "course not found"))
        self.assertEqual(proxy._fetch_with_backoff.await_count, 2)
        self.assertEqual({call.args[0] for call in proxy._record_invalid.call_args_list},
                         {("BIO-2AA3", "winter-2026"), ("BIO-2AA3", "spring-2026")})

    async def test_first_term_success_does_not_query_later_terms(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._now = lambda: datetime(2026, 1, 1, tzinfo=timezone.utc)
        proxy._determine_term_order_refined = AsyncMock(return_value=[("winter", 2026), ("spring", 2026)])
        proxy._determine_term_order_fallback = lambda: []
        proxy._should_skip_invalid = lambda pair: False
        proxy._get_term_id = AsyncMock(return_value=101)
        proxy._build_url = lambda term_id, course: str(term_id)
        soup = object()
        proxy._fetch_with_backoff = AsyncMock(return_value=(soup, None))
        proxy._record_invalid = unittest.mock.Mock()

        self.assertEqual(await proxy._fetch_course_online("BIO-2AA3"), (soup, None))
        proxy._fetch_with_backoff.assert_awaited_once()
        proxy._record_invalid.assert_not_called()

    async def test_transient_first_term_can_fall_through_without_invalidation(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._now = lambda: datetime(2026, 1, 1, tzinfo=timezone.utc)
        proxy._determine_term_order_refined = AsyncMock(return_value=[("winter", 2026), ("spring", 2026)])
        proxy._determine_term_order_fallback = lambda: []
        proxy._should_skip_invalid = lambda pair: False
        proxy._get_term_id = AsyncMock(side_effect=[101, 202])
        proxy._build_url = lambda term_id, course: str(term_id)
        soup = object()
        proxy._fetch_with_backoff = AsyncMock(side_effect=[(None, "network error"), (soup, None)])
        proxy._record_invalid = unittest.mock.Mock()

        self.assertEqual(await proxy._fetch_course_online("BIO-2AA3"), (soup, None))
        self.assertEqual(proxy._fetch_with_backoff.await_count, 2)
        proxy._record_invalid.assert_not_called()

    async def test_mixed_not_found_and_network_failure_is_not_final_not_found(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._now = lambda: datetime(2026, 1, 1, tzinfo=timezone.utc)
        proxy._determine_term_order_refined = AsyncMock(return_value=[("winter", 2026), ("spring", 2026)])
        proxy._determine_term_order_fallback = lambda: []
        proxy._should_skip_invalid = lambda pair: False
        proxy._get_term_id = AsyncMock(side_effect=[101, 202])
        proxy._build_url = lambda term_id, course: str(term_id)
        proxy._fetch_with_backoff = AsyncMock(side_effect=[(None, "course not found"), (None, "network error")])
        proxy._record_invalid = unittest.mock.Mock()

        self.assertEqual(await proxy._fetch_course_online("BIO-2AA3"),
                         (None, "Unknown error fetching course data."))
        proxy._record_invalid.assert_called_once_with(("BIO-2AA3", "winter-2026"))

    async def test_missing_term_id_prevents_definitive_not_found(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._now = lambda: datetime(2026, 1, 1, tzinfo=timezone.utc)
        proxy._determine_term_order_refined = AsyncMock(return_value=[("winter", 2026), ("spring", 2026)])
        proxy._determine_term_order_fallback = lambda: []
        proxy._should_skip_invalid = lambda pair: False
        proxy._get_term_id = AsyncMock(side_effect=[101, None])
        proxy._build_url = lambda term_id, course: str(term_id)
        proxy._fetch_with_backoff = AsyncMock(return_value=(None, "course not found"))
        proxy._record_invalid = unittest.mock.Mock()

        self.assertEqual(await proxy._fetch_course_online("BIO-2AA3"),
                         (None, "Unknown error fetching course data."))
        proxy._fetch_with_backoff.assert_awaited_once_with("101", "winter-2026", ("BIO-2AA3", "winter-2026"))
        proxy._record_invalid.assert_called_once_with(("BIO-2AA3", "winter-2026"))

    async def test_transient_errors_retry_without_invalidating_pair(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._record_invalid = unittest.mock.Mock()
        pair = ("BIO-2AA3", "winter-2026")
        for transient in ("network error", "HTTP 429", "HTTP 503", "unexpected error"):
            with self.subTest(transient=transient):
                proxy._fetch_and_parse = AsyncMock(side_effect=[(None, transient), (object(), None)])
                proxy._record_invalid.reset_mock()
                with patch("coursemanager.course_data_proxy.BASE_DELAY_SECONDS", 0):
                    soup, err = await proxy._fetch_with_backoff("url", pair[1], pair)
                self.assertIsNotNone(soup)
                self.assertIsNone(err)
                self.assertEqual(proxy._fetch_and_parse.await_count, 2)
                proxy._record_invalid.assert_not_called()

    async def test_exhausted_timeouts_do_not_invalidate_pair(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._record_invalid = unittest.mock.Mock()
        proxy._fetch_and_parse = AsyncMock(return_value=(None, "network error"))
        with patch("coursemanager.course_data_proxy.BASE_DELAY_SECONDS", 0):
            result = await proxy._fetch_with_backoff("url", "winter-2026", ("BIO-2AA3", "winter-2026"))
        self.assertEqual(result, (None, "network error"))
        self.assertEqual(proxy._fetch_and_parse.await_count, 2)
        proxy._record_invalid.assert_not_called()

    async def test_confirmed_not_found_invalidates_only_current_pair(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._record_invalid = unittest.mock.Mock()
        proxy._fetch_and_parse = AsyncMock(return_value=(None, "course not found"))
        pair = ("BIO-2AA3", "winter-2026")
        result = await proxy._fetch_with_backoff("url", pair[1], pair)
        self.assertEqual(result, (None, "course not found"))
        proxy._record_invalid.assert_called_once_with(pair)
        proxy._fetch_and_parse.assert_awaited_once()

    async def test_auth_or_other_client_error_does_not_invalidate_course_term(self):
        proxy = object.__new__(CourseDataProxy)
        proxy.log = SimpleNamespace(debug=lambda *args: None)
        proxy._record_invalid = unittest.mock.Mock()
        pair = ("BIO-2AA3", "winter-2026")
        for err in ("HTTP 401", "HTTP 403", "authentication required"):
            with self.subTest(err=err):
                proxy._fetch_and_parse = AsyncMock(return_value=(None, err))
                proxy._record_invalid.reset_mock()
                self.assertEqual(await proxy._fetch_with_backoff("url", pair[1], pair), (None, err))
                proxy._fetch_and_parse.assert_awaited_once()
                proxy._record_invalid.assert_not_called()

    async def test_term_change_refreshes_id_and_only_affected_invalid_pairs(self):
        config = SimpleNamespace(term_codes=AsyncMock(return_value={"fall-2026": 202, "winter-2026": 303}))
        proxy = CourseDataProxy(config, unittest.mock.Mock())
        now = datetime.now(timezone.utc)
        proxy._term_codes_last_update = now
        proxy._term_codes_cache = {"fall-2026": 101, "winter-2026": 303}
        proxy._invalid_course_term_cache = {
            ("BIO-2AA3", "fall-2026"): now,
            ("BIO-2AA3", "winter-2026"): now,
        }

        proxy.invalidate_term_code("fall-2026")

        self.assertEqual(await proxy._get_term_id("fall-2026"), 202)
        config.term_codes.assert_awaited_once()
        self.assertEqual(set(proxy._invalid_course_term_cache), {("BIO-2AA3", "winter-2026")})

    async def test_disjoint_courses_cluster_with_bounded_categories(self):
        clustering = CourseChannelClustering()
        members = {f"C-{n:04d}": {n} for n in range(51)}
        result = clustering.cluster_courses(members)
        self.assertEqual(set(result), set(members))
        self.assertLessEqual(max(Counter(result.values()).values()), 40)
        self.assertEqual(clustering.evaluate_clusters(clustering._build_graph(members), [{c} for c in members]), {"modularity": 0.0})

    async def test_connected_graph_still_uses_custom_clustering(self):
        calls = []

        def custom(graph):
            calls.append(graph.number_of_edges())
            return [set(graph.nodes())]

        clustering = CourseChannelClustering(clustering_func=custom)
        result = clustering.cluster_courses({"BIO-2AA3": {1, 2}, "CHEM-2AA3": {1, 2}})
        self.assertEqual(calls, [1])
        self.assertEqual(set(result), {"BIO-2AA3", "CHEM-2AA3"})


class PruneLoopTests(unittest.IsolatedAsyncioTestCase):
    def make_service(self):
        service = object.__new__(ChannelService)
        service.bot = SimpleNamespace(
            wait_until_ready=AsyncMock(), guilds=[SimpleNamespace(id=1), SimpleNamespace(id=2)],
            is_closed=iter([False, True]).__next__,
        )
        service.config = SimpleNamespace(
            prune_threshold_days=AsyncMock(return_value=120),
            channel_prune_interval=AsyncMock(return_value=1),
            enabled_guilds=AsyncMock(return_value=[1, 2]),
            course_category=AsyncMock(return_value="COURSES"),
        )
        return service

    async def test_bounded_bot_history_is_inconclusive_only_at_limit(self):
        service = self.make_service()
        service.config.channel_prune_history_limit = AsyncMock(return_value=15)
        created = datetime(2025, 1, 1, tzinfo=timezone.utc)

        class Channel:
            last_message = None
            created_at = created

            def __init__(self, count):
                self.count = count

            async def history(self, *, limit):
                for _ in range(min(self.count, limit)):
                    yield SimpleNamespace(author=SimpleNamespace(bot=True))

        self.assertIsNone(await service._compute_last_activity(Channel(16)))
        self.assertEqual(await service._compute_last_activity(Channel(14)), created)

    async def test_inspection_failure_does_not_stop_later_channels_or_guilds(self):
        service = object.__new__(ChannelService)
        first, second, third = object(), object(), object()
        guild_a = SimpleNamespace(id=1, name="one")
        guild_b = SimpleNamespace(id=2, name="two")
        service.bot = SimpleNamespace(
            wait_until_ready=AsyncMock(), guilds=[guild_a, guild_b],
            is_closed=iter([False, True]).__next__,
        )
        service.config = SimpleNamespace(
            prune_threshold_days=AsyncMock(return_value=120),
            channel_prune_interval=AsyncMock(return_value=1),
            enabled_guilds=AsyncMock(return_value=[1, 2]),
            course_category=AsyncMock(return_value="COURSES"),
        )
        seen = []

        async def inspect(guild, channel, threshold):
            seen.append((guild.id, channel))
            if channel is first:
                raise RuntimeError("history temporarily unavailable")

        service.channel_prune_helper = inspect
        with patch("coursemanager.channel_service._iter_course_text_channels", side_effect=lambda guild, _: [first, second] if guild.id == 1 else [third]), patch("coursemanager.channel_service._safe_sleep", new_callable=AsyncMock):
            await service.auto_channel_prune()
        self.assertEqual(seen, [(1, first), (1, second), (2, third)])

    async def test_guild_enumeration_failure_does_not_stop_next_guild(self):
        service = self.make_service()
        channel = object()
        service.channel_prune_helper = AsyncMock()

        def channels(guild, prefix):
            if guild.id == 1:
                raise RuntimeError("guild cache unavailable")
            return [channel]

        with patch("coursemanager.channel_service._iter_course_text_channels", side_effect=channels), patch("coursemanager.channel_service._safe_sleep", new_callable=AsyncMock):
            await service.auto_channel_prune()
        service.channel_prune_helper.assert_awaited_once()
        self.assertEqual(service.channel_prune_helper.await_args.args[0].id, 2)

    async def test_cancellation_propagates_without_another_cycle(self):
        service = self.make_service()
        service.bot.is_closed = lambda: False
        service.channel_prune_helper = AsyncMock(side_effect=asyncio.CancelledError())
        with patch("coursemanager.channel_service._iter_course_text_channels", return_value=[object()]), patch("coursemanager.channel_service._safe_sleep", new_callable=AsyncMock) as sleep:
            with self.assertRaises(asyncio.CancelledError):
                await service.auto_channel_prune()
        sleep.assert_not_awaited()

    async def test_persistent_startup_config_failure_does_not_spin(self):
        service = self.make_service()
        service.config.prune_threshold_days.side_effect = RuntimeError("config unavailable")
        with self.assertRaises(RuntimeError):
            await service.auto_channel_prune()
        service.config.prune_threshold_days.assert_awaited_once()

    async def test_delete_permission_and_http_failures_remain_nonfatal(self):
        service = self.make_service()
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        service._now = lambda: now
        service._compute_last_activity = AsyncMock(return_value=now.replace(year=2025))
        response = unittest.mock.Mock(status=403, reason="Forbidden")
        channel = SimpleNamespace(name="bio-2aa3", delete=AsyncMock())
        for failure in (discord.Forbidden(response, "denied"), discord.HTTPException(response, "failed")):
            channel.delete.side_effect = failure
            await service.channel_prune_helper(SimpleNamespace(id=1), channel, timedelta(days=120))
        self.assertEqual(channel.delete.await_count, 2)


class ConcurrentJoinTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_course_reuses_channel_created_by_first_request(self):
        service = CourseService(object(), object())
        guild = SimpleNamespace(id=1)
        course = CourseCode("BIO-2AA3")
        channels = {}
        service.get_course_channel = lambda _guild, code: channels.get(code.formatted_channel_name())
        started = asyncio.Event()
        release = asyncio.Event()
        creates = []

        async def create(_guild, _category, code):
            creates.append(code.formatted_channel_name())
            started.set()
            await release.wait()
            channel = object()
            channels[code.formatted_channel_name()] = channel
            return channel

        service.create_course_channel = create
        first = asyncio.create_task(service._get_or_create_channel(guild, course, object()))
        await started.wait()
        second = asyncio.create_task(service._get_or_create_channel(guild, course, object()))
        await asyncio.sleep(0)
        release.set()
        result = await asyncio.gather(first, second)
        self.assertIs(result[0], result[1])
        self.assertEqual(creates, [course.formatted_channel_name()])
        self.assertEqual(len(service._channel_creation_locks), 0)

    async def test_different_courses_create_independently(self):
        service = CourseService(object(), object())
        guild = SimpleNamespace(id=1)
        service.get_course_channel = lambda guild, course: None
        both_started = asyncio.Event()
        release = asyncio.Event()
        started = []

        async def create(_guild, _category, course):
            started.append(course.formatted_channel_name())
            if len(started) == 2:
                both_started.set()
            await release.wait()
            return object()

        service.create_course_channel = create
        tasks = [asyncio.create_task(service._get_or_create_channel(guild, CourseCode(code), object()))
                 for code in ("BIO-2AA3", "CHEM-2AA3")]
        try:
            await asyncio.wait_for(both_started.wait(), timeout=1)
            self.assertEqual(set(started), {"bio-2aa3", "chem-2aa3"})
        finally:
            release.set()
            await asyncio.gather(*tasks)

    async def test_creation_failure_releases_lock_for_retry(self):
        service = CourseService(object(), object())
        guild = SimpleNamespace(id=1)
        course = CourseCode("BIO-2AA3")
        channels = {}
        service.get_course_channel = lambda _guild, code: channels.get(code.formatted_channel_name())
        calls = 0

        async def create(_guild, _category, code):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("create failed")
            channels[code.formatted_channel_name()] = object()
            return channels[code.formatted_channel_name()]

        service.create_course_channel = create
        with self.assertRaises(RuntimeError):
            await service._get_or_create_channel(guild, course, object())
        result = await asyncio.wait_for(service._get_or_create_channel(guild, course, object()), timeout=1)
        self.assertIs(result, channels[course.formatted_channel_name()])
        self.assertEqual(calls, 2)

    async def test_cancellation_while_waiting_preserves_lock(self):
        service = CourseService(object(), object())
        guild = SimpleNamespace(id=1)
        course = CourseCode("BIO-2AA3")
        channels = {}
        service.get_course_channel = lambda _guild, code: channels.get(code.formatted_channel_name())
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def create(_guild, _category, code):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            channels[code.formatted_channel_name()] = object()
            return channels[code.formatted_channel_name()]

        service.create_course_channel = create
        first = asyncio.create_task(service._get_or_create_channel(guild, course, object()))
        await started.wait()
        waiting = asyncio.create_task(service._get_or_create_channel(guild, course, object()))
        await asyncio.sleep(0)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        release.set()
        result = await first
        self.assertIs(await asyncio.wait_for(service._get_or_create_channel(guild, course, object()), timeout=1), result)
        self.assertEqual(calls, 1)

    async def test_cancellation_during_create_allows_later_retry(self):
        service = CourseService(object(), object())
        guild = SimpleNamespace(id=1)
        course = CourseCode("BIO-2AA3")
        channels = {}
        service.get_course_channel = lambda _guild, code: channels.get(code.formatted_channel_name())
        started = asyncio.Event()
        creates = 0

        async def create(_guild, _category, code):
            nonlocal creates
            creates += 1
            if creates == 1:
                started.set()
                await asyncio.Event().wait()
            channels[code.formatted_channel_name()] = object()
            return channels[code.formatted_channel_name()]

        service.create_course_channel = create
        first = asyncio.create_task(service._get_or_create_channel(guild, course, object()))
        await started.wait()
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        self.assertIsNotNone(await asyncio.wait_for(service._get_or_create_channel(guild, course, object()), timeout=1))
        self.assertEqual(creates, 2)


if __name__ == "__main__":
    unittest.main()
