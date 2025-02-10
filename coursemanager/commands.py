import asyncio
import logging
from typing import Optional

import discord
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import error, info, success, warning

from .channel_service import ChannelService
from .course_service import CourseService
from .constants import GLOBAL_DEFAULTS

log = logging.getLogger("red.course_channel_cog")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(logging.StreamHandler())


class CourseChannelCog(commands.Cog):
    """Cog for managing course channels and dynamic grouping."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.config: Config = Config.get_conf(
            self, identifier=1234567890, force_registration=True
        )
        self.config.register_global(**GLOBAL_DEFAULTS)
        self.channel_service: ChannelService = ChannelService(bot, self.config)
        self.course_service: CourseService = CourseService(bot, self.config)
        self._grouping_task: Optional[asyncio.Task[None]] = asyncio.create_task(
            self.channel_service.dynamic_grouping_task()
        )
        self._prune_task: Optional[asyncio.Task[None]] = asyncio.create_task(
            self.channel_service.auto_prune_task()
        )
        log.debug("CourseChannelCog initialized.")

    async def cog_check(self, ctx: commands.Context) -> bool:
        """
        Global check for commands in this cog.

        For commands in the 'course' group (except for 'enable' and 'disable')
        we require that Course Manager is enabled in the guild. This prevents
        join/leave and similar commands from running when Course Manager is disabled.
        """
        # Allow DM commands.
        if ctx.guild is None:
            return True

        # Identify commands that are part of the 'course' group.
        # We exempt the commands that enable or disable the manager,
        # as well as the top-level group command itself.
        if ctx.command.qualified_name.lower().startswith("course"):
            if ctx.command.name.lower() in {"enable", "disable", "course"}:
                return True
            enabled = await self.config.enabled_guilds()
            if ctx.guild.id not in enabled:
                await ctx.send(
                    error(
                        "Course Manager is disabled in this server. "
                        "Please enable it using `course enable`."
                    )
                )
                return False
        return True

    def cog_unload(self) -> None:
        log.debug("Unloading CourseChannelCog; cancelling background tasks.")
        if self._grouping_task:
            self._grouping_task.cancel()
        if self._prune_task:
            self._prune_task.cancel()

    # Channel commands

    @commands.command(name="setdefaultcategory")
    async def set_default_category(
        self, ctx: commands.Context, *, category_name: str
    ) -> None:
        """Set the default category for channel creation."""
        await self.channel_service.set_default_category(ctx, category_name)
        await ctx.send(success(f"Default category set to **{category_name}**"))

    @commands.command(name="createchannel")
    async def create_channel(
        self,
        ctx: commands.Context,
        channel_name: str,
        category: Optional[discord.CategoryChannel] = None,
    ) -> None:
        """Create a new text channel in the specified or default category."""
        await self.channel_service.create_channel(ctx, channel_name, category)

    @commands.command(name="deletechannel")
    async def delete_channel(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        """Delete the specified text channel."""
        await self.channel_service.delete_channel(ctx, channel)

    @commands.command(name="listchannels")
    async def list_channels(
        self,
        ctx: commands.Context,
        *,
        category: Optional[discord.CategoryChannel] = None,
    ) -> None:
        """List text channels in a category or across the server."""
        await self.channel_service.list_channels(ctx, category)

    @commands.command(name="setchannelperm")
    async def set_channel_permission(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel,
        member: discord.Member,
        allow: bool,
    ) -> None:
        """Set or remove permissions for a member in a channel."""
        await self.channel_service.set_channel_permission(ctx, channel, member, allow)

    # Course commands

    @commands.group(name="course", invoke_without_command=True)
    async def course(self, ctx: commands.Context) -> None:
        """Main command group for course functionalities."""
        await ctx.send_help(self.course)

    @course.command(name="enable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def enable(self, ctx: commands.Context) -> None:
        """Enable Course Manager in this server."""
        await self.course_service.enable(ctx)

    @course.command(name="disable")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def disable(self, ctx: commands.Context) -> None:
        """Disable Course Manager in this server."""
        await self.course_service.disable(ctx)

    @course.command(name="join")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def join_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """Join a course channel."""
        await self.course_service.join_course(ctx, course_code)

    @course.command(name="leave")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def leave_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """Leave a course channel."""
        await self.course_service.leave_course(ctx, course_code)

    @course.command(name="refresh")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def refresh_course(self, ctx: commands.Context, *, course_code: str) -> None:
        """Refresh course data."""
        await self.course_service.refresh_course_data(ctx, course_code)

    @course.command(name="list")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def list_enrollments(self, ctx: commands.Context) -> None:
        """List your enrolled courses."""
        await self.course_service.list_enrollments(ctx)

    @course.command(name="details")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def course_details(self, ctx: commands.Context, *, course_code: str) -> None:
        """Get details for a course."""
        embed = await self.course_service.course_details(ctx, course_code)
        if embed is None:
            await ctx.send(error(f"Course not found: {course_code}"))
        else:
            await ctx.send(embed=embed)

    @commands.admin()
    @course.command(name="delete")
    async def admin_delete_channel(
        self, ctx: commands.Context, *, channel: discord.TextChannel
    ) -> None:
        """Admin: Delete a course channel."""
        await self.course_service.admin_delete_channel(ctx, channel)

    @commands.admin()
    @course.command(name="setlogging")
    async def set_logging(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        """Admin: Set the logging channel."""
        await self.course_service.set_logging(ctx, channel)

    @commands.is_owner()
    @commands.group(name="dc", invoke_without_command=True)
    async def dev_course(self, ctx: commands.Context) -> None:
        """Developer commands for course management."""
        await ctx.send_help(self.dev_course)

    @dev_course.command(name="term")
    async def set_term_code(
        self, ctx: commands.Context, term_name: str, term_id: int
    ) -> None:
        """Set term code for a term name."""
        await self.course_service.set_term_code(ctx, term_name, term_id)

    @dev_course.command(name="clearstale")
    async def clear_stale_config(self, ctx: commands.Context) -> None:
        """Clear stale configuration entries."""
        await self.course_service.clear_stale_config(ctx)

    @dev_course.command(name="prune")
    async def manual_prune(self, ctx: commands.Context) -> None:
        """Manually prune inactive course channels."""
        await self.course_service.manual_prune(ctx)

    @dev_course.command(name="clearcourses")
    async def clear_courses(self, ctx: commands.Context) -> None:
        """Clear all course data and listings."""
        await self.course_service.clear_courses(ctx)

    @dev_course.command(name="list")
    async def list_courses(self, ctx: commands.Context) -> None:
        """List all course config keys."""
        await self.course_service.list_courses(ctx)

    @dev_course.command(name="listall")
    async def list_all_courses(self, ctx: commands.Context) -> None:
        """List all courses from listings."""
        await self.course_service.list_all_courses(ctx)

    @dev_course.command(name="populate")
    async def populate_courses(self, ctx: commands.Context) -> None:
        """Populate course listings from external source."""
        await self.course_service.populate_courses(ctx)
