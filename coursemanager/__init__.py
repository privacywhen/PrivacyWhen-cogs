from .course_commands import CourseChannelCog


async def setup(bot: course_commands.Bot) -> None:
    await bot.add_cog(CourseChannelCog(bot))
