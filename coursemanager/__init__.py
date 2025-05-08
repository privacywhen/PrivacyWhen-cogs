from .course_commands import CourseChannelCog


async def setup(bot) -> None:
    await bot.add_cog(CourseChannelCog(bot))
