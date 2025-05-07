from .course_commands import CourseChannelCog  # noqa: D104


async def setup(bot) -> None:  # noqa: ANN001
    await bot.add_cog(CourseChannelCog(bot))
