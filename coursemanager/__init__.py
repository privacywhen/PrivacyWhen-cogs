from .course_commands import CourseChannelCog


async def setup(bot):
    await bot.add_cog(CourseChannelCog(bot))
