from .coursemanager import CourseManager


async def setup(bot):
    await bot.add_cog(CourseManager(bot))
