from .course_manager import CourseManager


async def setup(bot):
    await bot.add_cog(CourseManager(bot))
