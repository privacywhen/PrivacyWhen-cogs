from .course_manager import CourseManager


def setup(bot):
    cog = CourseManager(bot)
    bot.add_cog(cog)
