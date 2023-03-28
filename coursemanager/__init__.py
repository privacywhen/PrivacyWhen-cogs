from .course_manager import CourseManager

def setup(bot):
    bot.add_cog(CourseManager(bot))