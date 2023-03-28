from .course_manager import coursemanager

def setup(bot):
    bot.add_cog(coursemanager(bot))