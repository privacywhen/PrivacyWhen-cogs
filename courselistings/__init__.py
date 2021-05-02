from .getcourses import GetCourses

def setup(bot):
    n = GetCourses(bot)
    bot.add_cog(n)

