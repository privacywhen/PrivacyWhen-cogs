from .course_commands import ChannelCog


async def setup(bot):
    await bot.add_cog(ChannelCog(bot))
