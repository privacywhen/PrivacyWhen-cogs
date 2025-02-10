from redbot.core import commands
from .commands import CourseChannelCog


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CourseChannelCog(bot))
