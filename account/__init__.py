from .account import Account

async def setup(bot):
    await bot.add_cog(Account(bot))
