# -*- coding: utf-8 -*-
from .bookmark import Bookmark

async def setup(bot):
    bot.add_cog(Bookmark(bot))