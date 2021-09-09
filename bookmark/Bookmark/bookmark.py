# -*- coding: utf-8 -*-
import logging
from collections import namedtuple

import discord
from redbot.core import checks, Config, commands, bot

log = logging.getLogger("red.cbd-cogs.bookmark")

__all__ = ["UNIQUE_ID", "Bio"]

UNIQUE_ID = 0x426f6f6b6d61726b

PlaceMarker = namedtuple("PlaceMarker", "text link")

class Bookmark(commands.Cog):
    """Let users bookmark messages
    
    Add a reaction to bookmark a message and remove the reaction to remove it from your bookmarks"""
    def __init__(self, bot: bot.Red, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot = bot
        self.conf = Config.get_conf(self, identifier=UNIQUE_ID, force_registration=True)
        self.conf.register_user(bookmarks=[])
        self.conf.register_guild(bookmark=":bookmark:")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        """Handle adding bookmarks"""
        await self._handle_reaction(payload, True)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        """Handle removing bookmarks"""
        await self._handle_reaction(payload, False)

    async def _handle_reaction(self, payload: discord.RawReactionActionEvent, add: bool):
        try:
            bookmark = await self.conf.guild(self.bot.get_guild(payload.guild_id)).bookmark()
        except AttributeError:
            # Ignore out-of-guild payloads
            return
        # Ignore reactions that are not bookmarks
        if payload.emoji.name != bookmark:
            return
        user = self.bot.get_user(payload.user_id)
        bookmarks = await self.conf.user(user).bookmarks()
        if add:
            message = await self.bot.get_channel(payload.channel_id).fetch_message(payload.message_id)
            content = message.clean_content[:50]
            if not content:
                try:
                    content = message.attachments[0].filename
                except IndexError:
                    try:
                        content = message.embeds[0].title
                    except IndexError:
                        content = message.system_content[:50]
            bookmarks.append(PlaceMarker(content or "[no content]",
                                         message.jump_url))
        else:
            for i, mark in enumerate(bookmarks):
                # Unpack jump_url from PlaceMarker
                _, link = mark
                if link == f"https://discordapp.com/channels/{payload.guild_id}/{payload.channel_id}/{payload.message_id}":
                    del bookmarks[i]
                    break
        await self.conf.user(user).bookmarks.set(bookmarks)

    @commands.command(name="setbookmarkemoji")
    async def set_bookmark_emoji(self, ctx: commands.Context):
        """Set the emoji to use for bookmarks
        
        The default is :bookmark:"""
        query = await ctx.send("What emoji should be used for bookmarks?")
        def check_reaction_user(reaction: discord.Reaction, user: discord.User):
            return user == ctx.author and reaction.message.id == query.id
        try:
            reaction, user = await ctx.bot.wait_for("reaction_add", check=check_reaction_user, timeout=60)
        except:
            await ctx.send("I'm done waiting. Please try reacting with an emoji next time.")
            return
        await self.conf.guild(ctx.message.guild).bookmark.set(reaction.emoji)
        await ctx.send(f"Bookmark emoji set to {reaction.emoji}")

    @commands.command()
    async def bookmarks(self, ctx: commands.Context):
        """View your bookmarks"""
        bookmarks = await self.conf.user(ctx.message.author).bookmarks()
        try:
            embed_permission = ctx.message.channel.permissions_for(ctx.message.guild.me).embed_links
        except AttributeError:
            # Probably means we're in a DM
            embed_permission = True
        if embed_permission:
            payload = ""
            for preview, link in bookmarks:
                payload += f"[{preview}]({link})\n"
            embed = discord.Embed(title="Bookmarks", description=payload)
            await ctx.send(embed=embed)
        else:
            payload = "**Bookmarks**"
            for preview, link in bookmarks:
                payload += f"\n[{preview}]({link})"
            await ctx.send(payload)