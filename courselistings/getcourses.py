import discord
from .course_finder import Course
from redbot.core import commands

course_finder = Course()

class GetCourses(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command()
    async def course(self, ctx, *args):
        if not args:
            await ctx.send("Available commands: COURSE SHOW, COURSE SEARCH, COURSE REQS")
            return

        subcommand = args[0].upper().replace("'", '')
        if subcommand == "SHOW":
            await self.show(ctx, args[1], args[2])
        elif subcommand == "SEARCH":
            await self.search(ctx, args[1:])
        elif subcommand == "REQS":
            await self.reqs(ctx, args[1], args[2])
        else:
            await ctx.send(f"Unknown command: {args[0]}")

    async def show(self, ctx, dept, code):
        dept, code = dept.upper().replace("'", ''), code.upper().replace("'", '')
        course_data = course_finder.find_course(dept, code)
        if course_data == "Error":
            await ctx.send(f"Failed to retrieve course data for {str(dept + ' ' + code).upper()}")
            return

        embed = discord.Embed(title=course_data[0], color=discord.Color.blue(), description=f"{course_data[2]}\n\n{course_data[1]} {course_data[3]}")
        if course_data[4]:
            embed.add_field(name="Other Info", value=course_data[4])
        await ctx.send(embed=embed)

    async def search(self, ctx, query):
        course_list = course_finder.search_for_course(' '.join(query))
        courses = '\n'.join(course_list)
        if len(courses) > 1950:
            await ctx.send(f"Please provide a more specific query. `{' '.join(query)}` provided too many results to display.")
            return
        if not courses:
            await ctx.send(f"`{' '.join(query)}` returned no results. Please ensure you are typing full words that appear in the course name (eg. \"discrete mathematics\" vs. \"discrete math\")")
            return

        embed = discord.Embed(title=f"Courses Containing Keyword(s) `{' '.join(query)}`", color=discord.Color.blue(), description=courses)
        await ctx.send(embed=embed)

    async def reqs(self, ctx, dept, code):
        dept, code = dept.upper().replace("'", ''), code.upper().replace("'", '')
        course_data = course_finder.find_course(dept, code)
        if course_data == "Error":
            await ctx.send(f"Failed to retrieve prerequisite data for {dept} {code}")
        else:
            embed = discord.Embed(title=f"Requisites/Info for {course_data[0]}", color=discord.Color.blue(), description=course_data[4] if course_data[4] else "Not Available")
        await ctx.send(embed=embed)
