import yaml
from discord.ext import commands, tasks

with open('config.yml', 'r') as file:
    data = yaml.safe_load(file)

embed_color = data["General"]["EMBED_COLOR"]
delay_seconds = data["General"]["DELAY_SECONDS"]

class FormsCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def cog_load(self):
        self.check_stopped_loop.start()

    @tasks.loop(seconds=delay_seconds)
    async def check_stopped_loop(self):
        ...

    @check_stopped_loop.before_loop
    async def check_stopped_loop_before(self):
        await self.bot.wait_until_ready()

async def setup(bot: commands.Bot):
    await bot.add_cog(FormsCog(bot))