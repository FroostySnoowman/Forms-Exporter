import discord
import pandas as pd
import aiosqlite
import asyncio
import yaml
import os
from discord.ext import commands, tasks
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def load_config():
    with open('config.yml', "r") as file:
        data = yaml.safe_load(file)
    return data

config = load_config()
embed_color = config["General"]["EMBED_COLOR"]
delay_seconds = config["General"]["DELAY_SECONDS"]

def get_credentials():
    creds_path = os.path.join(config["Google"]["GOOGLE_SERVICE_ACCOUNT_FILE"])
    try:
        credentials = service_account.Credentials.from_service_account_file(
            creds_path, 
            scopes=[
                "https://www.googleapis.com/auth/forms.responses.readonly",
                "https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/drive.readonly"
            ]
        )
        return credentials
    except Exception as e:
        print(f"Error getting credentials: {e}")
        raise

def flatten_response(response):
    row = {}
    row["responseId"] = response.get("responseId", "")
    row["createTime"] = response.get("createTime", "")
    answers = response.get("answers", {})

    for question_id, answer in answers.items():
        text = ""
        if "textAnswers" in answer:
            text_answers = answer["textAnswers"].get("answers", [])
            if text_answers:
                text = text_answers[0].get("value", "")
        else:
            text = str(answer)
        
        row[question_id] = text

    return row

def get_config_mapping():
    mapping = config.get("MappingOverrides", {})
    return mapping

def export_using_forms_api(form_id, credentials):
    try:
        forms_service = build("forms", "v1", credentials=credentials)
        response = forms_service.forms().responses().list(formId=form_id).execute()
        responses = response.get("responses", [])
        if not responses:
            return None, {}
        
        rows = [flatten_response(resp) for resp in responses]

        df = pd.DataFrame(rows).fillna("")

        mapping = get_config_mapping()

        return df, mapping
    except HttpError as err:
        print(f"Forms API error: {err}")
        return None, {}

def export_using_sheet_api(linked_sheet_id, credentials):
    try:
        sheets_service = build("sheets", "v4", credentials=credentials)
        metadata = sheets_service.spreadsheets().get(spreadsheetId=linked_sheet_id).execute()
        first_sheet = metadata['sheets'][0]['properties']['title']
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=linked_sheet_id,
            range=first_sheet
        ).execute()
    
        values = result.get('values', [])
    
        if not values:
            return None
    
        df = pd.DataFrame(values)
        df = df.reset_index(drop=True)
    
        if not df.empty:
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
        
        return df
    except HttpError as err:
        print(f"Sheets API error: {err}")
        return None

def get_linked_sheet_id(form_id, credentials):
    try:
        forms_service = build("forms", "v1", credentials=credentials)
        form_metadata = forms_service.forms().get(formId=form_id).execute()
        destination = form_metadata.get("responseDestination", {})
        
        if destination.get("destinationType") == "SPREADSHEET":
            linked_sheet = destination.get("spreadsheet")
            if linked_sheet:
                return linked_sheet
        return None
    except HttpError as err:
        print(f"Error retrieving form metadata: {err}")
        return None

async def export_form(form_id):
    credentials = await asyncio.to_thread(get_credentials)

    result = await asyncio.to_thread(export_using_forms_api, form_id, credentials)

    df, mapping = result if result is not None else (None, {})

    if df is None:
        linked_sheet_id = await asyncio.to_thread(get_linked_sheet_id, form_id, credentials)
        if linked_sheet_id:
            df = await asyncio.to_thread(export_using_sheet_api, linked_sheet_id, credentials)
            mapping = {}
        else:
            return None
    
    if df is None or df.empty:
        return None
    
    if mapping:
        new_columns = {}
        
        for col in df.columns:
            if col not in ["responseId", "createTime"] and col in mapping:
                new_columns[col] = mapping[col]
        
        if new_columns:
            df = df.rename(columns=new_columns)

    msg_lines = []

    for idx, row in df.iterrows():
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute('SELECT * FROM forms WHERE response_id = ?', (row["responseId"],))
            existing_row = await cursor.fetchone()

            if existing_row:
                continue

            if "createTime" in row:
                dt = datetime.strptime(row['createTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
                unix_ts = int(dt.timestamp())

                msg_lines.append(f"Create Time: <t:{unix_ts}:f>\n")
            
            for col in df.columns:
                if col not in ["responseId", "createTime"]:
                    msg_lines.append(f"**{col}**: {row[col]}")
            
            msg_lines.append("")

            await db.execute('INSERT INTO forms VALUES (?);', (row["responseId"],))
            await db.commit()

    message_text = "\n".join(msg_lines)

    return message_text

class FormsCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def cog_load(self):
        self.check_stopped_loop.start()

    @tasks.loop(seconds=delay_seconds)
    async def check_stopped_loop(self):
        for form_config in config["Forms"]:

            response_text = await export_form(form_config["GOOGLE_FORM_ID"])
            if response_text is None:
                response_text = "No responses found."

            guild = self.bot.get_guild(config["General"]["GUILD_ID"])
            
            channel = self.bot.get_channel(config["General"]["CHANNEL_ID"])
            if channel:
                if response_text != "":
                    embed = discord.Embed(title="New Response", description=response_text, color=discord.Color.from_str(embed_color))
                    embed.timestamp = datetime.now()

                    embed.set_author(name=self.bot.user.name, icon_url=self.bot.user.display_avatar.url)

                    if guild:
                        embed.set_thumbnail(url=guild.icon.url if guild.icon else None)

                    await channel.send(embed=embed)

    @check_stopped_loop.before_loop
    async def check_stopped_loop_before(self):
        await self.bot.wait_until_ready()

async def setup(bot: commands.Bot):
    await bot.add_cog(FormsCog(bot))