import asyncio
import pathlib
import yaml
import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def load_config():
    config_path = f"{pathlib.Path(__file__).parent.absolute()}/config.yml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")    
    with open(config_path, "r") as file:
        data = yaml.safe_load(file)
    return data

config = load_config()

def get_credentials():
    creds_path = os.path.join(
        pathlib.Path(__file__).parent.absolute(),
        config["Google"]["GOOGLE_SERVICE_ACCOUNT_FILE"]
    )
    credentials = service_account.Credentials.from_service_account_file(
        creds_path, 
        scopes=[
            "https://www.googleapis.com/auth/forms.responses.readonly",
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly"
        ]
    )
    return credentials

def flatten_response(response):
    """
    Converts a single form response into a flat dictionary.
    It includes the responseId and createTime plus each answer.
    For an answer that contains textAnswers, extract the first value.
    """
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
            # Fallback: store the whole answer as a string.
            text = str(answer)
        row[question_id] = text
    return row

def get_config_mapping():
    """
    Retrieves the question mapping from the configuration overrides.
    Keys should match the question IDs found in the responses.
    """
    mapping = config.get("MappingOverrides", {})
    if mapping:
        print("Retrieved override mapping from config:")
        for k, v in mapping.items():
            print(f"  {k} => {v}")
    else:
        print("No override mapping provided in config.")
    return mapping

def export_using_forms_api(form_id, credentials):
    """Attempt to get responses directly via the Forms API."""
    try:
        forms_service = build("forms", "v1", credentials=credentials)
        response = forms_service.forms().responses().list(formId=form_id).execute()
        responses = response.get("responses", [])
        if not responses:
            print("No responses found via Forms API.")
            return None, {}
        rows = [flatten_response(resp) for resp in responses]
        df = pd.DataFrame(rows).fillna("")
        # Instead of retrieving a mapping from metadata, we use our config
        mapping = get_config_mapping()
        return df, mapping
    except HttpError as err:
        print(f"Forms API error: {err}")
        return None, {}

def export_using_sheet_api(linked_sheet_id, credentials):
    """Retrieve responses from the linked response spreadsheet using the Sheets API."""
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
            print("No data found in the linked sheet.")
            return None
        df = pd.DataFrame(values)
        df = df.reset_index(drop=True)
        # In the Sheets fallback, assume the first row is a header.
        if not df.empty:
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
        return df
    except HttpError as err:
        print(f"Sheets API error: {err}")
        return None

def get_linked_sheet_id(form_id, credentials):
    """
    If the Forms API response retrieval fails, try to get the linked sheet ID.
    The form metadata may include a field 'responseDestination' that looks like:
      {
         "destinationType": "SPREADSHEET",
         "spreadsheet": "SPREADSHEET_ID"
      }
    """
    try:
        forms_service = build("forms", "v1", credentials=credentials)
        form_metadata = forms_service.forms().get(formId=form_id).execute()
        destination = form_metadata.get("responseDestination", {})
        if destination.get("destinationType") == "SPREADSHEET":
            linked_sheet = destination.get("spreadsheet")
            if linked_sheet:
                print(f"Found linked sheet ID: {linked_sheet}")
                return linked_sheet
        print("No linked sheet found in form metadata.")
        return None
    except HttpError as err:
        print(f"Error retrieving form metadata: {err}")
        return None

async def export_form(form_id, export_format, file_name):
    print(f"Exporting form {form_id}...")
    credentials = get_credentials()

    # First, attempt to get responses via the Forms API.
    df, mapping = export_using_forms_api(form_id, credentials)

    # If direct retrieval fails, try retrieving the linked sheet.
    if df is None:
        linked_sheet_id = get_linked_sheet_id(form_id, credentials)
        if linked_sheet_id:
            df = export_using_sheet_api(linked_sheet_id, credentials)
            mapping = {}  # Cannot apply mapping if using fallback.
        else:
            print("Unable to retrieve responses by any method.")
            return None

    if df is None:
        print("No data to export.")
        return None

    # Diagnostic: show available columns and mapping keys.
    print("Response columns:", df.columns.tolist())
    print("Mapping keys:", list(mapping.keys()))
    
    # Rename columns using the override mapping (for columns that are question IDs).
    if mapping:
        new_columns = {}
        for col in df.columns:
            if col not in ["responseId", "createTime"] and col in mapping:
                new_columns[col] = mapping[col]
        if new_columns:
            df = df.rename(columns=new_columns)
            print("Renamed columns:", new_columns)
        else:
            print("No matching columns found between responses and mapping.")
    
    file_path = f"{pathlib.Path(__file__).parent.absolute()}/{file_name}"

    if export_format == "csv":
        df.to_csv(file_path, sep='\t', index=False)
        print(f"Exported full form responses to {file_path} as CSV.")
    elif export_format == "xlsx":
        df.to_excel(file_path, index=False)
        print(f"Exported full form responses to {file_path} as XLSX.")
    else:
        print(f"Error: Unsupported export format '{export_format}'.")
        return None

    if not os.path.exists(file_path):
        print(f"File was not created: {file_path}")
    return file_path

async def initial_export():
    for form_config in config["Forms"]:
        file_path = await export_form(
            form_config["GOOGLE_FORM_ID"],
            form_config.get("ExportFormat", "csv"),
            form_config["FILE_NAME"]
        )
        if file_path and os.path.exists(file_path):
            print(f"Initial file ready: {file_path}")
        else:
            print("Initial export failed.")

async def run_every_hour():
    while True:
        try:
            print("Running hourly export...")
            for form_config in config["Forms"]:
                print(f"Exporting form {form_config['GOOGLE_FORM_ID']}")
                await export_form(
                    form_config["GOOGLE_FORM_ID"],
                    form_config.get("ExportFormat", "csv"),
                    form_config["FILE_NAME"]
                )
            print("Sleeping for 1 hour...")
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"Error in scheduled task: {e}")

async def main():
    # Run an initial export then continue with hourly exports.
    await initial_export()
    await run_every_hour()

if __name__ == '__main__':
    asyncio.run(main())