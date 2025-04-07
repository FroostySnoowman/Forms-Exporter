import asyncio
import pathlib
import yaml
import os
from gsheets import Sheets
from flask import Flask, send_file
from threading import Thread
from google.oauth2 import service_account

app = Flask(__name__)

def load_config():
    config_path = f'{pathlib.Path(__file__).parent.absolute()}/config.yml'
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as file:
        data = yaml.safe_load(file)
    
    return data

config = load_config()

def get_credentials():
    creds_path = os.path.join(pathlib.Path(__file__).parent.absolute(), config["Google"]["GOOGLE_SERVICE_ACCOUNT_FILE"])
    
    credentials = service_account.Credentials.from_service_account_file(
        creds_path, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly', 'https://www.googleapis.com/auth/drive.readonly'])
    
    return credentials

async def export_spreadsheet(spreadsheet_id, export_mode, column_name, txt_file_name, export_format, file_name):
    print(f"Exporting spreadsheet {spreadsheet_id}...")
    
    credentials = get_credentials()
    
    sheets = Sheets(credentials)
    
    spreadsheet = sheets.get(spreadsheet_id)
    sheet = spreadsheet.sheets[0]

    df = sheet.to_frame(header=None).reset_index(drop=True)
    df.columns = [str(i+1) for i in range(df.shape[1])]

    file_path = None

    if export_mode == "single_column":
        file_path = f'{pathlib.Path(__file__).parent.absolute()}/{txt_file_name}'

        if column_name in df.columns:
            df[column_name].astype(str).to_csv(file_path, index=False, header=False)
            print(f"Exported single column '{column_name}' to {file_path}")
        else:
            print(f"Error: Column '{column_name}' not found in spreadsheet.")
            return None

    elif export_mode == "full_spreadsheet":
        file_path = f'{pathlib.Path(__file__).parent.absolute()}/{file_name}'

        if export_format == "csv":
            df.to_csv(file_path, sep='\t', index=False)
            print(f"Exported full spreadsheet to {file_path} as CSV.")
        elif export_format == "xlsx":
            df.to_excel(file_path, index=False)
            print(f"Exported full spreadsheet to {file_path} as XLSX.")
        else:
            print(f"Error: Unsupported export format '{export_format}'.")
            return None

    if not os.path.exists(file_path):
        print(f"File was not created: {file_path}")
    return file_path

async def initial_export():
    for spreadsheet_config in config["Spreadsheets"]:
        file_path = await export_spreadsheet(
            spreadsheet_config["GOOGLE_SPREADSHEET_ID"],
            spreadsheet_config["ExportMode"],
            spreadsheet_config.get("ColumnName", ""),
            spreadsheet_config["TXT_FILE_NAME"],
            spreadsheet_config.get("ExportFormat", "csv"),
            spreadsheet_config["FILE_NAME"]
        )
        if file_path and os.path.exists(file_path):
            print(f"Initial file ready: {file_path}")
        else:
            print("Initial export failed.")

async def run_every_hour():
    while True:
        try:
            print("Running hourly export...")
            for spreadsheet_config in config["Spreadsheets"]:
                print(f"Exporting {spreadsheet_config['GOOGLE_SPREADSHEET_ID']}")
                await export_spreadsheet(
                    spreadsheet_config["GOOGLE_SPREADSHEET_ID"],
                    spreadsheet_config["ExportMode"],
                    spreadsheet_config.get("ColumnName", ""),
                    spreadsheet_config["TXT_FILE_NAME"],
                    spreadsheet_config.get("ExportFormat", "csv"),
                    spreadsheet_config["FILE_NAME"]
                )
            print("Sleeping for 1 hour...")
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"Error in scheduled task: {e}")

def start_asyncio_loop():
    asyncio.run(run_every_hour())

thread = Thread(target=start_asyncio_loop, daemon=True)
thread.start()

@app.route('/download/<filename>', methods=['GET'])
def download_latest_file(filename):
    file_path = f'{pathlib.Path(__file__).parent.absolute()}/{filename}'

    print(f"Attempting to serve file: {file_path}")

    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        return "No exported file available yet.", 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=config["General"]["WEBSERVER_PORT"], threaded=True)