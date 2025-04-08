# Forms Exporter

## General Setup
First, install Python 3.X and the required libraries through `pip install -r requirements.txt`.

## Config
The config is designed to be as user friendly as possible, allowing for everything to be configurable.

First, rename the `example_config.yml` file to `config.yml`.

```yml
Google:
    GOOGLE_SERVICE_ACCOUNT_FILE: ""
```
0. To retrieve a service account file (authorization file), simply head to https://console.cloud.google.com/welcome?organizationId=0
1. Select "Select a project" towards the top left of your screen and click "New Project".
2. Give the project any name you want and no location.
3. Wait until it's finished creating, then "Select Project".
4. Go back to the "Welcome" (click the Google Cloud in top left corner) screen and click "APIs & Services*.
5. Near the top middle of your screen, choose "Enable APIs and Services".
6. Search/select the "Google Drive API" and "Google Forms API".
7. Head back to your project's home page (click the Google Cloud in top left corner).
8. Select "IAM & Admin".
9. Choose "Service Accounts" from the left navigation bar.
10. Click "Create Service Account" near the top middle of your screen.
11. Give your Service Account any name you want and the service account ID any name you want (I recommend just leaving it alone if your service is a unique name).
12. Copy the service account ID email addres (mine for example is formsexporter@formsexporter.iam.gserviceaccount.com)
13. Click "Create and Continue".
14. Go to your google sheet at https://forms.google.com/ and "Share" the sheet with that service account ID email address.
15. Now, go back to the "Service Accounts" page and click the 3 dots "Actions" button.
16. Choose "Manage keys".
17. Click "Add Key" and "Create New Key".
18. Make sure JSON is selected and "Create". This will download a necessary file.
19. Upload that file to the bot and name it `service_account.json` (or whatever you put in your config.yml as `GOOGLE_SERVICE_ACCOUNT_FILE`).

To get the correct mapping overrides, you will need to run the `export.py` script before the `main.py` bot to correctly export the data to a csv file and to get the correct ids associated with each question.