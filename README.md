# GAM Clients Python Wrapper #
A simple wrapper for the GAM python API that allows for running reports quick and easy with very little knowledge of the GAM API required.

## Usage ##
You will need to set up a Service user on your GAM account and export the credentials.
To generate the credentials for your service user, please refer to documentation here: https://developers.google.com/ad-manager/api/start
Once retrieved, you can use part of the private key file to connect your python bot.
This package needs the following fields for your service account:
- private_key
- client_email
- token_url (In almost every case, this will be `https://accounts.google.com/o/oauth2/token`)

### Running Reports ###

Once you have your connection credentials, you can initiate the dfp reports client like this:
```
from gamclients.clients import GAMReports
settings = {"private_key":"YOUR_PRIVATE_KEY",
            "client_email":"YOUR_SERVICE_ACC_EMAIL",
            "token_url":"https://accounts.google.com/o/oauth2/token"}
bot_name = "GAM Reporting Bot"
gam_acc_id = "YOUR_GAM_ID"
gam = GAMReports(settings, gam_acc_id, bot_name)
```