# GAM Clients Python Wrapper #
A simple wrapper for the GAM python API that allows for running reports quick and easy with very little knowledge of the GAM API required.
All reports are returned as pandas dataframes.

## Install ##

Install this package using `pip install --index-url https://monupypi.herokuapp.com/simple/ gamclients`


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

# Run a pre-saved report from GAM (it must be shared with your service user in the UI)
report_df = gam.run_report(REPORT_ID)


# You can also define the report in JSON and run it that way
report_config = {
        'dimensions': [
            'DATE',
            'AD_UNIT_NAME',
            'DEVICE_CATEGORY_NAME',
            'LINE_ITEM_NAME',
            'CUSTOM_CRITERIA'
        ],
        'adUnitView': 'FLAT',
        'columns': [
            'TOTAL_ACTIVE_VIEW_ELIGIBLE_IMPRESSIONS',
            'TOTAL_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS',
            'TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS',
            'AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS',
            'AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE'
        ],
        'dimensionAttributes': [
            'LINE_ITEM_COST_PER_UNIT'
        ],
        'customFieldIds': [],
        'dateRangeType': 'YESTERDAY',
        'statement': {
            'query': ' where parent_ad_unit_id in (123456789)',
            'values': []
        },
        'adxReportCurrency': None,
        'timeZoneType': 'PUBLISHER'
    }
report_df = gam.run_report(report_config)

# Lastly, you can adjust any parameters using the same report json as above, and run that against a saved GAM query
report_df = gam.run_report(REPORT_ID, updated_params=report_config)
```
