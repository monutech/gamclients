"""
Quick Wrapper around the GAM API to allow for pulling reports by either ID
    or by manual setup. Results can be returned as temporary files, or as
    pandas dataframes.

Raises:
    Exception -- Raised exception when invalid login credentials are used

"""


from googleads import ad_manager
from googleads import oauth2
from googleads import errors
from datetime import datetime, timedelta
import tempfile
import base64
import json
from oauth2client.client import (AccessTokenRefreshError)
import binascii
import gzip
import pandas as pd
import numpy as np
from tqdm import tnrange, tqdm, tqdm_notebook
from IPython.display import display, clear_output

default_api_version = "v202008"


class GAMConnection():
    """Base Connection class for the GAM API Wrapper. Not super useful on its
    own.

    Raises:
        Exception: Login error- Incorrect login details, or GAM could not be
        reached

    Returns:
        GAMConnection -- Class to help interface with GAM
    """

    def __init__(self, login_dict, gam_account_num,
                 bot_name='GAM Bot', api_version=default_api_version):
        self.name = bot_name
        self.api_version = api_version
        self.login = login_dict
        self.network = gam_account_num
        self.connection = self.connect(self.login, self.network)
        if not self.connection:
            raise Exception("Invalid Login")

    def _create_tmp_file(
            self,
            content,
            mode='w+b',
            suffix=".json",
            return_path=False):
        """
        Given report text, create a tmp file and return it
        """
        content_file = tempfile.NamedTemporaryFile(
            suffix=suffix,
            delete=False,
            mode=mode)
        content_file.write(content)
        content_file.flush()
        return content_file.name if return_path else content_file

    def connect(self, key_dict, network_code):
        """Takes a dictionary containing GAM login information,
            along with a Network code and returns a GAM connection object

        Arguments:
            key_dict {dict} -- Login dict.
                            private_key,
                            client_email,
                            and token_uri are required
            network_code {str} -- GAM network to connect to

        Returns:
            AdmanagerClient -- GAM connection client
        """

        key_file = self._create_tmp_file(
            json.dumps(key_dict).encode('utf-8'),
            return_path=True)
        try:
            oauth2_client = oauth2.GoogleServiceAccountClient(
                key_file, scope=oauth2.GetAPIScope('ad_manager'))
            return ad_manager.AdManagerClient(
                oauth2_client,
                self.name, network_code=network_code)
        except (AccessTokenRefreshError, binascii.Error, ) as exc:
            print(f"Connection error, {exc}")
            return False


class GAMSystem(GAMConnection):
    """Class used to interface with generic System items within GAM. Currently
    only suppports interacting with Key Values
    """

    def __init__(self, login_dict, gam_account_num,
                 bot_name='GAM Bot', api_version=default_api_version):
        super().__init__(login_dict, gam_account_num,
                         bot_name=bot_name, api_version=api_version)
        self.custom_targeting_service = self.connection.GetService(
            "CustomTargetingService", version=self.api_version)

    def create_key(self, name):
        return self.custom_targeting_service.createCustomTargetingKeys(
            {"name": name, "type": "FREEFORM"}
        )

    def _get_key_by_name(self, key_name):
        """Retrieves key details from GAM based on the name of the key

        Arguments:
            key_name {str} -- Name of the key as it appears in GAM

        Returns:
            CustomTargetingKey -- Dict like object that contains key details
        """
        statement = (
            ad_manager.StatementBuilder(
                version=self.api_version).Where(f"name = '{key_name}'"))
        resp = self.custom_targeting_service.getCustomTargetingKeysByStatement(
            statement.ToStatement())

        return next(iter(resp['results'] or []), None)

    def get_current_values(self, key_id, return_column='name',
                           print_status_bar=False):
        """Returns a list of values currently in GAM for given key ID.
        ID can be retrieved using _get_key_by_name

        Arguments:
            key_id {int} -- GAM key ID

        Returns:
            [list] -- values assigned to key
        """
        if isinstance(key_id, str):
            key = self._get_key_by_name(key_id)
            if key:
                key_id = key["id"]
            else:
                key_id = None
        offset = 0
        received = 0
        total = 1
        results = []
        if print_status_bar:
            pbar = tqdm(total=100)
        lastReportedPercent = 0
        while received < total:
            statement = (
                ad_manager.StatementBuilder(
                    version=self.api_version).Where(
                        f"customTargetingKeyId = '{key_id}'"))
            statement.offset = offset
            resp = \
                self.custom_targeting_service \
                .getCustomTargetingValuesByStatement(
                    statement.ToStatement())
            total = resp["totalResultSetSize"]
            results.extend([value[return_column] for value in resp['results']])
            received = len(results)
            offset += 500
            if print_status_bar:
                reportedPercent = (len(results) / total) * 100
                pbar.update(reportedPercent - lastReportedPercent)
                lastReportedPercent = reportedPercent
        if print_status_bar:
            pbar.close()
        return results

    def _get_new_values(self, new_list, existing_list):
        """Returns all values in new_list that are not in existing_list

        Arguments:
            new_list {list} -- first list
            existing_list {list} -- second list

        Returns:
            list -- list of all objects in new_list that are not in
            existing_list
        """
        return np.setdiff1d(new_list, existing_list)

    def chunk_list_data(self, list_data, n):
        """Breaks a list into n sized chunks

        Arguments:
            list_data {list} -- list of objects to be broken up
            n {int} -- size of sub lists to be returned

        Returns:
            list -- list of lists with each sub list being n or smaller in
            length
        """
        return [list_data[i * n:(i + 1) * n]
                for i in range((len(list_data) + n - 1) // n)]

    def upload_new_values(
            self,
            key,
            values,
            create_key=False,
            print_status_bar=False,
            chunk_upload_size=200):
        """Uploads a list of values to the specified key in GAM.

        Arguments:
            key {str} -- Key name in GAM
            values {list or dataframe or dataframe series} -- values to be
            assigned to the specified key

        Keyword Arguments:
            print_status_bar {bool} -- print log during execution?
             (default: {False})

        Returns:
            bool -- always true.
        """
        key_details = self._get_key_by_name(key)
        if not key_details:
            if create_key:
                key_details = self.create_key(key)
            else:
                if print_status_bar:
                    print("Key does not exist!")
                return None

        current_values = self.get_current_values(
            key_details["id"], print_status_bar=print_status_bar)

        if isinstance(values, pd.core.series.Series):
            values = values.values.tolist()
        elif isinstance(values, pd.core.frame.DataFrame):
            values = self.import_values_from_df(values)
        values = [str(i) for i in values]
        new_values_to_upload = self._get_new_values(values, current_values)
        prep_values = [{"customTargetingKeyId": key_details["id"],
                        "name": value} for value in new_values_to_upload]
        new_values_chunked = self.chunk_list_data(
            prep_values, chunk_upload_size)
        total_requests = len(new_values_chunked)
        for id, value_chunk in tqdm_notebook(
                enumerate(new_values_chunked), desc='upload'):
            try:
                tqdm.write(f"now uploading {value_chunk}")
                upload = self.custom_targeting_service \
                    .createCustomTargetingValues(value_chunk)
            except Exception as e:
                if chunk_upload_size == 1:
                    tqdm.write(
                        f"Skipping {value_chunk} as it is already in GAM")
                    continue
                raise e
            if print_status_bar:
                tqdm.write(
                    f"{round((id/float(total_requests))*100,2)}% - "
                    f"{id}/{total_requests} complete", end='\r')
        if print_status_bar:
            tqdm.write(f"Uploaded {len(prep_values)} new values for {key}")
        return True

    def deactivate_values(
            self,
            key,
            values,
            print_status_bar=False):
        """Uploads a list of values to the specified key in GAM.

        Arguments:
            key {str} -- Key name in GAM
            values {list or dataframe or dataframe series} -- values to be
            assigned to the specified key

        Keyword Arguments:
            print_status_bar {bool} -- print log during execution?
            (default: {False})

        Returns:
            bool -- always true.
        """
        key_details = self._get_key_by_name(key)
        if not key_details:
            if print_status_bar:
                print("Key does not exist!")
            return None

        if isinstance(values, pd.core.series.Series):
            values = values.values.tolist()
        elif isinstance(values, pd.core.frame.DataFrame):
            values = self.import_values_from_df(values)
        key_id = key_details['id']
        offset = 0
        received = 0
        total = 1
        results = []
        while received < total:
            statement = (ad_manager.StatementBuilder(version=self.api_version)
                         .Where(
                f"name IN {tuple(values)} AND "
                f"customTargetingKeyId = '{key_id}'"))
            statement.offset = offset
            resp = self.custom_targeting_service \
                .getCustomTargetingValuesByStatement(statement.ToStatement())
            total = resp["totalResultSetSize"]
            results.extend([value['name'] for value in resp['results']])
            received = len(results)

            value_ids = [value['id'] for value in resp['results']]
            action = {'xsi_type': 'DeleteCustomTargetingValues'}
            value_statement = (ad_manager.StatementBuilder(
                                version=self.api_version)
                               .Where('customTargetingKeyId = :keyId '
                                      f'AND id IN {tuple(value_ids)}')
                               .WithBindVariable('keyId', key_id))

            result = self.custom_targeting_service \
                .performCustomTargetingValueAction(
                    action, value_statement.ToStatement())

        return True

    def import_values_from_csv(
            self,
            path_to_file,
            column_with_keys=0,
            contains_headers=False,
            only_uniques=True):
        """Imports a list of values from a provided csv file

        Arguments:
            path_to_file {str} -- path to csv file.

        Keyword Arguments:
            column_with_keys {int} -- column to extract values from
            (default: {0})
            contains_headers {bool} -- Is there a header on this file?
            (default: {False})
            only_uniques {bool} -- Extract all values, or only uniques?
             (default: {True})

        Returns:
            list -- list of values
        """
        values = []
        with open(path_to_file) as f:
            values = [x.split()[column_with_keys] for x in f]
        if only_uniques:
            values = list(dict.fromkeys(values))
        if contains_headers:
            values = values[1:]
        return values

    def import_values_from_df(self, df, column_name=None, only_uniques=True):
        """Imports a list of values from a provided pandas dataframe

        Arguments:
            df {DataFrame} -- DF containing data.
        Keyword Arguments:
            column_name {str} -- Name of column to pull. First column is
                                    defaulted if None (default: {None})
            only_uniques {bool} -- Pulls only uniques if True.
                                    (default: {True})

        Returns:
            list -- List of values
        """
        if not column_name:
            column_name = df.columns[0]
        values = df[column_name].values.tolist()
        if only_uniques:
            values = list(dict.fromkeys(values))
        return values


class GAMReports(GAMConnection):
    name = 'monu-reports-scraper'

    def _convert_tmp_report_to_df(self, report_file, remove_column_types=True):
        """Takes a report file and returns the resulting dataframe

        Arguments:
            report_file {[type]} -- Temp file to be converted to dataframe

        Keyword Arguments:
            remove_column_types {bool} --
                                    Remove column type prefix on column names?
                                        (i.e. Dimension.XXXX or Column.YYYY)
                                        (default: {True})

        Returns:
            DataFrame -- Dataframe holding the contents of the file.
        """
        try:
            df = pd.read_csv(report_file.name)
        except pd.errors.EmptyDataError:
            print("Report is Empty!")
            return False
        if remove_column_types:
            df.columns = [col_name.split(".")[1] for col_name in df.columns]
        return df

    def get_report(
            self,
            query_items,
            filter_pql=None):
        """Takes a connection, and query items and returns the resulting report

        Arguments:
            connection {AdManagerClient} -- GAM connection client
            query_items {dict} -- Dict containing report data

        Keyword Arguments:
            filter_pql {dict} -- query string and values
                                following format {'query':"",'values':[]}
                                (default: {None})

        Returns:
            tuple -- report temp file, report id
        """

        gam_downloader = self.connection.GetDataDownloader(
            version=self.api_version)
        # queue report
        gam_query = query_items
        if filter_pql:
            gam_query["statement"] = filter_pql
        report_job = {'reportQuery': gam_query}
        try:
            report_job_id = gam_downloader.WaitForReport(report_job)
        except errors.AdManagerReportError as e:
            print(f'Failed to generate report. Error was: {e}')
            return False, 0
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz',)
        gam_downloader.DownloadReportToFile(
            report_job_id, 'CSV_DUMP', report_file)
        report_file.flush()
        response_content = bytes(
            gzip.open(
                report_file.name,
                'rt').read(),
            'UTF-8')
        report = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        report.write(response_content)
        report_file.flush()
        return report, report_job_id

    def get_saved_report_params(self, report_id):
        """Takes a GAM connection and report id and returns the query params

        Arguments:
            connection {AdManagerClient} -- Ad Manager connection
            report_id {int} -- report ID to grab

        Returns:
            ReportQuery -- params of the report
        """

        gam_reports = self.connection.GetService(
            'ReportService', version=self.api_version)
        statement = (ad_manager.StatementBuilder(version=self.api_version)
                     .Where('id = :id')
                     .WithBindVariable('id', int(report_id))
                     .Limit(1))
        response = gam_reports.getSavedQueriesByStatement(
            statement.ToStatement())
        if 'results' in response and len(response['results']):
            saved_query = response['results'][0]
            return saved_query
        else:
            print(f"Unable to find report {report_id}")
            return False

    def get_saved_report(
            self,
            report_id,
            updated_params=None,
            filter_pql=None):
        """Takes a connection and query id and returns the resulting report

        Arguments:
            connection {AdManagerClient} -- GAM connection client
            report_id {int} -- query id of query created in GAM

        Keyword Arguments:
            updated_params {dict} --dict of params to change, with their values
                                    (default: {None})
            filter_pql {dict} -- query string and values
                                following format {'query':"",'values':[]}
                                (default: {None})

        Returns:
            tuple -- report temp file, report id
        """

        gam_reports = self.connection.GetService(
            'ReportService', version=self.api_version)
        # Create statement object to filter for an order.
        statement = (ad_manager.StatementBuilder(version=self.api_version)
                     .Where('id = :id')
                     .WithBindVariable('id', int(report_id))
                     .Limit(1))

        response = gam_reports.getSavedQueriesByStatement(
            statement.ToStatement())

        if 'results' in response and len(response['results']):
            saved_query = response['results'][0]

            if saved_query['isCompatibleWithApiVersion']:
                # Set report query and modify it with given params.
                report_query = saved_query['reportQuery']
                if updated_params:
                    for new_param in updated_params:
                        report_query[new_param] = updated_params[new_param]
                return self.get_report(report_query, filter_pql)
            else:
                print("Report unable to run with current API Version:" +
                      f"{self.api_version}")
                return False, 0
        else:
            print(
                f"Unable to find report with id: {report_id}. " +
                "Please verify the report is shared with " +
                f"{self.connection.oauth2_client.creds.signer_email} " +
                f"in account {self.connection.network_code}")
            return False, 0

    def run_report(
            self,
            id_or_params,
            updated_params=None,
            filter_pql=None):
        """Takes a connection, query id or params, optional params, and
            optional filter criteria, and returns a dataframe containing the
            results of the report

        Arguments:
            connection {AdManagerClient} -- Ad Manager Connection
            id_or_params {int or dict} -- query id or manual query object

        Keyword Arguments:
            updated_params {dict} -- custom params to overwrite the default
                                    (default: {None})
            filter_pql {dict} -- query string and values
                                following format {'query':"",'values':[]}
                                (default: {None})
        """
        report = None
        report_id = 0
        if isinstance(id_or_params, int) or isinstance(id_or_params, str):
            report, report_id = self.get_saved_report(
                id_or_params, updated_params, filter_pql)
        elif isinstance(id_or_params, dict):
            if updated_params:
                id_or_params = {**id_or_params, **(updated_params or {})}
            report, report_id = self.get_report(id_or_params, filter_pql)
        if not report_id:
            return False
        return self._convert_tmp_report_to_df(report)
