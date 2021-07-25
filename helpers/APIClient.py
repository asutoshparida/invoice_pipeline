"""
APIClient.py
~~~~~~~~

Module containing helper function for calling REST endpoints
"""
import requests


class APIClient(object):
    """class for making API call

    :param conf: config object.
    :param log: log object.
    """

    def __init__(self, conf, log):
        # get spark app configs
        self.conf = conf
        self.logger = log

    def export_api_data_json(self):
        """
        Make Api GET call to https://personio-old-test.chargebee.com/api/v2/invoices
         & writes data to report_path as JSON
        :return:
        """
        self.logger.info("Exporting API report results to JSON file...")
        response = self.get_api_report()
        report_path = self.conf['report_path']
        self.logger.info("Writing results to :" + report_path)
        text_file = open(report_path, "w", encoding="utf8")
        text_file.write(response)
        text_file.close()

    def get_api_report(self):
        """
        Make Api GET call with BASIC Auth token
        :return:
        """
        api_base_url = self.conf['api_base_url']
        auth_token = self.conf['auth_token']

        self.logger.info("Getting report results for :" + api_base_url)
        header_gs = {'Authorization': auth_token}

        response = requests.get(api_base_url, headers=header_gs)
        if response.ok:
            self.logger.info("Report results received...")
            self.logger.info("HTTP %i - %s" % (response.status_code, response.reason))
            return response.text
        else:
            self.logger.info("HTTP %i - %s" % (response.status_code, response.reason))


