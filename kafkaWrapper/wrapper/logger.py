import logging as log
from time import gmtime, strftime
import sys
import traceback
import json
from pathlib import Path
import requests
import os
import time
import base64

log_json_out = None

## Setting logger
s_logging = log.getLogger("Kafka-Service.log")
formatter = log.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch = log.StreamHandler(sys.stdout)
# ch.setLevel(log.DEBUG)
# ch.setFormatter(formatter)
# s_logging.addHandler(ch)

logs = {"info" : list(), "error": list(), "data": dict()}
inlinelogs = {}
logger_set = False


class logging:
    """
    Logging class with static functions used for custom logging.
    """
    @staticmethod
    def isLogger():
        global logger_set
        return logger_set

    @staticmethod
    def set_log_file(filename_prefix, file_path=None ):
        """
        Set file logger with filename_prefix, so log file will be in "logs/{filename_prifix}{program_filename}_{data and time}.log
        :param filename_prefix:
        :return:
        """
        global formatter
        global s_logging
        global logger_set

        log_file = Path(sys.argv[0]).name
        log_file = str(filename_prefix) + log_file
        if file_path == None:
            log_folder = Path("logs")
        else:
            log_folder = Path(file_path)
        if not log_folder.exists():
            log_folder.mkdir(parents= True)
        fileHandler = log.FileHandler( "{0}/{1}_{2}.log".format( log_folder, log_file , strftime("%Y-%m-%d_%H-%M-%S", gmtime()) ))
        fileHandler.setLevel(log.DEBUG)
        fileHandler.setFormatter(formatter)
        s_logging.addHandler(fileHandler)
        s_logging.setLevel(10)
        logger_set = True

    @staticmethod
    def info(msg):
        global logs
        global s_logging
        global logger_set

        if logger_set:
            s_logging.info(msg)
        else:
            print("%s INFO: %s" % (time.strftime("%c"), msg ))
        if type(msg) is list:
            logs['info'] += msg
        else:
            logs['info'].append(msg)

    @staticmethod
    def debug(msg):
        global logs
        global s_logging
        global logger_set

        if logger_set:
            s_logging.info(msg)
        else:
            print("%s DEBUG: %s" % (time.strftime("%c"), msg))
        if type(msg) is list:
            logs['info'] += msg
        else:
            logs['info'].append(msg)

    @staticmethod
    def warn(msg):
        global logs
        global s_logging
        global logger_set

        if logger_set:
            s_logging.info(msg)
        else:
            print("%s WARN: %s" % (time.strftime("%c"), msg ))
        if type(msg) is list:
            logs['info'] += msg
        else:
            logs['info'].append(msg)

    @staticmethod
    def error(msg, code, ex=False,vpn=False):
        global logs
        global s_logging
        global logger_set

        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)

        if logger_set:
            s_logging.error(lines)
        # logs['error'].append({"msg": lines, "code": code})

        if logger_set:
            s_logging.error(msg)
        else:
            print("%s ERROR: %s" % (time.strftime("%c"), msg ))

        if vpn is True:
            logs["error"].append(msg)
            return

        if type(msg) is list:
            logs['error'] += msg
        elif type(msg) is dict:
            logs["error"].append(msg)
        else:
            logs['error'].append({"Error message": msg, "Error code": code})
        if (ex is True):
            logging.info("ex is true in error funtion of logger")
            logging.exit_log()
        else:
            logging.info("ex is false")

    @staticmethod
    def data( data):
        global logs
        logs['data'].update(data)

    @staticmethod
    def set_log_json_out(log_json):
        """
        Function used to set API URL for json log sending
        :param log_json:
        :return:
        """
        global log_json_out
        log_json_out = log_json

    @staticmethod
    def set_inline_callback_url(inlinecallbackurl):
        """
        Function used to set API URL for inline log sending
        :param inlinecallbackurl:
        :return:
        """
        global inline_callback_url
        inline_callback_url = inlinecallbackurl

    @staticmethod
    def exit_log():
        """
        Call at time when you want to exit program so that it will dump logs json to command line and also give logs json to API url set at initial time.
        :return:
        """
        try:
            global logs
            global log_json_out

            logging.info("In exit_log function")
            if log_json_out:
                logging.info("Sending data to COSMOS Appliance")
                logging.info("URL: %s" % log_json_out)
                logging.info("Data: %s" % json.dumps(logs))
                logs["info"]=[]
                r = requests.post(log_json_out, json=logs, timeout=30000 )
                if r.status_code != 200:
                    logging.info("Logger API response error status code: %s" % r.status_code )
                    logging.info(r.text)
                else:
                    logging.info("Callback URL success")
                    logging.info(r.text)
                # with open(log_json_out, 'w') as outfile:
                #     json.dump(logs, outfile)
            print("__Program__ __Output__")
            print(json.dumps(logs))
        except Exception as e:
            logging.info("Error in exit_log: [%s]" % e )
            #print json.dumps(logs)
            print(logs)
        sys.exit(0)

    @staticmethod
    def send_inline_logs(inlinelogs):
        """
        Function used to send inline logs to appliance
        :param job_name:
        :return:
        """
        global inline_callback_url

        logging.info("Sending inline logs")
        logging.info(json.dumps(inlinelogs))
        try:
            if inline_callback_url:
                logging.info("Sending inline status to COSMOS Appliance")
                logging.info("URL: %s" % inline_callback_url)
                logging.info("Data: %s" % json.dumps(inlinelogs))
                logs["info"]=[]
                r = requests.post(inline_callback_url, json=inlinelogs, timeout=30000 )
                if r.status_code != 200:
                    logging.info("Logger API response error status code: %s" % r.status_code )
                    logging.info(r.text)
                else:
                    logging.info("Inline Callback URL success")
                    logging.info(r.text)
        except Exception as e:
            logging.info("Error in Send Inline Logs: [%s]" % e )
            print(json.dumps(inlinelogs))

    def send_response_to_c3(self,portalUrl,response,auth):
        """
           params : portalurl : java post api
           payload
           auth : binary
        """

        encoded = base64.b64encode(auth)
        headers = {"Authorization" : b"Basic " + encoded,

                    "Content-Type": "application/json",
                    "X-Request-Type" : "Appliance"
                  }
        if isinstance(response,dict):
            logs["data"]=response
        response = json.dumps(logs)
        print (response)
        r = requests.post(portalUrl, data=response, headers=headers)

        if r.status_code != 200:
            self.info("task.status failed")
            self.error("Error in sending virtual machine data to CosmOs, data received as: %s ::: API RESPONSE CODE IS -%s " %(logs,r.status_code), 100, ex=True)
        self.info("cosmos portal URL: [%s]" % portalUrl)
        self.info("Headers Values for post api are: [%s]" % headers)
        self.info("post api response code: [%s]" % r.status_code)
        self.info("task.status passed")
        return -1
