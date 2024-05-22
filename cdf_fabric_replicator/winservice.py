import concurrent.futures
import logging
import os
import socket
import sys
import threading
from typing import Optional

import servicemanager
import win32event
import win32evtlog
import win32evtlogutil
import win32service
import win32serviceutil
from cognite.extractorutils.exceptions import InvalidConfigError
from cognite.extractorutils.threading import CancellationToken

from cognite.extractorutils.metrics import safe_get

from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.data_modeling import DataModelingReplicator
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from cdf_fabric_replicator.event import EventsReplicator

from cdf_fabric_replicator.metrics import Metrics


# Should be the same as product_short_name in installer/setup-config.json
service_name = "FabricConnector"
service_display_name = "Cognite Fabric Connector"
service_description = "Connector to Fabric and exchange data with CDF"


class WindowsService(win32serviceutil.ServiceFramework):
    _svc_name_ = service_name
    _svc_display_name_ = service_display_name
    _svc_description_ = service_description

    @classmethod
    def parse_command_line(cls) -> None:
        win32serviceutil.HandleCommandLine(cls)

    def __init__(self, args: list) -> None:
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self.cancellation_token = CancellationToken()

    def SvcStop(self) -> None:
        self.cancellation_token.cancel()
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self) -> None:
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, ""),
        )
        os.chdir(os.path.dirname(sys.executable))
        try:
            config_file_path = os.path.join(
                os.path.dirname(sys.executable), "config.yaml"
            )
            worker_list = []

            with EventsReplicator(
                metrics=safe_get(Metrics),
                stop_event=self.cancellation_token.create_child_token(),
                override_config_path=config_file_path,
            ) as event_replicator:
                worker_list.append(threading.Thread(target=event_replicator.run))

            with TimeSeriesReplicator(
                metrics=safe_get(Metrics),
                stop_event=self.cancellation_token.create_child_token(),
                override_config_path=config_file_path,
            ) as ts_replicator:
                worker_list.append(threading.Thread(target=ts_replicator.run))

            with CdfFabricExtractor(
                metrics=safe_get(Metrics),
                stop_event=self.cancellation_token.create_child_token(),
                override_config_path=config_file_path,
            ) as extractor:
                worker_list.append(threading.Thread(target=extractor.run))

            with DataModelingReplicator(
                metrics=safe_get(Metrics),
                stop_event=self.cancellation_token.create_child_token(),
                override_config_path=config_file_path,
            ) as dm_replicator:
                worker_list.append(threading.Thread(target=dm_replicator.run))

            for worker in worker_list:
                worker.start()

            for worker in worker_list:
                worker.join()

        except InvalidConfigError as e:
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_ERROR_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, "Invalid config file {}".format(e)),
            )
            raise e
        except SystemExit as e:
            logging.getLogger(__name__).exception(f"SystemExit ({e.code}) was thrown")
            if e.code != 0:
                self.ReportServiceStatus(win32service.SERVICE_ERROR_CRITICAL)
            raise e  # crash and notify service manager that something went wrong
        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Unexpected error during run: {str(e)}"
            )
            error_event_log(f"Unexpected error during run: {str(e)}")
            raise e  # crash and notify service manager that something went wrong


def error_event_log(message: str, additional: Optional[str] = None) -> None:
    if additional is None:
        additional = ""

    win32evtlogutil.ReportEvent(
        service_name,
        0,
        eventType=win32evtlog.EVENTLOG_WARNING_TYPE,
        strings=message,
        data=additional.encode(),
    )


if __name__ == "__main__":
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(WindowsService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(WindowsService)
