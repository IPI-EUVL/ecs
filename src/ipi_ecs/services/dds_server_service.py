import time
import uuid
import os
import traceback
import sys
import logging

import platformdirs
import win32serviceutil
import win32service
import win32event
import servicemanager

import ipi_ecs.core.tcp as tcp
from ipi_ecs.dds.server import get_server
from ipi_ecs.logging.client import LogClient

LOG_PATH = os.environ.get("ECS_LOG_DIR", os.path.join(platformdirs.site_data_dir("ipi-ecs", "IPI"), "dds_service.log"))
#LOG_PATH = r"C:\\euvl\\logs\\dds_service.log"

def _log_to_file(message: str) -> None:
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(message + "\n")
    except Exception:
        # Avoid crashing the service if logging fails.
        pass

_log_to_file("module load: sys.executable={0}".format(sys.executable))

class DDSServerService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ipi-ecs-DDSServerService"
    _svc_display_name_ = "ipi-ecs DDS Server Service"
    _svc_description_ = "DDS Server Service for ipi-ecs"

    def __init__(self, args):
        self.__logger_sock = None
        self.__logger = None
        self.__m_server = None
        _log_to_file("DDSServerService.__init__ sys.executable={0}".format(sys.executable))
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

        self.is_running = True
        print("Running DDSServerService...")

    def SvcStop(self):
        _log_to_file("DDSServerService.SvcStop")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_running = False

    def SvcDoRun(self):
        print("DDS Server Service is starting...")
        _log_to_file("DDSServerService.SvcDoRun")
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        try:
            # Initialize network resources only after SCM has started the service.
            self.__logger_sock = tcp.TCPClientSocket()
            self.__logger_sock.connect(("127.0.0.1", 11751))
            self.__logger_sock.start()

            host = os.environ.get("ECS_HOST")
            port = int(os.environ.get("ECS_PORT"))

            self.__logger = LogClient(self.__logger_sock, origin_uuid=uuid.UUID(bytes=bytes(16)))
            self.__logger.log(f"SERVICE INIT, Using: {host}, {port}", level="INFO", subsystem="DDSServerService")
            _log_to_file(f"DDSServerService.SvcDoRun, Using: {host}:{port}")

            self.__m_server = get_server(host, port, self.__logger)
            time.sleep(0.1)
            self.__m_server.start()

            while self.is_running:
                rc = win32event.WaitForSingleObject(self.hWaitStop, 5000)
                if rc == win32event.WAIT_OBJECT_0:
                    break
                time.sleep(1)

                #raise Exception("Simulated exception for testing")  # Remove or comment out in production

            self.__logger.log(f"DDSServerService.SvcDoRun stopping.", level="INFO", subsystem="DDSServerService")
        except Exception as e:
            _log_to_file(f"DDSServerService.SvcDoRun exception:\n{traceback.format_exc()}")
            servicemanager.LogErrorMsg(
                f"DDS Server Service failed:\n{traceback.format_exc()}"
            )
            for line in traceback.format_exception(None, e, e.__traceback__):
                for split in line.split("\n"):
                    self.__logger.log(split, level="ERROR", subsystem="DDSServerService")
            raise e
        finally:
            if hasattr(self, "__m_server"):
                self.__m_server.close()
            if hasattr(self, "__logger_sock"):
                self.__logger_sock.close()
            _log_to_file("DDSServerService.SvcDoRun stopped")
            self.ReportServiceStatus(win32service.SERVICE_STOPPED)

def main():
    if len(sys.argv) == 1:
        _log_to_file("main: StartServiceCtrlDispatcher")
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(DDSServerService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # Called from command line with parameters (install, start, etc.)
        rc = win32serviceutil.HandleCommandLine(DDSServerService)
        print("HandleCommandLine rc:", rc)

if __name__ == '__main__':
    main()
