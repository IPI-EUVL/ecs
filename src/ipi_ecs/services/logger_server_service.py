import time
import uuid
import os
import traceback
import sys
import logging

import win32serviceutil
import win32service
import win32event
import servicemanager
import platformdirs

from ipi_ecs.logging.logger_server import run_logger_server
from ipi_ecs.core.daemon import StopFlag
from ipi_ecs.logging.journal import resolve_log_dir

LOG_PATH = os.environ.get("ECS_LOG_PATH", os.path.join(platformdirs.site_data_dir("ipi-ecs", "IPI"), "logger_service.log"))
print(LOG_PATH)
#LOG_PATH = r"C:\\euvl\\logs\\logger_service.log"

def _log_to_file(message: str) -> None:
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(message + "\n")
    #pylint: disable=broad-except
    except Exception:
        # Avoid crashing the service if logging fails.
        pass

_log_to_file("module load: sys.executable={0}".format(sys.executable))

class LoggerService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ipi-ecs-LoggerService"
    _svc_display_name_ = "ipi-ecs Logger Service"
    _svc_description_ = "Logger Service for ipi-ecs"

    def __init__(self, args):
        self.__m_server = None
        _log_to_file("LoggerService.__init__ sys.executable={0}".format(sys.executable))
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

        self.is_running = True
        self.__stop_flag = None
        print("Running LoggerService...")

    def SvcStop(self):
        _log_to_file("LoggerService.SvcStop")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_running = False
        self.__stop_flag.stop()

    def SvcDoRun(self):
        print("Logger Service is starting...")
        _log_to_file("LoggerService.SvcDoRun")
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        try:
            host = "0.0.0.0"
            port = int(os.environ.get("ECS_LOG_PORT"))
            _log_to_file(f"LoggerService.SvcDoRun, Using: {host}:{port}")

            log_dir = resolve_log_dir(None, env_var="ECS_LOG_DIR")
            _log_to_file(f"LoggerService.SvcDoRun, Using: {log_dir}")

            self.__stop_flag = StopFlag()
            run_logger_server(
                (host, port),
                log_dir,
                rotate_max_bytes=int(os.environ.get("ECS_LOG_ROTATE_MAX_MB", 256)) * 1024 * 1024,
                stop_flag=self.__stop_flag
            )

        except Exception as e:
            _log_to_file(f"LoggerService.SvcDoRun exception:\n{traceback.format_exc()}")
            servicemanager.LogErrorMsg(
                f"Logger Service failed:\n{traceback.format_exc()}"
            )

            raise e
        finally:
            if hasattr(self, "__m_server"):
                self.__m_server.close()
            if hasattr(self, "__logger_sock"):
                self.__logger_sock.close()
            _log_to_file("LoggerService.SvcDoRun stopped")
            self.ReportServiceStatus(win32service.SERVICE_STOPPED)

def main():
    if len(sys.argv) == 1:
        _log_to_file("main: StartServiceCtrlDispatcher")
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(LoggerService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # Called from command line with parameters (install, start, etc.)
        rc = win32serviceutil.HandleCommandLine(LoggerService)
        print("HandleCommandLine rc:", rc)

if __name__ == '__main__':
    main()
