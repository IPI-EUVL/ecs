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
from ipi_ecs.logging.client import LogClient
from ipi_ecs.subsystems.lifecycle_manager import LifecycleManager

LOG_PATH = os.environ.get("ECS_LOG_DIR", os.path.join(platformdirs.site_data_dir("ipi-ecs", "IPI"), "lifecycle_manager.log"))
#LOG_PATH = r"C:\\euvl\\logs\\dds_service.log"

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

class LifecycleManagerService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ipi-ecs-LifecycleManagerService"
    _svc_display_name_ = "ipi-ecs Lifecycle Manager Service"
    _svc_description_ = "Lifecycle Manager Service for ipi-ecs"

    def __init__(self, args):
        self.__logger_sock = None
        self.__logger = None
        self._lifecycle_manager = None
        _log_to_file("LifecycleManagerService.__init__ sys.executable={0}".format(sys.executable))
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

        self.is_running = True

        self.__uuid = uuid.uuid4()

    def SvcStop(self):
        _log_to_file("LifecycleManagerService.SvcStop")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_running = False

    def add_subsystems(self):
        pass

    def SvcDoRun(self):
        _log_to_file("LifecycleManagerService.SvcDoRun")
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

            self.__logger = LogClient(self.__logger_sock, origin_uuid=uuid.UUID(bytes=bytes(16)))
            self.__logger.log("Lifecycle Manager Service initialized.", level="INFO", subsystem="LifecycleManagerService")

            self._lifecycle_manager = LifecycleManager(self.__uuid)
            print("Adding subsystems...")
            self.add_subsystems()
            print("Done")

            while self.is_running and self._lifecycle_manager.ok():
                rc = win32event.WaitForSingleObject(self.hWaitStop, 5000)
                if rc == win32event.WAIT_OBJECT_0:
                    break
                time.sleep(1)

                #raise Exception("Simulated exception for testing")  # Remove or comment out in production

            self.__logger.log("LifecycleManagerService.SvcDoRun stopping.", level="INFO", subsystem="LifecycleManagerService")
            _log_to_file("LifecycleManagerService.SvcDoRun stopping.")
        except Exception as e:
            _log_to_file(f"LifecycleManagerService.SvcDoRun exception:\n{traceback.format_exc()}")
            servicemanager.LogErrorMsg(
                f"Lifecycle Manager Service failed:\n{traceback.format_exc()}"
            )
            for line in traceback.format_exception(None, e, e.__traceback__):
                for split in line.split("\n"):
                    self.__logger.log(split, level="ERROR", subsystem="LifecycleManagerService")
            raise e
        finally:
            if hasattr(self, "_lifecycle_manager"):
                self._lifecycle_manager.close()
            if hasattr(self, "__logger_sock"):
                self.__logger_sock.close()
            _log_to_file("LifecycleManagerService.SvcDoRun stopped")
            self.ReportServiceStatus(win32service.SERVICE_STOPPED)

