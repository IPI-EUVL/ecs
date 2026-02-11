from math import floor
import multiprocessing
import os
import pickle
import struct
import time
import uuid
import sys
import mt_events
import segment_bytes

from enum import Enum

from ipi_ecs.core import daemon
from ipi_ecs.dds.magics import *
import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types
import ipi_ecs.dds.client as client
import ipi_ecs.dds.magics as magics
import ipi_ecs.core.tcp as tcp
import ipi_ecs.db.db_library as db_library

from ipi_ecs.logging.client import LogClient
from ipi_ecs.subsystems.experiment_controller import ExperimentController, RunSettings, RunState

from chamber_ctl.subsystems import uuids
from chamber_ctl.subsystems.target_motion import TargetMotion, TargetMotionConfig, TargetMotionProfile, MotionSegment, MotionState
from chamber_ctl.subsystems.ljs_target_motion import LJSerialTargetMotion
from chamber_ctl.subsystems.exposure_controller import ExposureSettings

class ExperimentClient:
    EXP_IN_PROGRESS = b"Experiment is in progress."

    def __init__(self, exp_name: str, client_name: str, logger: LogClient):
        self.__exp_name = exp_name
        self.__client_name = client_name
        self.__logger = logger

        self.__subsystem = None

        self.__preinit_handle = None
        self.__start_handle = None
        self.__stop_handle = None

        self.__xstate_publisher = None

        self.__settings_type = RunSettings
        
    def _can_preinit(self, settings: RunSettings, state: RunState) -> tuple[bool, bytes]:
        return True, OP_OK
    
    def _on_continue_state(self):
        return True, self.EXP_IN_PROGRESS
    
    def _on_preinit(self, handle) -> bytes:
        return b": " + self.__client_name.encode() + b" is starting."
    
    def _on_did_preinit(self, reason: bytes | None = None):
        assert self.__preinit_handle is not None

        ret = OP_OK + b": " + self.__client_name.encode() + b" has initialized successfully."

        if reason:
            ret = OP_OK + b": " + self.__client_name.encode() + reason
        
        self.__preinit_handle.ret(ret)
        self.__preinit_handle = None

    def _can_start(self, settings: RunSettings, state: RunState) -> tuple[bool, bytes]:
        return True, OP_OK
    
    def _on_start(self, handle) -> bytes:
        return b": " + self.__client_name.encode() + b" is running."
    
    def _on_did_start(self, reason: bytes | None = None):
        assert self.__start_handle is not None

        ret = OP_OK + b": " + self.__client_name.encode() + b" has started successfully."

        if reason:
            ret = OP_OK + b": " + self.__client_name.encode() + reason

        self.__start_handle.ret(ret)
        self.__start_handle = None
    
    def _on_stop(self, handle) -> bytes:
        return b": " + self.__client_name.encode() + b" is stopping."
    
    def _on_did_stop(self, reason: bytes | None = None):
        assert self.__stop_handle is not None

        ret = OP_OK + b": " + self.__client_name.encode() + b" has stopped successfully."

        if reason:
            ret = OP_OK + b": " + self.__client_name.encode() + reason

        self.__stop_handle.ret(ret)
        self.__stop_handle = None
            
    
    def __on_can_start_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        #print("Can start exposure event called by:", s_uuid, param)
        #print(param)
        decoded_param = segment_bytes.decode(param)
        settings = self.__settings_type.decode(decoded_param[0].decode("utf-8"))
        state = RunState.decode(decoded_param[1].decode("utf-8"))
        #print("Decoded param:", settings, state)
        ok, reason = self._can_start(settings, state)

        if not ok:
            print("Cannot start:", reason.decode("utf-8"))
            handle.fail((f"Cannot start {self.__client_name}: ").encode("utf-8") + reason)
            return
        
        handle.ret(f"{self.__client_name} can start.".encode("utf-8"))

    def __on_preinit_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        decoded_param = segment_bytes.decode(param)
        settings = self.__settings_type.decode(decoded_param[0].decode("utf-8"))
        state = RunState.decode(decoded_param[1].decode("utf-8"))
        ok, reason = self._can_preinit(settings, state)

        if not ok:
            print("Cannot start :", reason.decode("utf-8"))
            self.__logger.log("Preinit event called but cannot start " + self.__client_name + ": " + reason.decode("utf-8"), level="WARN", l_type="CTRL", subsystem=self.__client_name)
            handle.fail((f"Cannot start {self.__client_name}: ").encode("utf-8") + reason)
            return
        
        self.__logger.log(f"Pre-initializing {self.__client_name}.", level="INFO", l_type="CTRL", subsystem=self.__client_name, event="preinit_exp")
        
        ret = self._on_preinit(handle)

        assert self.__preinit_handle is None

        self.__preinit_handle = handle
        self.__preinit_handle.feedback(OP_IN_PROGRESS + ret)

    def __on_start_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        #print("Start event called by:", s_uuid, param)
        decoded_param = segment_bytes.decode(param)
        settings = self.__settings_type.decode(decoded_param[0].decode("utf-8"))
        state = RunState.decode(decoded_param[1].decode("utf-8"))

        ok, reason = self._can_start(settings, state)

        if not ok:
            print("Cannot start:", reason.decode("utf-8"))
            self.__logger.log("Start event called but cannot start " + self.__client_name + ": " + reason.decode("utf-8"), level="WARN", l_type="CTRL", subsystem=self.__client_name)
            handle.fail((f"Cannot start {self.__client_name}: ").encode("utf-8") + reason)
            return
        
        self.__logger.log(f"Starting {self.__client_name}.", level="INFO", l_type="CTRL", subsystem=self.__client_name, event="start_exp")

        ret = self._on_start(handle)

        self.__start_handle = handle
        self.__start_handle.feedback(OP_IN_PROGRESS + ret)

    def __on_stop_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        #print("Stop event called by:", s_uuid, param)
        decoded_param = segment_bytes.decode(param)
        end_state = int.from_bytes(decoded_param[0], 'big')
        r_uuid = uuid.UUID(bytes=decoded_param[1]) if len(decoded_param[1]) > 0 else None
        end_reason = decoded_param[2] if len(decoded_param[2]) > 0 else None

        ret = self._on_stop(handle)

        self.__stop_handle = handle

        self.__logger.log(f"Stopping {self.__client_name}, run: {r_uuid}, reason: {end_reason}, with state: {end_state}", level="INFO", l_type="CTRL", subsystem=self.__client_name, event="stop_exp")

        handle.feedback(OP_IN_PROGRESS + ret)

    def _setup_subsystem(self, handle: client._RegisteredSubsystemHandle):
        self.__subsystem = handle

        handle.add_event_handler(f"can_begin_{self.__exp_name}".encode("utf-8")).on_called(self.__on_can_start_event)
        handle.add_event_handler(f"preinit_{self.__exp_name}".encode("utf-8")).on_called(self.__on_preinit_event)
        handle.add_event_handler(f"init_{self.__exp_name}".encode("utf-8")).on_called(self.__on_start_event)
        handle.add_event_handler(f"stopped_{self.__exp_name}".encode("utf-8")).on_called(self.__on_stop_event)

        self.__xstate_publisher = handle.add_kv_handler(b"exp_state")
        self.__xstate_publisher.on_get(self.__on_xstate_read)
        self.__xstate_publisher.set_type(types.ByteTypeSpecifier())

    def __on_xstate_read(self, requester):
        ok, reason = self._on_continue_state()
        return segment_bytes.encode([ok.to_bytes(1, 'big'), reason])
    
    def register_experiment_settings_type(self, settings_type: type[RunSettings]):
        self.__settings_type = settings_type
