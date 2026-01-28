import argparse
import contextlib
import io
import queue
import sys
import time
import mt_events
import uuid
import threading
from typing import Dict, Iterable, Set

from ipi_ecs.dds import magics
from ipi_ecs.core import daemon

def wait_for(awaiter: mt_events.Awaiter, timeout: float = 10.0):
    done = False
    r_state, r_reason, r_value = None, None, None

    def on_done(state=None, reason=None, value=None):
        nonlocal done, r_state, r_reason, r_value
        r_reason = reason
        r_state = state
        r_value = value
        
        done = True

    begin = time.monotonic()
    awaiter.then(lambda h: on_done(value=h)).catch(on_done)

    while not done and (time.monotonic() - begin) < timeout:
        time.sleep(0.1)

    if done:
        return r_value, r_state, r_reason
    
    raise TimeoutError("Awaiter did not complete in time.")

def wait_for_event(awaiter: mt_events.Awaiter, s_uuid: uuid.UUID, timeout: float = 10.0):
    done = False
    r_state, r_reason, r_value = None, None, None

    def on_done(state=None, reason=None, value=None):
        nonlocal done, r_state, r_reason, r_value
        r_reason = reason
        r_state = state
        r_value = value
        
        done = True

    begin = time.monotonic()
    awaiter.then(lambda h: on_done(value=h)).catch(on_done)

    while not done and (time.monotonic() - begin) < timeout:
        time.sleep(0.1)

        print(r_state, r_reason, r_value)
    if done:
        if r_value is not None:
            r_state = r_value.get_state(s_uuid)
            if r_value.get_result(s_uuid) is not None:
                if not r_value.get_result(s_uuid).startswith(magics.OP_OK):
                    r_reason = r_value.get_result(s_uuid)
            else:
                r_reason = "No result from target."
            
            r_value = r_value.get_result(s_uuid)
        
        return r_value, r_state, r_reason
    
    raise TimeoutError("Awaiter did not complete in time.")

class CaptiveCLITemplate:
    def __init__(self, name, desc):
        self.name = name
        self.desc = desc

        p = argparse.ArgumentParser(description=desc)

        sub = p.add_subparsers(dest="cmd", required=True)

        pl = sub.add_parser("help", help="Print this help message.")
        pl.set_defaults(fn=lambda args: p.print_help())

        self._build_parser(sub, p)

        self.__parser = p

        self.__out_queue = queue.Queue()

        self.__stdout = sys.stdout
        self.__stderr = sys.stderr

        self.captured_output = io.StringIO()
        self.captured_output_stderr = io.StringIO()

        self.__daemon = daemon.Daemon()
        self.__daemon.add(self.__input_parse_thread)
        self.__daemon.add(self.__queue_thread)

        self.__daemon.start()

    def _build_parser(self, sub: argparse._SubParsersAction, p: argparse.ArgumentParser):
        pass


    def parse_and_execute(self, argstr: Iterable[str]) -> str:
        try:
            m_args = self.__parser.parse_args(argstr)
            m_args.fn(m_args)
        except SystemExit as e:
            print(f"Argument parsing failed: {e}")
        except Exception as e:
            print(f"Error executing command: {e}")
            raise

    def __input_parse_thread(self, stop_flag: daemon.StopFlag):
        try:
            while stop_flag.run():
                commands = input()
                if commands.strip().lower() in ("exit", "quit"):
                    print("Exiting...")
                    break

                self.parse_and_execute(commands.strip().split())

        except KeyboardInterrupt:
            pass
        except EOFError:
            pass
        finally:
            self.close()

        return 0
    
    def __queue_thread(self, stop_flag: daemon.StopFlag):
        try:
            print(f"{self.name}> ", end="", flush=True)
            while stop_flag.run():
                with contextlib.redirect_stdout(self.captured_output):
                    with contextlib.redirect_stderr(self.captured_output_stderr):
                        time.sleep(0.1)
                        output = self.captured_output.getvalue() + self.captured_output_stderr.getvalue()
                        if output.count("\n") != 0:
                            self.captured_output.truncate(0)
                            self.captured_output.seek(0)
                            self.captured_output_stderr.truncate(0)
                            self.captured_output_stderr.seek(0)

                            for s_str in output.splitlines(keepends=False):
                                self.__out_queue.put(s_str)
                
                with contextlib.redirect_stdout(self.__stdout):
                    with contextlib.redirect_stderr(self.__stderr):
                        if not self.__out_queue.empty():
                            while not self.__out_queue.empty():
                                output = self.__out_queue.get()
                                print(f"{self.name}> ", output, "", sep="")

                            print(f"{self.name}> ", end="", flush=True)

        except KeyboardInterrupt:
            pass
        finally:
            pass

        return 0
    
    def ok(self) -> bool:
        return self.__daemon.is_ok()
    
    def close(self):
        self.__daemon.stop()

        contextlib.redirect_stdout(self.__stdout)
        contextlib.redirect_stderr(self.__stderr)

def main():
    cli = CaptiveCLITemplate("CaptiveCLI", "A captive command line interface.")
    try:
        while cli.ok():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        cli.close()

if __name__ == "__main__":
    main()