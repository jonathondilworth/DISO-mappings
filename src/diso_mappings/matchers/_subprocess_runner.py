"""
Driver-side helper: run a matcher subprocess with std::out and std::err captured 
to a per-pair log file (escalate timeout via SIGTERM -> SIGKILL), and optionally
invoke a tick callback once per polling interval for UX purposes (e.g. a live
terminal timer).

The runner has no matcher-specific knowledge: no knowledge of JVMs, of where
the log file lives in the run directory layout, or of what the subprocess
writes. Those concerns stay in the adapters. The runner only knows:

    * how to wrap a command in a new process session
    * how to stream std::out and std::err to a writable file path
    * how to poll in ticks so an adapter can update its terminal UX, and
    * how to terminate the process group on timeout
"""
from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Callable

from diso_mappings.io.terminal import warn


_TERMINATE_GRACE_SECONDS = 10.0



def run_subprocess_with_timeout(
    cmd: list[str],
    *,
    cwd: Path,
    timeout: float | None,
    log_path: Path,
    on_tick: Callable[[float], None] | None = None,
    tick_interval: float = 1.0,
    matcher_display_name: str = "subprocess",
    env: dict[str, str] | None = None,
) -> int:
    """
    Runs 'cmd' as a subprocess (in a new session) with std::out and std::err 
    written to 'log_path', where every polling occurs each 'tick_interval' 
    seconds and, if 'on_tick' is provided (as a callback), then invoke it with 
    the elapsed wall-clock time; on timeout, send SIGTERM (terminate) to the 
    process group; if that's ignored for `_TERMINATE_GRACE_SECONDS`,
    escalate to SIGKILL (kill process). Should return the childs exit code 
    on a clean completion. Raises TimeoutError on timeout. WARNING: some
    matchers have different exit codes, for instance: AML has a known 
    "exit 0 on save failure" path (somewhere in its code).
    """
    log_path.parent.mkdir(parents=True, exist_ok=True) # makes the logs dir
    with open(log_path, "wb") as log_file:
        log_file.write(b"+ " + " ".join(cmd).encode() + b"\n")
        log_file.flush() # captures the initial command
        child_proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        ) # starts subprocess
        loop_start = time.monotonic()
        while True: # (timed) polling loop
            elapsed = time.monotonic() - loop_start
            if on_tick is not None:
                wait_budget = tick_interval
                if timeout is not None:
                    remaining = timeout - elapsed
                    if remaining <= 0:
                        wait_budget = 0.0  # over budget
                    else:
                        wait_budget = min(tick_interval, remaining)
                    # end-if : time budget specified
                # end-if : on-tick behaviour specified
            else:
                if timeout is not None:
                    remaining = timeout - elapsed
                    wait_budget = max(0.0, remaining)
                else: # no timeout specificed (wait forever)
                    wait_budget = None
                # end-if : timeout budget specified
            # end-if : no on-tick behaviour specified
            
            try:
                return child_proc.wait(timeout=wait_budget)
            
            except subprocess.TimeoutExpired: # timeout expended
                elapsed = time.monotonic() - loop_start
                if timeout is not None and elapsed >= timeout:
                    break  # drop out of the loop, proceed to termination
                # else: just a tick (invoke callback if set)
                if on_tick is not None:
                    try: on_tick(elapsed)
                    except Exception as tick_exception:
                        # we don't really expect an on-tick callback to raise, so...
                        warn(f"on_tick callback raised {type(tick_exception).__name__}: {tick_exception}.")
                        on_tick = None # issues a warning and disables on-tick behaviour

        # termination: out-of-budget path
        warn(f"{matcher_display_name} exceeded timeout; sending SIGTERM (pgid={child_proc.pid})")
        try:
            os.killpg(child_proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass  # process already gone

        try:
            child_proc.wait(timeout=_TERMINATE_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            warn(f"{matcher_display_name} ignored SIGTERM; sending SIGKILL")
            try:
                os.killpg(child_proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child_proc.wait(timeout=5)

        # we only reach this point when `timeout is not None` AND `elapsed >= timeout`
        raise TimeoutError(f"{matcher_display_name} exceeded timeout of {timeout:g}s")