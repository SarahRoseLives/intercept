"""
Process health monitoring and auto-restart functionality.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, Optional, Any

logger = logging.getLogger('intercept.process_monitor')


@dataclass
class ProcessInfo:
    """Information about a monitored process."""
    name: str
    process: Any  # subprocess.Popen
    started_at: datetime = field(default_factory=datetime.now)
    restart_count: int = 0
    last_restart: Optional[datetime] = None
    restart_callback: Optional[Callable] = None
    max_restarts: int = 3
    backoff_seconds: float = 5.0
    enabled: bool = True


class ProcessMonitor:
    """
    Monitor and auto-restart processes.

    Usage:
        monitor = ProcessMonitor()
        monitor.register('pager', process, restart_callback=start_pager)
        monitor.start()
    """

    def __init__(self, check_interval: float = 5.0):
        self.processes: Dict[str, ProcessInfo] = {}
        self.check_interval = check_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def register(
        self,
        name: str,
        process: Any,
        restart_callback: Optional[Callable] = None,
        max_restarts: int = 3,
        backoff_seconds: float = 5.0
    ) -> None:
        """
        Register a process for monitoring.

        Args:
            name: Unique name for the process
            process: The subprocess.Popen object
            restart_callback: Function to call to restart the process
            max_restarts: Maximum number of automatic restarts
            backoff_seconds: Base backoff time between restarts
        """
        with self._lock:
            self.processes[name] = ProcessInfo(
                name=name,
                process=process,
                restart_callback=restart_callback,
                max_restarts=max_restarts,
                backoff_seconds=backoff_seconds
            )
            logger.info(f"Registered process for monitoring: {name}")

    def unregister(self, name: str) -> None:
        """Remove a process from monitoring."""
        with self._lock:
            if name in self.processes:
                del self.processes[name]
                logger.info(f"Unregistered process: {name}")

    def update_process(self, name: str, process: Any) -> None:
        """Update the process object for a registered name."""
        with self._lock:
            if name in self.processes:
                self.processes[name].process = process
                self.processes[name].started_at = datetime.now()

    def start(self) -> None:
        """Start the monitoring thread."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        logger.info("Process monitor started")

    def stop(self) -> None:
        """Stop the monitoring thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=self.check_interval + 1)
        logger.info("Process monitor stopped")

    def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._running:
            self._check_all_processes()
            time.sleep(self.check_interval)

    def _check_all_processes(self) -> None:
        """Check health of all registered processes."""
        with self._lock:
            for name, info in list(self.processes.items()):
                if not info.enabled:
                    continue

                if info.process is None:
                    continue

                # Check if process has terminated
                return_code = info.process.poll()
                if return_code is not None:
                    logger.warning(
                        f"Process '{name}' terminated with code {return_code}"
                    )
                    self._handle_crash(name, info)

    def _handle_crash(self, name: str, info: ProcessInfo) -> None:
        """Handle a crashed process."""
        if info.restart_callback is None:
            logger.info(f"No restart callback for '{name}', skipping auto-restart")
            return

        if info.restart_count >= info.max_restarts:
            logger.error(
                f"Process '{name}' exceeded max restarts ({info.max_restarts}), "
                "disabling auto-restart"
            )
            info.enabled = False
            return

        # Calculate backoff with exponential increase
        backoff = info.backoff_seconds * (2 ** info.restart_count)
        logger.info(
            f"Attempting to restart '{name}' in {backoff:.1f}s "
            f"(attempt {info.restart_count + 1}/{info.max_restarts})"
        )

        # Wait for backoff period
        time.sleep(backoff)

        # Attempt restart
        try:
            info.restart_callback()
            info.restart_count += 1
            info.last_restart = datetime.now()
            logger.info(f"Successfully restarted '{name}'")
        except Exception as e:
            logger.error(f"Failed to restart '{name}': {e}")
            info.restart_count += 1

    def get_status(self) -> Dict[str, Any]:
        """
        Get status of all monitored processes.

        Returns:
            Dict with process status information
        """
        with self._lock:
            status = {}
            for name, info in self.processes.items():
                is_running = (
                    info.process is not None and
                    info.process.poll() is None
                )
                status[name] = {
                    'running': is_running,
                    'started_at': info.started_at.isoformat() if info.started_at else None,
                    'restart_count': info.restart_count,
                    'last_restart': info.last_restart.isoformat() if info.last_restart else None,
                    'auto_restart_enabled': info.enabled,
                    'return_code': info.process.poll() if info.process else None
                }
            return status

    def reset_restart_count(self, name: str) -> None:
        """Reset the restart count for a process (e.g., after manual restart)."""
        with self._lock:
            if name in self.processes:
                self.processes[name].restart_count = 0
                self.processes[name].enabled = True

    def is_healthy(self) -> bool:
        """Check if all processes are healthy."""
        with self._lock:
            for info in self.processes.values():
                if info.process is not None and info.process.poll() is not None:
                    return False
            return True


# Global monitor instance
process_monitor = ProcessMonitor()
