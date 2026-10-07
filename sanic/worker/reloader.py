from __future__ import annotations

import os
import sys

from asyncio import new_event_loop
from itertools import chain
from multiprocessing.connection import Connection
from pathlib import Path
from signal import SIGINT, SIGTERM
from signal import signal as signal_func
from time import sleep

from sanic.startup.dependencies import run_process_event
from sanic.worker.loader import AppLoader


class Reloader:
    INTERVAL = 1.0  # seconds

    def __init__(
        self,
        publisher: Connection,
        interval: float,
        reload_dirs: set[Path],
        app_loader: AppLoader,
    ):
        self._publisher = publisher
        self.interval = interval or self.INTERVAL
        self.reload_dirs = reload_dirs
        self.run = True
        self.app_loader = app_loader

    def __call__(self) -> None:
        app = self.app_loader.load()
        signal_func(SIGINT, self.stop)
        signal_func(SIGTERM, self.stop)
        mtimes: dict[str, float] = {}

        reloader_start = app.listeners.get("reload_process_start")
        reloader_stop = app.listeners.get("reload_process_stop")
        before_trigger = app.listeners.get("before_reload_trigger")
        after_trigger = app.listeners.get("after_reload_trigger")
        loop = new_event_loop()
        if reloader_start or app.listener_orchestrator.has(
            "reload_process_start"
        ):
            run_process_event(app, "reload_process_start", loop)

        while self.run:
            changed = set()
            for filename in self.files():
                try:
                    if self.check_file(filename, mtimes):
                        path = (
                            filename
                            if isinstance(filename, str)
                            else filename.resolve()
                        )
                        changed.add(str(path))
                except OSError:
                    continue
            if changed:
                if before_trigger or app.listener_orchestrator.has(
                    "before_reload_trigger"
                ):
                    run_process_event(app, "before_reload_trigger", loop)
                self.reload(",".join(changed) if changed else "unknown")
                if after_trigger or app.listener_orchestrator.has(
                    "after_reload_trigger"
                ):
                    run_process_event(
                        app, "after_reload_trigger", loop, changed=changed
                    )
            sleep(self.interval)
        else:
            if reloader_stop or app.listener_orchestrator.has(
                "reload_process_stop"
            ):
                run_process_event(
                    app, "reload_process_stop", loop, reverse=True
                )

    def stop(self, *_):
        self.run = False

    def reload(self, reloaded_files):
        message = f"__ALL_PROCESSES__:{reloaded_files}"
        self._publisher.send(message)

    def files(self):
        return chain(
            self.python_files(),
            *(d.glob("**/*") for d in self.reload_dirs),
        )

    def python_files(self):  # no cov
        """项目内部接口说明。"""
        # The list call is necessary on Python 3 in case the module
        # dictionary modifies during iteration.
        for module in list(sys.modules.values()):
            if module is None:
                continue
            filename = getattr(module, "__file__", None)
            if filename:
                old = None
                while not os.path.isfile(filename):
                    old = filename
                    filename = os.path.dirname(filename)
                    if filename == old:
                        break
                else:
                    if filename[-4:] in (".pyc", ".pyo"):
                        filename = filename[:-1]
                    yield filename

    @staticmethod
    def check_file(filename, mtimes) -> bool:
        need_reload = False

        mtime = os.stat(filename).st_mtime
        old_time = mtimes.get(filename)
        if old_time is None:
            mtimes[filename] = mtime
        elif mtime > old_time:
            mtimes[filename] = mtime
            need_reload = True

        return need_reload
