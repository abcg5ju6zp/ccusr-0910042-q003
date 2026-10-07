from __future__ import annotations

from enum import Enum, auto
from functools import partial
from typing import Callable, cast, overload

from sanic.base.meta import SanicMeta
from sanic.exceptions import BadRequest
from sanic.models.futures import FutureListener
from sanic.models.handler_types import ListenerType, Sanic
from sanic.startup.dependencies import ListenerFailurePolicy


class ListenerEvent(str, Enum):
    def _generate_next_value_(name: str, *args) -> str:  # type: ignore
        return name.lower()

    BEFORE_SERVER_START = "server.init.before"
    AFTER_SERVER_START = "server.init.after"
    BEFORE_SERVER_STOP = "server.shutdown.before"
    AFTER_SERVER_STOP = "server.shutdown.after"
    MAIN_PROCESS_START = auto()
    MAIN_PROCESS_READY = auto()
    MAIN_PROCESS_STOP = auto()
    RELOAD_PROCESS_START = auto()
    RELOAD_PROCESS_STOP = auto()
    BEFORE_RELOAD_TRIGGER = auto()
    AFTER_RELOAD_TRIGGER = auto()


class ListenerMixin(metaclass=SanicMeta):
    def __init__(self, *args, **kwargs) -> None:
        self._future_listeners: list[FutureListener] = []

    def _apply_listener(self, listener: FutureListener):
        raise NotImplementedError  # noqa

    @overload
    def listener(
        self,
        listener_or_event: ListenerType[Sanic],
        event_or_none: str,
        apply: bool = ...,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ...,
    ) -> ListenerType[Sanic]: ...

    @overload
    def listener(
        self,
        listener_or_event: str,
        event_or_none: None = ...,
        apply: bool = ...,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ...,
    ) -> Callable[[ListenerType[Sanic]], ListenerType[Sanic]]: ...

    def listener(
        self,
        listener_or_event: ListenerType[Sanic] | str,
        event_or_none: str | None = None,
        apply: bool = True,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> (
        ListenerType[Sanic]
        | Callable[[ListenerType[Sanic]], ListenerType[Sanic]]
    ):
        """注册生命周期事件监听器。

        除既有的 ``priority`` 外，还可声明监听器之间的编排关系：

        :param name: 监听器在依赖图中的唯一名称，供其它监听器在
            ``depends`` 中引用；缺省时取函数的限定名。
        :param depends: 必须先于本监听器完成的前置监听器名称。注册阶段
            不校验，统一在启动前检查缺失或循环依赖。
        :param rollback: 本监听器成功后若后续步骤失败，用于撤销已完成
            工作的回调；按完成顺序的逆序调用。
        :param on_failure: 本监听器失败时的策略，``"abort"``（默认，
            停止并回滚）或 ``"continue"``（跳过它及其依赖者继续执行）。
        """

        def register_listener(
            listener: ListenerType[Sanic], event: str, priority: int = 0
        ) -> ListenerType[Sanic]:
            """项目内部接口说明。"""
            nonlocal apply

            future_listener = FutureListener(
                listener,
                event,
                priority,
                name=name,
                depends=frozenset(depends or ()),
                rollback=rollback,
                on_failure=(
                    on_failure
                    if isinstance(on_failure, str)
                    else on_failure.value
                ),
            )
            self._future_listeners.append(future_listener)
            if apply:
                self._apply_listener(future_listener)
            return listener

        if callable(listener_or_event):
            if event_or_none is None:
                raise BadRequest(
                    "Invalid event registration: Missing event name."
                )
            return register_listener(
                listener_or_event, event_or_none, priority
            )
        else:
            return partial(
                register_listener,
                event=listener_or_event,
                priority=priority,
            )

    def _setup_listener(
        self,
        listener: ListenerType[Sanic] | None,
        event: str,
        priority: int,
        *,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        if listener is not None:
            return self.listener(
                listener,
                event,
                priority=priority,
                name=name,
                depends=depends,
                rollback=rollback,
                on_failure=on_failure,
            )
        return cast(
            ListenerType[Sanic],
            partial(
                self.listener,
                event_or_none=event,
                priority=priority,
                name=name,
                depends=depends,
                rollback=rollback,
                on_failure=on_failure,
            ),
        )

    def main_process_start(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "main_process_start",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def main_process_ready(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "main_process_ready",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def main_process_stop(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "main_process_stop",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def reload_process_start(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "reload_process_start",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def reload_process_stop(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "reload_process_stop",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def before_reload_trigger(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "before_reload_trigger",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def after_reload_trigger(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "after_reload_trigger",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def before_server_start(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "before_server_start",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def after_server_start(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "after_server_start",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def before_server_stop(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "before_server_stop",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )

    def after_server_stop(
        self,
        listener: ListenerType[Sanic] | None = None,
        *,
        priority: int = 0,
        name: str | None = None,
        depends: tuple[str, ...] | list[str] | set[str] | None = None,
        rollback: Callable[..., object] | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
    ) -> ListenerType[Sanic]:
        """项目内部接口说明。"""
        return self._setup_listener(
            listener,
            "after_server_stop",
            priority,
            name=name,
            depends=depends,
            rollback=rollback,
            on_failure=on_failure,
        )
