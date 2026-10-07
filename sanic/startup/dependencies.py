"""监听器依赖声明、校验与有序执行。

维护者通过 :meth:`sanic.Sanic.listener` 的 ``name`` / ``depends`` /
``rollback`` / ``on_failure`` 参数声明启动监听器之间的前置依赖和失败策略。

- 启动前（``_startup``）校验依赖是否缺失、是否成环、同名监听器是否冲突；
- 按依赖拓扑序执行，互不约束的节点保持注册顺序（稳定排序），因此
  未声明任何依赖的监听器行为与旧版本完全一致；
- 某一步失败时，已完成且声明了 ``rollback`` 的步骤按完成顺序的逆序撤销，
  随后原样抛出导致失败的异常；
- 计划是注册声明的纯函数：多应用共用同一监听函数、工作进程反复重启、
  同一函数重复注册，都会得到相同的执行/回滚顺序。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Callable, NamedTuple, Sequence

from sanic.exceptions import SanicException
from sanic.log import error_logger
from sanic.server.events import trigger_events


if TYPE_CHECKING:
    from sanic.app import Sanic


ListenerType = Callable[..., Any]
RollbackType = Callable[..., Any]


class ListenerFailurePolicy(str, Enum):
    """单步监听器失败时的处理策略。"""

    #: 立即停止后续步骤，并撤销已经完成的可回滚步骤（默认）。
    ABORT = "abort"
    #: 跳过本步骤及其后续依赖，继续执行不依赖它的其它步骤。
    CONTINUE = "continue"


class ListenerDependencyError(SanicException):
    """依赖关系声明非法的基类。"""


class MissingListenerDependency(ListenerDependencyError):
    """声明依赖了一个当前事件中不存在的监听器名称。"""

    def __init__(self, event: str, listener: str, dependency: str) -> None:
        super().__init__(
            f"Listener {listener!r} for event {event!r} depends on "
            f"{dependency!r}, but no listener with that name is registered "
            f"for event {event!r}."
        )


class CyclicListenerDependency(ListenerDependencyError):
    """监听器依赖关系中存在环。"""

    def __init__(self, event: str, cycle: Sequence[str]) -> None:
        chain = " -> ".join([*cycle, cycle[0]])
        super().__init__(
            f"Cyclic listener dependency detected for event {event!r}: "
            f"{chain}."
        )


class DuplicateListenerName(ListenerDependencyError):
    """同一事件中两个不同监听器声明显式同名。"""

    def __init__(self, event: str, name: str) -> None:
        super().__init__(
            f"Duplicate listener name {name!r} for event {event!r}: "
            "distinct listener functions cannot share an explicit name."
        )


class ListenerStep(NamedTuple):
    """拓扑计划中的一个可执行步骤。"""

    key: str
    name: str
    listener: ListenerType
    depends: frozenset[str]
    rollback: RollbackType | None
    on_failure: ListenerFailurePolicy
    priority: int
    seq: int


def listener_name(listener: ListenerType) -> str:
    """推导监听器的默认名称。

    优先使用函数自身（或被 ``functools.partial`` 包装函数）的限定名，
    保证多应用共用同一个监听函数时推导出同一个名称。
    """
    func = getattr(listener, "func", None) or listener
    return (
        getattr(func, "__qualname__", None)
        or getattr(func, "__name__", None)
        or repr(listener)
    )


@dataclass
class _Declaration:
    """一次 ``register_listener`` 调用留下的原始声明。"""

    seq: int
    listener: ListenerType
    explicit_name: str | None
    depends: frozenset[str]
    rollback: RollbackType | None
    on_failure: ListenerFailurePolicy
    priority: int

    @property
    def name(self) -> str:
        return self.explicit_name or listener_name(self.listener)


def topological_order(
    event: str,
    nodes: Sequence[ListenerStep],
    name_to_key: dict[str, str],
) -> list[ListenerStep]:
    """稳定 Kahn 拓扑排序。

    依赖边强制先后；互不约束的节点按 ``(priority 降序, 注册序号升序)``
    出队，priority 缺省（均为 0）时即严格注册顺序。
    """
    by_key = {node.key: node for node in nodes}
    dependents: dict[str, set[str]] = {node.key: set() for node in nodes}
    indegree: dict[str, int] = {node.key: 0 for node in nodes}

    for node in nodes:
        for dep_name in node.depends:
            dep_key = name_to_key.get(dep_name)
            if dep_key is None:
                # 依赖目标不存在，或目标未显式纳入编排：
                # 统一在启动前报错，提示目标需要显式声明 name。
                raise MissingListenerDependency(event, node.name, dep_name)
            dependents[dep_key].add(node.key)
            indegree[node.key] += 1

    ready = sorted(
        (key for key, deg in indegree.items() if deg == 0),
        key=lambda key: (-by_key[key].priority, by_key[key].seq),
    )
    ordered: list[ListenerStep] = []
    while ready:
        key = ready.pop(0)
        ordered.append(by_key[key])
        unlocked: list[str] = []
        for dependent in dependents[key]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                unlocked.append(dependent)
        if unlocked:
            ready.extend(unlocked)
            ready.sort(key=lambda k: (-by_key[k].priority, by_key[k].seq))

    if len(ordered) != len(nodes):
        unresolved = {key for key, deg in indegree.items() if deg > 0}
        cycle = _find_cycle(by_key, unresolved)
        raise CyclicListenerDependency(
            event,
            cycle or [by_key[key].name for key in sorted(unresolved)],
        )

    return ordered


def _find_cycle(
    by_key: dict[str, ListenerStep],
    candidates: set[str],
) -> list[str] | None:
    """在候选节点子图中沿依赖方向找出一条具体的环。"""
    stack: list[str] = []
    on_stack: set[str] = set()

    def visit(key: str) -> list[str] | None:
        stack.append(key)
        on_stack.add(key)
        node = by_key[key]
        dep_keys = sorted(
            (
                dep_key
                for dep_key in candidates
                if by_key[dep_key].name in node.depends
            ),
            key=lambda k: by_key[k].seq,
        )
        for dep_key in dep_keys:
            if dep_key in on_stack:
                start = stack.index(dep_key)
                return stack[start:]
            found = visit(dep_key)
            if found:
                return found
        stack.pop()
        on_stack.remove(key)
        return None

    for key in sorted(candidates, key=lambda k: by_key[k].seq):
        found = visit(key)
        if found:
            return [by_key[k].name for k in found]
    return None


class ListenerOrchestrator:
    """每个应用一份：收集声明、构建计划并执行/回滚。"""

    def __init__(self, app: Sanic) -> None:
        self.app = app
        #: event -> name -> 合并后的声明
        self._declarations: dict[str, dict[str, _Declaration]] = {}
        #: event -> set(name)：默认限定名撞车的受管监听器，启动前报错
        self._auto_name_collisions: dict[str, set[str]] = {}
        self._plans: dict[str, list[ListenerStep]] = {}
        #: 最近一次执行中已完成的受管步骤，供分段触发时回滚使用
        self._finished: dict[str, list[ListenerStep]] = {}

    def register(
        self,
        event: str,
        listener: ListenerType,
        *,
        name: str | None = None,
        depends: Sequence[str] | set[str] | None = None,
        rollback: RollbackType | None = None,
        on_failure: str | ListenerFailurePolicy = ListenerFailurePolicy.ABORT,
        priority: int = 0,
    ) -> str:
        """登记一个受管监听器，返回它在依赖图中的名称。

        只有显式提供了 ``name`` / ``depends`` / ``rollback`` 的监听器才会
        进入这里；普通监听器由调用方留在原有执行通道。同一名称重复注册
        会幂等合并（依赖取并集、回滚取非空值），保证重复注册结果一致。
        """
        try:
            policy = (
                on_failure
                if isinstance(on_failure, ListenerFailurePolicy)
                else ListenerFailurePolicy(str(on_failure))
            )
        except ValueError:
            allowed = ", ".join(p.value for p in ListenerFailurePolicy)
            raise ListenerDependencyError(
                f"Invalid on_failure {on_failure!r}. Use one of: {allowed}."
            ) from None

        step_name = name or listener_name(listener)
        by_name = self._declarations.setdefault(event, {})
        new_depends = frozenset(depends or ())
        if step_name not in by_name:
            by_name[step_name] = _Declaration(
                seq=len(by_name),
                listener=listener,
                explicit_name=name,
                depends=new_depends,
                rollback=rollback,
                on_failure=policy,
                priority=priority,
            )
        else:
            first = by_name[step_name]
            if not _same_listener(first.listener, listener):
                if name is not None:
                    raise DuplicateListenerName(event, step_name)
                # 默认限定名相同但并非同一函数（如两个模块中的同名函数）：
                # 延迟到 _build 阶段，在启动前给出明确错误，避免静默丢步骤。
                self._auto_name_collisions.setdefault(event, set()).add(
                    step_name
                )
            # 重复注册同一函数：幂等合并，保留首次注册序号以稳定顺序。
            by_name[step_name] = _Declaration(
                seq=first.seq,
                listener=first.listener,
                explicit_name=first.explicit_name,
                depends=first.depends | new_depends,
                rollback=rollback or first.rollback,
                on_failure=policy,
                priority=priority or first.priority,
            )
        self._plans.pop(event, None)
        return step_name

    def has(self, event: str) -> bool:
        return bool(self._declarations.get(event))

    def validate(self) -> None:
        """启动前校验所有已登记事件的依赖完整性与无环性。"""
        for event in sorted(self._declarations):
            self.plan(event)

    def plan(self, event: str) -> list[ListenerStep]:
        if event not in self._plans:
            self._plans[event] = self._build(event)
        return self._plans[event]

    def _build(self, event: str) -> list[ListenerStep]:
        by_name = self._declarations.get(event, {})
        collisions = self._auto_name_collisions.get(event)
        if collisions:
            # 启动前明确报错：不同函数默认限定名相同且都受管，
            # 若静默合并会丢失其中一个监听器。
            raise DuplicateListenerName(event, sorted(collisions)[0])
        nodes: list[ListenerStep] = [
            ListenerStep(
                key=name,
                name=name,
                listener=decl.listener,
                depends=decl.depends,
                rollback=decl.rollback,
                on_failure=decl.on_failure,
                priority=decl.priority,
                seq=decl.seq,
            )
            for name, decl in by_name.items()
        ]
        name_to_key = {node.name: node.key for node in nodes}
        return topological_order(event, nodes, name_to_key)

    async def run_managed(
        self,
        event: str,
        app: Sanic,
        loop: Any | None = None,
        *,
        reverse: bool = False,
        **kwargs: Any,
    ) -> None:
        """按依赖计划执行某事件的受管监听器。

        计划是注册声明的纯函数，因此同一应用被工作进程反复重启时，
        每次触发的执行顺序和回滚顺序都完全一致。``reverse`` 用于停机类
        事件：按启动顺序的逆序执行。未声明依赖的普通监听器不在这里执行，
        由调用方沿原有信号/``trigger_events`` 通道触发。
        """
        if not self.has(event):
            return
        order = list(self.plan(event))
        if reverse:
            order.reverse()
        finished: list[ListenerStep] = []
        skipped: set[str] = set()
        # 预先登记，使执行中途失败时外部也能幂等地感知/清理已完成步骤。
        self._finished[event] = finished

        for step in order:
            if not reverse and step.depends & skipped:
                skipped.add(step.name)
                error_logger.warning(
                    "Skipping listener %r for %r because an upstream "
                    "dependency was skipped.",
                    step.name,
                    event,
                )
                continue
            try:
                await _invoke(step.listener, app, loop, kwargs)
            except BaseException as exc:  # noqa: BLE001 - 统一编排失败
                if (
                    not reverse
                    and step.on_failure is ListenerFailurePolicy.CONTINUE
                ):
                    error_logger.warning(
                        "Listener %r for %r failed but is marked "
                        "on_failure='continue'; its dependents will be "
                        "skipped: %r",
                        step.name,
                        event,
                        exc,
                    )
                    skipped.add(step.name)
                    continue
                if not reverse:
                    await self._rollback(finished, app, loop, kwargs)
                    # 内部已回滚，清空记录避免外部 rollback_event 重复执行。
                    self._finished[event] = []
                raise

            finished.append(step)

    async def rollback_event(
        self,
        event: str,
        app: Sanic,
        loop: Any | None = None,
        **kwargs: Any,
    ) -> None:
        """撤销最近一次该事件执行中已完成的可回滚步骤（逆序）。

        执行后清空记录，因此重复调用（或失败步骤内部已回滚过）是幂等的。
        """
        finished = self._finished.pop(event, [])
        await self._rollback(finished, app, loop, kwargs)

    async def _rollback(
        self,
        finished: Sequence[ListenerStep],
        app: Sanic,
        loop: Any | None,
        kwargs: dict[str, Any],
    ) -> None:
        """按完成顺序的逆序撤销已完成的可回滚步骤。"""
        for step in reversed(finished):
            if step.rollback is None:
                continue
            try:
                await _invoke(step.rollback, app, loop, kwargs)
            except Exception as exc:  # noqa: BLE001
                error_logger.error(
                    "Rollback for listener %r failed: %r", step.name, exc
                )


async def trigger_listeners(
    listeners: Sequence[ListenerType],
    app: Sanic,
    loop: Any | None,
    **kwargs: Any,
) -> None:
    """``server.events.trigger_events`` 的可等待版本，语义保持一致。

    供“受管计划 + 普通监听器”在同一个事件循环内顺序执行使用；按给定
    顺序逐个调用，普通监听器的失败直接向上抛出，由调用方触发受管步骤
    的回滚。
    """
    for listener in listeners:
        try:
            result = listener(app, **kwargs)
        except TypeError:
            result = listener(app, loop, **kwargs)
        if isawaitable(result):
            await result


def run_process_event(
    app: Sanic,
    event: str,
    loop: Any,
    *,
    reverse: bool = False,
    **kwargs: Any,
) -> None:
    """进程类事件的同步触发入口（``trigger_events`` 链路）。

    从 ``app.listeners`` 取出未声明依赖的普通监听器，与受管计划在
    ``loop`` 中执行：

    - 启动类（``reverse=False``）：受管计划先执行，普通监听器随后；
    - 停机类（``reverse=True``）：普通监听器按原注册顺序执行（保持
      既有规则不变），受管计划按拓扑序逆序执行。

    若应用上没有真实的编排器（例如测试中使用的 Mock 对象），
    完全退化为 :func:`sanic.server.events.trigger_events` 的原有行为。
    """
    orchestrator = getattr(app, "listener_orchestrator", None)
    legacy = list(app.listeners.get(event, ()))

    if not isinstance(orchestrator, ListenerOrchestrator):
        trigger_events(legacy, loop, app, **kwargs)
        return

    async def _runner() -> None:
        if reverse:
            # 普通监听器保持既有规则：注册顺序，先于受管步骤执行。
            await trigger_listeners(legacy, app, loop, **kwargs)
            await orchestrator.run_managed(
                event, app, loop, reverse=True, **kwargs
            )
        else:
            await orchestrator.run_managed(event, app, loop, **kwargs)
            try:
                await trigger_listeners(legacy, app, loop, **kwargs)
            except BaseException:
                await orchestrator.rollback_event(event, app, loop, **kwargs)
                raise

    loop.run_until_complete(_runner())


def _same_listener(first: ListenerType, other: ListenerType) -> bool:
    """判断两次注册是否为同一个监听函数（穿透 partial 包装）。"""
    return getattr(first, "func", first) is getattr(
        other, "func", other
    ) and getattr(first, "args", ()) == getattr(other, "args", ())


async def _invoke(
    func: Callable[..., Any],
    app: Sanic,
    loop: Any | None,
    kwargs: dict[str, Any],
) -> Any:
    """兼容新旧两种监听器签名。

    新签名为 ``listener(app, **kwargs)``，旧式为
    ``listener(app, loop, **kwargs)``。
    """
    try:
        result = func(app, **kwargs)
    except TypeError:
        result = func(app, loop, **kwargs)
    if isawaitable(result):
        result = await result
    return result
