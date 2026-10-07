"""监听器依赖声明、校验、稳定排序与失败回滚的回归测试。"""

from __future__ import annotations

import asyncio

import pytest

from sanic import Blueprint, Sanic
from sanic.response import text
from sanic.startup.dependencies import (
    CyclicListenerDependency,
    DuplicateListenerName,
    ListenerDependencyError,
    ListenerFailurePolicy,
    MissingListenerDependency,
    run_process_event,
)


def _new_app(name: str = "dep-test") -> Sanic:
    app = Sanic(name)
    app.add_route(lambda request: text("ok"), "/")
    return app


async def _init_before(app: Sanic):
    loop = asyncio.get_running_loop()
    await app._startup()
    await app._server_event("init", "before", loop=loop)


# --------------------------------------------------------------------- #
# 排序
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_topological_order_respects_dependencies():
    app = _new_app("order")
    output: list[str] = []

    app.register_listener(
        lambda app: output.append("auth"),
        "before_server_start",
        name="auth",
    )
    app.register_listener(
        lambda app: output.append("risk"),
        "before_server_start",
        name="risk",
        depends=("warmup",),
    )
    app.register_listener(
        lambda app: output.append("warmup"),
        "before_server_start",
        name="warmup",
        depends=("auth",),
    )

    await _init_before(app)
    assert output == ["auth", "warmup", "risk"]


@pytest.mark.asyncio
async def test_unordered_nodes_keep_registration_order():
    app = _new_app("stable")
    output: list[str] = []
    for letter in ("a", "b", "c", "d"):
        app.register_listener(
            lambda app, letter=letter: output.append(letter),
            "before_server_start",
            name=f"step-{letter}",
        )

    await _init_before(app)
    assert output == ["a", "b", "c", "d"]


@pytest.mark.asyncio
async def test_decorator_form_registers_dependencies():
    app = _new_app("decorator")
    output: list[str] = []

    @app.before_server_start(name="auth")
    async def auth(app):
        output.append("auth")

    @app.before_server_start(name="risk", depends=("auth",))
    async def risk(app):
        output.append("risk")

    await _init_before(app)
    assert output == ["auth", "risk"]


@pytest.mark.parametrize(
    "method_name,event",
    (
        ("main_process_start", "main_process_start"),
        ("main_process_ready", "main_process_ready"),
        ("reload_process_start", "reload_process_start"),
        ("after_server_start", "server.init.after"),
        ("before_server_stop", "server.shutdown.before"),
    ),
)
def test_keyword_decorator_form_supported_on_all_convenience_methods(
    method_name, event
):
    app = _new_app(f"kw-{method_name}")
    method = getattr(app, method_name)

    @method(name="gated")
    def listener(app): ...

    assert app.listener_orchestrator.has(event)
    assert [s.name for s in app.listener_orchestrator.plan(event)] == ["gated"]


@pytest.mark.asyncio
async def test_diamond_dependency_is_stable():
    app = _new_app("diamond")
    output: list[str] = []

    app.register_listener(
        lambda app: output.append("root"),
        "before_server_start",
        name="root",
    )
    app.register_listener(
        lambda app: output.append("left"),
        "before_server_start",
        name="left",
        depends=("root",),
    )
    app.register_listener(
        lambda app: output.append("right"),
        "before_server_start",
        name="right",
        depends=("root",),
    )
    app.register_listener(
        lambda app: output.append("leaf"),
        "before_server_start",
        name="leaf",
        depends=("left", "right"),
    )

    await _init_before(app)
    assert output == ["root", "left", "right", "leaf"]


# --------------------------------------------------------------------- #
# 启动前校验：缺失 / 循环 / 重名
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_missing_dependency_fails_before_serving():
    app = _new_app("missing")
    app.register_listener(
        lambda app: None,
        "before_server_start",
        name="risk",
        depends=("not-registered",),
    )
    with pytest.raises(MissingListenerDependency):
        await app._startup()


@pytest.mark.asyncio
async def test_cyclic_dependency_fails_before_serving():
    app = _new_app("cyclic")
    app.register_listener(
        lambda app: None,
        "before_server_start",
        name="a",
        depends=("b",),
    )
    app.register_listener(
        lambda app: None,
        "before_server_start",
        name="b",
        depends=("a",),
    )
    with pytest.raises(CyclicListenerDependency) as exc_info:
        await app._startup()
    message = str(exc_info.value)
    assert "a" in message and "b" in message


@pytest.mark.asyncio
async def test_self_dependency_is_a_cycle():
    app = _new_app("self-dep")
    app.register_listener(
        lambda app: None,
        "before_server_start",
        name="solo",
        depends=("solo",),
    )
    with pytest.raises(CyclicListenerDependency):
        await app._startup()


@pytest.mark.asyncio
async def test_duplicate_explicit_name_distinct_functions_rejected():
    app = _new_app("dup-name")

    async def first(app): ...

    async def second(app): ...

    app.register_listener(first, "before_server_start", name="shared")
    with pytest.raises(DuplicateListenerName):
        app.register_listener(second, "before_server_start", name="shared")


@pytest.mark.asyncio
async def test_duplicate_auto_name_distinct_functions_rejected():
    app = _new_app("dup-auto-name")

    def make_listener():
        def same_name(app): ...

        return same_name

    # 两个不同函数具有相同限定名，且都通过 rollback 纳入编排。
    app.register_listener(
        make_listener(),
        "before_server_start",
        rollback=lambda app: None,
    )
    app.register_listener(
        make_listener(),
        "before_server_start",
        rollback=lambda app: None,
    )
    with pytest.raises(DuplicateListenerName):
        await app._startup()


# --------------------------------------------------------------------- #
# 失败策略与回滚
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_failure_rolls_back_completed_steps_in_reverse():
    app = _new_app("rollback")
    output: list[str] = []

    app.register_listener(
        lambda app: output.append("auth"),
        "before_server_start",
        name="auth",
        rollback=lambda app: output.append("auth-rollback"),
    )
    app.register_listener(
        lambda app: output.append("warmup"),
        "before_server_start",
        name="warmup",
        depends=("auth",),
        rollback=lambda app: output.append("warmup-rollback"),
    )

    def boom(app):
        output.append("risk")
        raise RuntimeError("risk not ready")

    app.register_listener(
        boom,
        "before_server_start",
        name="risk",
        depends=("warmup",),
    )

    with pytest.raises(RuntimeError, match="risk not ready"):
        await _init_before(app)

    assert output == [
        "auth",
        "warmup",
        "risk",
        "warmup-rollback",
        "auth-rollback",
    ]


@pytest.mark.asyncio
async def test_rollback_failure_does_not_mask_original():
    app = _new_app("rollback-fails")
    output: list[str] = []

    app.register_listener(
        lambda app: output.append("auth"),
        "before_server_start",
        name="auth",
        rollback=lambda app: (_ for _ in ()).throw(RuntimeError("rb boom")),
    )

    def boom(app):
        raise RuntimeError("original")

    app.register_listener(
        boom,
        "before_server_start",
        name="risk",
        depends=("auth",),
    )

    with pytest.raises(RuntimeError, match="original"):
        await _init_before(app)
    assert output == ["auth"]


@pytest.mark.asyncio
async def test_continue_policy_skips_dependents_and_proceeds():
    app = _new_app("continue")
    output: list[str] = []

    @app.before_server_start
    async def plain(app):
        output.append("plain")

    @app.before_server_start(
        name="flaky", on_failure=ListenerFailurePolicy.CONTINUE
    )
    async def flaky(app):
        output.append("flaky")
        raise RuntimeError("flaky down")

    @app.before_server_start(name="dependent", depends=("flaky",))
    async def dependent(app):
        output.append("dependent")

    @app.before_server_start(name="independent")
    async def independent(app):
        output.append("independent")

    await _init_before(app)
    # flaky 失败但 continue：dependent 被跳过，independent 继续；
    # 未声明依赖的普通监听器按原规则执行。
    assert output == ["flaky", "independent", "plain"]
    assert "dependent" not in output


def test_invalid_failure_policy_rejected():
    app = _new_app("bad-policy")
    with pytest.raises(ListenerDependencyError):
        app.register_listener(
            lambda app: None,
            "before_server_start",
            name="x",
            on_failure="explode",
        )


# --------------------------------------------------------------------- #
# 与普通监听器混用 / 旧规则兼容
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_legacy_listeners_still_follow_registration_order():
    app = _new_app("legacy")
    output: list[str] = []

    @app.before_server_start
    async def one(app):
        output.append("one")

    @app.before_server_start
    async def two(app):
        output.append("two")

    await _init_before(app)
    assert output == ["one", "two"]
    # 普通监听器不进入编排，仍登记在 app.listeners 的信号通道中。
    assert not app.listener_orchestrator.has("server.init.before")


@pytest.mark.asyncio
async def test_managed_steps_run_before_legacy_listeners():
    app = _new_app("mixed")
    output: list[str] = []

    @app.before_server_start
    async def legacy(app):
        output.append("legacy")

    @app.before_server_start(name="gated")
    async def gated(app):
        output.append("gated")

    await _init_before(app)
    assert output == ["gated", "legacy"]


@pytest.mark.asyncio
async def test_blueprint_managed_listener_registered_once():
    app = _new_app("bp-managed")
    bp = Blueprint("bp")
    output: list[str] = []

    @bp.before_server_start(name="bp-auth")
    async def bp_auth(app):
        output.append("bp-auth")

    app.blueprint(bp)
    await _init_before(app)
    assert output == ["bp-auth"]
    # 不能同时出现在普通信号监听通道中
    wrapped = [
        route
        for route in app.signal_router.name_index.get("server.init.before", [])
    ]
    assert not wrapped or all(
        "bp_auth" not in repr(getattr(r, "handler", r)) for r in wrapped
    )


# --------------------------------------------------------------------- #
# 幂等：重复注册 / 多应用共享 / 重启
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_duplicate_registration_is_idempotent():
    app = _new_app("dedupe")

    async def auth(app): ...

    async def warmup(app): ...

    app.register_listener(auth, "before_server_start", name="auth")
    app.register_listener(
        warmup, "before_server_start", name="warmup", depends=("auth",)
    )
    # 同一函数、同一名称再次注册
    app.register_listener(
        warmup, "before_server_start", name="warmup", depends=("auth",)
    )

    app.listener_orchestrator.validate()
    names = [
        step.name
        for step in app.listener_orchestrator.plan("server.init.before")
    ]
    assert names == ["auth", "warmup"]


@pytest.mark.asyncio
async def test_shared_function_across_apps_has_consistent_plan():
    async def shared_auth(app): ...

    async def shared_risk(app): ...

    plans = []
    for name in ("app-a", "app-b"):
        app = _new_app(name)
        app.register_listener(shared_auth, "before_server_start", name="auth")
        app.register_listener(
            shared_risk,
            "before_server_start",
            name="risk",
            depends=("auth",),
        )
        app.listener_orchestrator.validate()
        plans.append(
            [
                step.name
                for step in app.listener_orchestrator.plan(
                    "server.init.before"
                )
            ]
        )

    assert plans[0] == plans[1] == ["auth", "risk"]


@pytest.mark.asyncio
async def test_restart_reproduces_identical_order():
    app = _new_app("restart")
    output: list[str] = []

    app.register_listener(
        lambda app: output.append("auth"),
        "before_server_start",
        name="auth",
    )
    app.register_listener(
        lambda app: output.append("risk"),
        "before_server_start",
        name="risk",
        depends=("auth",),
    )

    loop = asyncio.get_running_loop()
    await app._startup()
    for _ in range(2):
        await app._server_event("init", "before", loop=loop)
    assert output == ["auth", "risk", "auth", "risk"]


# --------------------------------------------------------------------- #
# 进程类事件（main_process_* 等，同步 trigger_events 链路）
# --------------------------------------------------------------------- #


def test_process_event_runs_managed_then_legacy_sync():
    app = _new_app("proc")
    output: list[str] = []

    app.register_listener(
        lambda app: output.append("managed"),
        "main_process_ready",
        name="managed",
    )
    app.register_listener(
        lambda app: output.append("legacy"),
        "main_process_ready",
    )

    loop = asyncio.new_event_loop()
    try:
        run_process_event(app, "main_process_ready", loop)
    finally:
        loop.close()
    assert output == ["managed", "legacy"]


def test_process_event_failure_rolls_back():
    app = _new_app("proc-rb")
    output: list[str] = []

    app.register_listener(
        lambda app: output.append("auth"),
        "main_process_start",
        name="auth",
        rollback=lambda app: output.append("auth-rollback"),
    )
    app.register_listener(
        lambda app: (_ for _ in ()).throw(RuntimeError("down")),
        "main_process_start",
        name="risk",
        depends=("auth",),
    )

    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(RuntimeError, match="down"):
            run_process_event(app, "main_process_start", loop)
    finally:
        loop.close()
    assert output == ["auth", "auth-rollback"]


def test_process_event_passes_extra_kwargs():
    app = _new_app("proc-kwargs")
    seen: dict[str, object] = {}

    def listener(app, changed=None):
        seen["changed"] = changed

    app.register_listener(listener, "after_reload_trigger", name="with-kwargs")
    loop = asyncio.new_event_loop()
    try:
        run_process_event(app, "after_reload_trigger", loop, changed={"a.py"})
    finally:
        loop.close()
    assert seen["changed"] == {"a.py"}


def test_process_event_reverse_runs_legacy_first_then_managed_reverse():
    app = _new_app("proc-reverse")
    output: list[str] = []

    app.register_listener(
        lambda app: output.append("managed"),
        "main_process_stop",
        name="managed",
    )
    app.register_listener(
        lambda app: output.append("legacy"),
        "main_process_stop",
    )

    loop = asyncio.new_event_loop()
    try:
        run_process_event(app, "main_process_stop", loop, reverse=True)
    finally:
        loop.close()
    # 停机：普通监听器先执行，受管步骤随后（逆序）
    assert output == ["legacy", "managed"]


# --------------------------------------------------------------------- #
# 跨启动阶段回滚 / 停机顺序
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_after_start_failure_rolls_back_before_phase():
    app = _new_app("cross-phase")
    trace: list[str] = []

    app.register_listener(
        lambda app: trace.append("auth"),
        "before_server_start",
        name="auth",
        rollback=lambda app: trace.append("auth-rollback"),
    )
    app.register_listener(
        lambda app: (_ for _ in ()).throw(RuntimeError("after phase failed")),
        "after_server_start",
        name="final",
    )

    loop = asyncio.get_running_loop()
    await app._startup()
    await app._server_event("init", "before", loop=loop)
    with pytest.raises(RuntimeError, match="after phase failed"):
        await app._server_event("init", "after", loop=loop)

    # after 阶段失败也会撤销 before 阶段已完成的可回滚步骤。
    assert trace == ["auth", "auth-rollback"]


@pytest.mark.asyncio
async def test_shutdown_runs_managed_steps_in_reverse_topo_order():
    app = _new_app("shutdown-order")
    output: list[str] = []

    app.register_listener(
        lambda app: output.append("auth-stop"),
        "before_server_stop",
        name="auth",
    )
    app.register_listener(
        lambda app: output.append("risk-stop"),
        "before_server_stop",
        name="risk",
        depends=("auth",),
    )

    loop = asyncio.get_running_loop()
    await app._startup()
    await app._server_event("shutdown", "before", loop=loop)
    # 停机时受管步骤按拓扑序的逆序执行（最后启动的最先停止）。
    assert output == ["risk-stop", "auth-stop"]


# --------------------------------------------------------------------- #
# 全流程集成：create_server 真正启动工作进程
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_full_server_startup_runs_dependent_listeners(app: Sanic, port):
    order: list[str] = []

    @app.before_server_start(name="auth")
    async def auth(app):
        order.append("auth")

    @app.before_server_start(name="warmup", depends=("auth",))
    async def warmup(app):
        order.append("warmup")

    @app.before_server_start(name="risk", depends=("warmup",))
    async def risk(app):
        order.append("risk")

    srv = await app.create_server(
        debug=True, return_asyncio_server=True, port=port
    )
    await srv.startup()
    await srv.before_start()
    assert order == ["auth", "warmup", "risk"]
    srv.close()


@pytest.mark.asyncio
async def test_full_server_startup_aborts_before_serving(app: Sanic, port):
    @app.before_server_start(name="auth")
    async def auth(app): ...

    @app.before_server_start(name="risk", depends=("auth",))
    async def risk(app):
        raise RuntimeError("risk control plane unavailable")

    srv = await app.create_server(
        debug=True, return_asyncio_server=True, port=port
    )
    await srv.startup()
    with pytest.raises(RuntimeError, match="risk control plane"):
        await srv.before_start()
