import time
import uuid
import json
import logging
import traceback
import functools
import httpx
from contextvars import ContextVar
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.websockets import WebSocket


SPRITE_ENDPOINT = "https://sprite-app-production-7715.up.railway.app/api/events"

_config: dict = {}
_ctx_trace_id: ContextVar[str] = ContextVar('trace_id', default=None)
_ctx_session_id: ContextVar[str] = ContextVar('session_id', default=None)
_ctx_user_id: ContextVar[str] = ContextVar('user_id', default=None)


def init(environment: str = "production", endpoint: str = SPRITE_ENDPOINT):
    global _config
    _config = {
        "environment": environment,
        "endpoint": endpoint,
    }
    _setup_log_handler(endpoint)


def _setup_log_handler(endpoint: str):
    handler = SpriteLogHandler(endpoint)
    handler.setLevel(logging.WARNING)  # WARNING 이상만 수집
    logging.getLogger().addHandler(handler)


class SpriteLogHandler(logging.Handler):
    def __init__(self, endpoint: str):
        super().__init__()
        self.endpoint = endpoint

    def emit(self, record: logging.LogRecord):
        _fire_and_forget(self._send(record))

    async def _send(self, record: logging.LogRecord):
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    self.endpoint,
                    json={
                        "platform": "fastapi",
                        "session_id": "server",
                        "type": "log",
                        "name": record.levelname,
                        "payload": {
                            "level": record.levelname,
                            "message": record.getMessage(),
                            "logger": record.name,
                            "version": _config.get("version"),
                            "environment": _config.get("environment"),
                        },
                    },
                    timeout=2.0,
                )
        except Exception:
            pass


class SpriteMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, endpoint: str = SPRITE_ENDPOINT, get_user_id=None):
        super().__init__(app)
        self.endpoint = endpoint
        self.get_user_id = get_user_id  # 유저 ID 추출 함수 (선택)

    async def dispatch(self, request: Request, call_next):
        # Sprite 이벤트 수신 경로는 후킹 안 함
        if request.url.path == "/events":
            return await call_next(request)

        trace_id = request.headers.get("x-trace-id", str(uuid.uuid4()))
        session_id = request.headers.get("x-session-id", trace_id)

        start = time.perf_counter()
        status_code = 500
        error_info = None

        validation_errors = None

        _ctx_trace_id.set(trace_id)
        _ctx_session_id.set(session_id)

        if self.get_user_id:
            try:
                user_id = await self.get_user_id(request)
                _ctx_user_id.set(user_id)
            except Exception:
                user_id = None
        else:
            user_id = None

        try:
            response = await call_next(request)
            status_code = response.status_code

            if status_code == 422:
                body = b""
                async for chunk in response.body_iterator:
                    body += chunk
                try:
                    validation_errors = json.loads(body).get("detail")
                except Exception:
                    pass
                response = Response(
                    content=body,
                    status_code=status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type,
                )
        except Exception as e:
            error_info = {"message": str(e), "type": type(e).__name__}
            raise
        finally:
            latency_ms = round((time.perf_counter() - start) * 1000, 2)


            event_type = "error" if (error_info or status_code >= 500 or status_code == 422) else "network"
            event_name = f"{request.method} {request.url.path}"

            payload = {
                "method": request.method,
                "path": str(request.url.path),
                "status_code": status_code,
                "latency_ms": latency_ms,
                "version": _config.get("version"),
                "environment": _config.get("environment"),
            }
            if error_info:
                payload["error"] = error_info
            if validation_errors:
                payload["validation_errors"] = validation_errors

            await self._send(
                session_id=session_id,
                trace_id=trace_id,
                user_id=user_id,
                type=event_type,
                name=event_name,
                payload=payload,
            )

        return response

    async def _send(self, **kwargs):
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    self.endpoint,
                    json={"platform": "fastapi", **kwargs},
                    timeout=2.0,
                )
        except Exception:
            pass


def _fire_and_forget(coro) -> None:
    import asyncio
    import threading
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(coro)
    except RuntimeError:
        def _run():
            try:
                asyncio.run(coro)
            except Exception:
                pass
        threading.Thread(target=_run, daemon=True).start()


def capture_error(error: Exception, extra: dict = {}):
    """try-catch 안에서 수동으로 에러 캡처 — sync/async 어디서든 호출 가능"""
    _fire_and_forget(_send_event(
        type="error",
        name=type(error).__name__,
        session_id=_ctx_session_id.get() or "unknown",
        trace_id=_ctx_trace_id.get(),
        user_id=_ctx_user_id.get(),
        payload={"message": str(error), "type": type(error).__name__, **extra},
    ))


def track(name: str, properties: dict = {}):
    """유저 행동 이벤트 — sync/async 어디서든 호출 가능"""
    _fire_and_forget(_send_event(
        type="track",
        name=name,
        session_id=_ctx_session_id.get() or "unknown",
        trace_id=_ctx_trace_id.get(),
        user_id=_ctx_user_id.get(),
        payload=properties,
    ))


def capture_db_errors(engine):
    """SQLAlchemy 엔진에 DB 에러 자동 수집 훅 등록 — init() 후 engine 생성 시점에 한 번만 호출"""
    try:
        from sqlalchemy import event as sa_event

        @sa_event.listens_for(engine, "handle_error")
        def _on_db_error(exception_context):
            err = exception_context.original_exception
            _fire_and_forget(_send_event(
                type="error",
                name=type(err).__name__,
                session_id=_ctx_session_id.get() or "server",
                trace_id=_ctx_trace_id.get(),
                user_id=_ctx_user_id.get(),
                payload={
                    "message": str(err),
                    "stack": traceback.format_exc(),
                    "component": "database",
                    "statement": str(exception_context.statement)[:500] if exception_context.statement else None,
                },
            ))

    except ImportError:
        pass


def task(fn):
    """Background task 래퍼 — 에러를 자동으로 캡처. 동기/비동기 모두 지원"""
    if asyncio_iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def async_wrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:
                _fire_and_forget(_send_event(
                    type="error",
                    name=type(e).__name__,
                    session_id="server",
                    trace_id=None,
                    user_id=None,
                    payload={"message": str(e), "stack": traceback.format_exc(), "task": fn.__name__},
                ))
                raise
        return async_wrapper
    else:
        @functools.wraps(fn)
        def sync_wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                _fire_and_forget(_send_event(
                    type="error",
                    name=type(e).__name__,
                    session_id="server",
                    trace_id=None,
                    user_id=None,
                    payload={"message": str(e), "stack": traceback.format_exc(), "task": fn.__name__},
                ))
                raise
        return sync_wrapper


def asyncio_iscoroutinefunction(fn):
    import asyncio
    return asyncio.iscoroutinefunction(fn)


async def capture_websocket_errors(websocket: WebSocket, handler):
    """WebSocket 핸들러를 감싸서 에러 자동 캡처 — AI Agent가 ws 엔드포인트에 심음

    사용법:
        @app.websocket("/ws")
        async def ws_endpoint(ws: WebSocket):
            await sprite.capture_websocket_errors(ws, _handle)

        async def _handle(ws):
            await ws.accept()
            ...
    """
    try:
        await handler(websocket)
    except Exception as e:
        import asyncio
        asyncio.create_task(_send_event(
            type="error",
            name=type(e).__name__,
            session_id=_ctx_session_id.get() or "server",
            trace_id=_ctx_trace_id.get(),
            user_id=_ctx_user_id.get(),
            payload={"message": str(e), "stack": traceback.format_exc(), "component": "websocket"},
        ))
        raise


async def _send_event(type: str, name: str, session_id: str, payload: dict, trace_id: str = None, user_id: str = None):
    endpoint = _config.get("endpoint", SPRITE_ENDPOINT)
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                endpoint,
                json={
                    "platform": "fastapi",
                    "session_id": session_id,
                    "trace_id": trace_id,
                    "user_id": user_id,
                    "type": type,
                    "name": name,
                    "payload": {
                        **payload,
                        "version": _config.get("version"),
                        "environment": _config.get("environment"),
                    },
                },
                timeout=2.0,
            )
    except Exception:
        pass
