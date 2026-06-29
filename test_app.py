import logging
from fastapi import FastAPI
from starlette.requests import Request
from pydantic import BaseModel
import sprite
from sprite import SpriteMiddleware, capture_error, track

logger = logging.getLogger(__name__)

sprite.init(api_key="test-key", version="1.0.0", environment="development")
app = FastAPI()


async def get_user_id(request: Request) -> str | None:
    # AI Agent가 실제 앱에 맞게 여기를 채워줌
    # 예: JWT 디코딩, 세션 조회 등
    return request.headers.get("x-user-id")


app.add_middleware(
    SpriteMiddleware,
    api_key="test-key",
    get_user_id=get_user_id,
)


@app.get("/api/users")
async def get_users():
    return [{"id": 1, "name": "홍길동"}]


@app.get("/api/payment")
async def payment():
    track("payment_viewed", {"amount": 1000})
    try:
        result = {"status": "success", "amount": 1000}
        track("payment_completed", {"amount": 1000})
        return result
    except Exception as e:
        capture_error(e, {"amount": 1000})
        return {"status": "failed"}


@app.get("/api/error")
async def error_endpoint():
    logger.error("결제 처리 중 심각한 오류 발생", extra={"user_id": "user_123"})
    raise ValueError("결제 처리 중 오류 발생")


class PaymentRequest(BaseModel):
    amount: int
    currency: str

@app.post("/api/payment")
async def create_payment(body: PaymentRequest):
    return {"status": "success", "amount": body.amount}
