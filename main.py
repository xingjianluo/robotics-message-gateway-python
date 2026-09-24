"""本地启动入口。

保持原有使用方式（``uvicorn main:app`` / ``python -m main``）不变。
"""

from fastapi import FastAPI

from gateway.app import app as gateway_app

# 网关本身即应用：根路径返回服务索引（见 gateway.app.root）
app: FastAPI = gateway_app


@app.get("/health")
def health() -> dict[str, int]:
    return {"ok": 1}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
