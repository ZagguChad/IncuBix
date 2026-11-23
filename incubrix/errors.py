from fastapi import HTTPException


class ServiceError(HTTPException):
    def __init__(self, status_code: int, code: str, message: str, retryable: bool = False, details: dict | None = None):
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details or {}
        super().__init__(status_code=status_code, detail=message)
