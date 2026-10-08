"""
JWT Authentication router for FoodChain API.
Provides login, token validation, RBAC dependency, and audit logging.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Request, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES
from database import get_connection, pwd_context
from schemas import LoginRequest, TokenResponse

logger = logging.getLogger("foodchain.auth")

router = APIRouter(prefix="/api/auth", tags=["Authentication"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")
oauth2_scheme_optional = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)

# Normalize legacy role names to canonical role set
ROLE_ALIAS = {
    "distributer": "distributor",   # fix historical typo in DB
    "farmer": "producer",           # legacy alias
    "warehouse": "distributor",     # legacy alias
    "retailer": "distributor",      # legacy alias
}
VALID_ROLES = {"admin", "producer", "distributor", "consumer", "manager"}


def normalize_role(role: str) -> str:
    """Return canonical role name, handling legacy aliases."""
    r = (role or "").lower().strip()
    return ROLE_ALIAS.get(r, r)


def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """Decode JWT and return user dict. Raises 401 on failure."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        role = normalize_role(payload.get("role", ""))
        user_id = payload.get("uid")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        with get_connection() as conn:
            active = conn.execute(
                "SELECT is_active FROM users WHERE id = ? AND username = ?",
                (user_id, username),
            ).fetchone()
        if not active or not active["is_active"]:
            raise HTTPException(status_code=401, detail="Account is inactive")
        return {"username": username, "role": role, "user_id": user_id}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def get_current_user_optional(token: Optional[str] = Depends(oauth2_scheme_optional)) -> Optional[dict]:
    """Decode JWT if provided; returns None for unauthenticated requests."""
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        role = normalize_role(payload.get("role", ""))
        user_id = payload.get("uid")
        if username:
            return {"username": username, "role": role, "user_id": user_id}
    except JWTError:
        pass
    return None


def require_role(*roles):
    """
    Role-based access control dependency.
    Usage: Depends(require_role('admin', 'producer'))
    Supports legacy role aliases transparently.
    """
    normalized = {normalize_role(r) for r in roles}

    def checker(user: dict = Depends(get_current_user)):
        # Administrators can inspect and operate role workspaces from the
        # central Admin panel without granting those capabilities to others.
        current_role = normalize_role(user.get("role", ""))
        if current_role != "admin" and current_role not in normalized:
            logger.warning(
                "RBAC DENIED | user=%s | role=%s | required=%s",
                user.get("username"),
                user.get("role"),
                normalized,
            )
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user

    return checker


@router.post("/login", response_model=TokenResponse)
def login(request: LoginRequest, req: Request = None):
    """Authenticate user and return JWT token with role."""
    ip = req.client.host if req and req.client else "unknown"

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, username, password_hash, role, is_active FROM users WHERE username = ?",
            (request.username,),
        )
        user = cursor.fetchone()

    if not user or not user["is_active"] or not pwd_context.verify(request.password, user["password_hash"]):
        logger.warning("AUTH FAILURE | user=%s | ip=%s", request.username, ip)
        # Write audit log for failed login (no password logged)
        _write_audit_log("LOGIN", "FAILURE", request.username, None, None,
                         "Invalid credentials", ip)
        raise HTTPException(status_code=401, detail="Invalid credentials")

    canonical_role = normalize_role(user["role"])
    token = create_access_token({
        "sub": user["username"],
        "role": canonical_role,
        "uid": user["id"],
    })

    logger.info("AUTH SUCCESS | user=%s | role=%s | ip=%s", user["username"], canonical_role, ip)
    _write_audit_log("LOGIN", "SUCCESS", user["username"], canonical_role, None,
                     f"Login from {ip}", ip)

    return TokenResponse(
        access_token=token,
        role=canonical_role,
        username=user["username"],
    )


@router.get("/me")
def get_me(user: dict = Depends(get_current_user)):
    """Returns current user info — useful for frontend to verify token validity."""
    return {"username": user["username"], "role": user["role"]}


def _write_audit_log(event_type, result, user, role, batch_id, detail, ip):
    """Best-effort audit log write — does not raise on failure."""
    try:
        from audit_logger import log_event
        log_event(event_type, result, user=user, role=role,
                  batch_id=batch_id, detail=detail, ip_address=ip)
    except Exception as e:
        logger.error("Audit log write failed: %s", e)
