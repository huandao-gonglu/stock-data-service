from __future__ import annotations


class BaiduAuthorizationRequired(RuntimeError):
    """Raised when a command requires a manual Baidu authorization flow."""


class BaiduDesktopAuthorizer:
    """Placeholder for a future interactive authorization flow.

    The MVP deliberately keeps sync as CLI/cron and unit tests free of desktop
    authorization side effects.
    """

    def authorize(self) -> dict:
        raise BaiduAuthorizationRequired("interactive Baidu authorization is not implemented in the MVP")
