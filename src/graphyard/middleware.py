"""Response policy for private inventory and health diagnostics, including errors."""

from django.utils.cache import patch_vary_headers


class InventoryCachePolicyMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        if request.path_info.startswith("/inventory/") or request.path_info in {
            "/v1/inventory/status",
            "/v1/health",
        }:
            response["Cache-Control"] = "private, no-store"
        if request.path_info == "/v1/health":
            patch_vary_headers(response, ("Cookie",))
        return response
