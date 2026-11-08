"""Response policy for private inventory routes, including error responses."""


class InventoryCachePolicyMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        if (
            request.path_info.startswith("/inventory/")
            or request.path_info == "/v1/inventory/status"
        ):
            response["Cache-Control"] = "private, no-store"
        return response
