import sys
import os
import asyncio
from pathlib import Path
from aiohttp import web

# Global configuration
MAX_CONCURRENT_CONNECTIONS = 16
active_connections = 0

@web.middleware
async def concurrency_limit_middleware(request, handler):
    global active_connections
    if active_connections >= MAX_CONCURRENT_CONNECTIONS:
        return web.Response(status=503, text="Service Unavailable: Max concurrent connections reached")

    active_connections += 1

    try:
        response = await handler(request)

        # Monkey patch write_eof but also hook into the task cancellation to prevent leaks
        if hasattr(response, 'write_eof'):
            original_write_eof = response.write_eof
            _eof_called = False

            async def hooked_write_eof(*args, **kwargs):
                nonlocal _eof_called
                try:
                    await original_write_eof(*args, **kwargs)
                finally:
                    if not _eof_called:
                        _eof_called = True
                        global active_connections
                        active_connections -= 1

            response.write_eof = hooked_write_eof

            # Hook into task to decrement on sudden cancel if write_eof is not called
            current_task = asyncio.current_task()
            if current_task:
                def on_task_done(fut):
                    nonlocal _eof_called
                    if not _eof_called:
                        _eof_called = True
                        global active_connections
                        active_connections -= 1
                current_task.add_done_callback(on_task_done)

        return response

    except Exception:
        # Decrement if handler threw an exception before response was generated
        active_connections -= 1
        raise

async def handle_request(request):
    base_folder_path = request.app['base_folder']
    folder_name = request.app['folder_name']

    # Strip leading slash
    rel_path = request.match_info.get('path', '').lstrip('/')

    # The expected URL is http://ip:port/foldername/filename
    # If the user requests exactly the folder name or something starting with folder_name/
    if rel_path != folder_name and not rel_path.startswith(folder_name + '/'):
        return web.Response(status=404, text="Not Found - path must start with folder name")

    # Remove the folder_name prefix
    sub_path = rel_path[len(folder_name):].lstrip('/')

    try:
        target_path = (base_folder_path / sub_path).resolve()
        # Ensure the resolved target is strictly within the base folder
        if not target_path.is_relative_to(base_folder_path):
            return web.Response(status=403, text="Forbidden")
    except (ValueError, RuntimeError):
        return web.Response(status=400, text="Bad Request")

    full_path = str(target_path)

    if not os.path.exists(full_path):
        return web.Response(status=404, text="Not Found")

    if os.path.isdir(full_path):
        files = []
        for f in os.listdir(full_path):
            if os.path.isfile(os.path.join(full_path, f)) or os.path.isdir(os.path.join(full_path, f)):
                files.append(f)

        response_data = {
            "max_concurrent_files": MAX_CONCURRENT_CONNECTIONS,
            "files": files
        }
        return web.json_response(response_data)

    elif os.path.isfile(full_path):
        return web.FileResponse(full_path)

    return web.Response(status=404, text="Not Found")

def main():
    if len(sys.argv) < 3:
        print('Usage: python sender.py "foldername" "portno"')
        sys.exit(1)

    folder_name = sys.argv[1]
    try:
        port_no = int(sys.argv[2])
    except ValueError:
        print("Error: portno must be an integer.")
        sys.exit(1)

    if not os.path.isdir(folder_name):
        print(f"Error: Directory '{folder_name}' does not exist.")
        sys.exit(1)

    app = web.Application(middlewares=[concurrency_limit_middleware])

    try:
        app['base_folder'] = Path(folder_name).resolve(strict=True)
    except Exception as e:
        print(f"Error resolving directory: {e}")
        sys.exit(1)

    app['folder_name'] = app['base_folder'].name

    app.router.add_get('/', handle_request)
    app.router.add_get('/{path:.*}', handle_request)

    print(f"Starting server on port {port_no}, serving folder: {app['folder_name']}")
    print(f"Max concurrent connections set to: {MAX_CONCURRENT_CONNECTIONS}")
    web.run_app(app, port=port_no)

if __name__ == '__main__':
    main()
