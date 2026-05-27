from typing import Dict, Optional
import threading
import queue
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

result_queue = queue.Queue()

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        query_components = parse_qs(urlparse(self.path).query)
        params = {k: v[0] for k, v in query_components.items()}
        result_queue.put(params)
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
        content = """
        <html>
            <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
                <h1>Authentication Successful</h1>
                <p>You can now close this window and return to the CLI.</p>
            </body>
        </html>
        """
        self.wfile.write(content.encode('utf-8'))

    def log_message(self, format, *args):
        # Suppress logging
        pass

def find_available_port(start: int = 8500, max_attempts: int = 10) -> int:
    """Return the first free TCP port starting from *start*, trying up to *max_attempts* ports."""
    for offset in range(max_attempts):
        port = start + offset
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(
        f"No available port found in range {start}–{start + max_attempts - 1}"
    )


def start_callback_server(port: int = 8888) -> Dict[str, str]:
    server = HTTPServer(('127.0.0.1', port), CallbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    
    # Wait for result
    try:
        result = result_queue.get(timeout=300) # 5 minutes timeout
        server.shutdown()
        return result
    except queue.Empty:
        server.shutdown()
        return {}
