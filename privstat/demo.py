"""Display actual HTTP responses from a temporary, locally owned service."""

import argparse
import json
import re
import socket
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.request import urlopen

import uvicorn

from .app import create_app


@contextmanager
def running_demo(port: int):
    with tempfile.TemporaryDirectory(prefix="privstat-demo-") as directory:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", port))
            listener.listen(128)
            server = uvicorn.Server(
                uvicorn.Config(
                    create_app(Path(directory) / "demo.sqlite3"), log_level="info"
                )
            )
            worker = threading.Thread(target=server.run, kwargs={"sockets": [listener]})
            worker.start()
            try:
                deadline = time.monotonic() + 15
                while not server.started:
                    if not worker.is_alive() or time.monotonic() >= deadline:
                        raise RuntimeError("PrivStat service did not start.")
                    time.sleep(0.05)
                yield f"http://127.0.0.1:{listener.getsockname()[1]}"
            finally:
                server.should_exit = True
                worker.join(timeout=10)
                if worker.is_alive():
                    raise RuntimeError("PrivStat service did not shut down.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Show the PrivStat catalog through HTTP.")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    with running_demo(args.port) as base_url:
        print(f"PrivStat live catalog: {base_url}", flush=True)
        for endpoint in ("/", "/health", "/api/datasets"):
            with urlopen(base_url + endpoint, timeout=10) as response:
                content = response.read().decode("utf-8")
                print(f"\nGET {endpoint} -> HTTP {response.status}", flush=True)
                if endpoint == "/":
                    title = re.search(r"<title>(.*?)</title>", content)
                    print(title.group(1) if title else content, flush=True)
                else:
                    print(json.dumps(json.loads(content), ensure_ascii=False, indent=2), flush=True)
    print("PrivStat demo service closed.", flush=True)


if __name__ == "__main__":
    main()
