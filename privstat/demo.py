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
from urllib.error import HTTPError
from urllib.request import Request, urlopen

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
                    create_app(Path(directory) / "demo.sqlite3"), log_level="warning"
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


def _get_json(base_url: str, endpoint: str):
    with urlopen(base_url + endpoint, timeout=10) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def _print_json(prefix: str, payload) -> None:
    print(prefix, json.dumps(payload, ensure_ascii=False, indent=2), flush=True)


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

        publish_payload = {
            "request_id": "demo-release-0001",
            "dataset_id": "retail-demo",
            "filters": [
                {"field": "region", "value": "north"},
                {"field": "membership", "value": "standard"},
            ],
            "epsilon": 1.0,
        }
        request = Request(
            base_url + "/api/releases",
            data=json.dumps(publish_payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        print("\nPOST /api/releases ->", flush=True)
        _print_json("  request:", publish_payload)
        try:
            with urlopen(request, timeout=10) as response:
                release = json.loads(response.read().decode("utf-8"))
            print(f"  HTTP {200}", flush=True)
            _print_json("  response:", release)
        except HTTPError as error:
            print(f"  HTTP {error.code}: {error.read().decode('utf-8')}", flush=True)
            raise

        print("\nGET /api/privacy-budget ->", flush=True)
        status, budget = _get_json(base_url, "/api/privacy-budget")
        _print_json(f"  HTTP {status}:", budget)

        print("\nGET /api/releases ->", flush=True)
        status, releases = _get_json(base_url, "/api/releases")
        _print_json(f"  HTTP {status}:", releases)
    print("\nPrivStat demo service closed.", flush=True)


if __name__ == "__main__":
    main()
