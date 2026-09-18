import argparse

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the PrivStat local data catalog.")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    uvicorn.run("privstat.app:app", host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
