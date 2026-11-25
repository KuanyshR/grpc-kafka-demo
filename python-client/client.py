#!/usr/bin/env python3
"""Simple Python gRPC client for the PingPong server.

Uses the SAME shared proto file as the Java apps: ../proto/ping_pong.proto
Python stubs are generated automatically into ./generated on first run.
"""
import argparse
import os
import socket
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PROTO_DIR = os.path.abspath(os.path.join(HERE, "..", "proto"))
PROTO_FILE = os.path.join(PROTO_DIR, "ping_pong.proto")
GEN_DIR = os.path.join(HERE, "generated")


def generate_stubs():
    """(Re)generate Python stubs from the shared proto if missing or outdated."""
    target = os.path.join(GEN_DIR, "ping_pong_pb2_grpc.py")
    if os.path.exists(target) and os.path.getmtime(target) >= os.path.getmtime(PROTO_FILE):
        return
    from grpc_tools import protoc

    os.makedirs(GEN_DIR, exist_ok=True)
    result = protoc.main([
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={GEN_DIR}",
        f"--pyi_out={GEN_DIR}",
        f"--grpc_python_out={GEN_DIR}",
        PROTO_FILE,
    ])
    if result != 0:
        sys.exit(f"protoc failed with exit code {result}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Python gRPC PingPong client")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--message", default="Ping")
    parser.add_argument("--client-id", default=f"python-client@{socket.gethostname()}")
    parser.add_argument("--timeout", type=float, default=5.0, help="deadline in seconds")
    args = parser.parse_args()

    generate_stubs()
    sys.path.insert(0, GEN_DIR)

    import grpc
    from google.protobuf.json_format import MessageToJson
    import ping_pong_pb2
    import ping_pong_pb2_grpc

    request = ping_pong_pb2.PingRequest(
        message=args.message,
        client_id=args.client_id,
        timestamp=int(time.time() * 1000),
    )

    with grpc.insecure_channel(f"{args.host}:{args.port}") as channel:
        stub = ping_pong_pb2_grpc.PingPongServiceStub(channel)
        print(f">>> Sending request to {args.host}:{args.port}")
        print(MessageToJson(request))
        try:
            response = stub.Ping(request, timeout=args.timeout)
        except grpc.RpcError as e:
            print(f"gRPC call failed: {e.code().name} - {e.details()}", file=sys.stderr)
            return 1
        print("<<< Received response")
        print(MessageToJson(response))
    return 0


if __name__ == "__main__":
    sys.exit(main())
