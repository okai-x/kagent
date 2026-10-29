"""Call 3dagnt or download a known sandbox artifact using the Python SDK."""

import argparse
import asyncio
import os
from uuid import uuid4

from google.protobuf.json_format import MessageToJson

from kagent.sdk import KagentClient


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8083")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--session")
    parser.add_argument("--request-id", help="Persist this ID and reuse it for create retries")
    parser.add_argument("--message")
    parser.add_argument("--message-id", default=str(uuid4()))
    parser.add_argument("--sandbox")
    parser.add_argument("--download")
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.download and not (args.sandbox and args.output):
        parser.error("--download requires --sandbox and --output")
    if not args.download and not args.message:
        parser.error("provide --message or --download")
    if args.message and not (args.session or args.request_id):
        parser.error("a new session requires --request-id; otherwise supply --session")
    async with KagentClient(args.url, user_id=args.user, token=os.getenv("KAGENT_TOKEN")) as client:
        if args.message:
            session_id = args.session
            if not session_id:
                session = await client.create_session("3dagnt", request_id=args.request_id)
                session_id = session.id
            print(f"session_id={session_id} message_id={args.message_id}", flush=True)  # noqa: T201
            async for event in client.stream_message(session_id, args.message, message_id=args.message_id):
                print(MessageToJson(event), flush=True)  # noqa: T201
        if args.download:
            size = await client.download(args.sandbox, args.download, args.output)
            print(f"downloaded_bytes={size} path={args.output}")  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
