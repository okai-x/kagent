"""Explicit live smoke test: creates a temporary sandbox and a retained 3dagnt session."""

import argparse
import asyncio
import hashlib
import os
from pathlib import Path
from uuid import uuid4

from a2a.types import a2a_pb2 as a2a

from kagent.sdk import KagentClient


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8083")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    request_id = str(uuid4())
    (args.output / "request-id.txt").write_text(request_id)
    script = args.output / "cube.py"
    script.write_text(
        "import bpy\n"
        "bpy.ops.wm.read_factory_settings(use_empty=False)\n"
        "scene = bpy.context.scene\n"
        "scene.render.engine = 'CYCLES'\n"
        "scene.cycles.device = 'CPU'\n"
        "scene.cycles.samples = 4\n"
        "scene.render.resolution_x = scene.render.resolution_y = 128\n"
        "scene.render.resolution_percentage = 100\n"
        "scene.render.filepath = '/data/workspace/cube.png'\n"
        "bpy.ops.wm.save_as_mainfile(filepath='/data/workspace/cube.blend')\n"
        "bpy.ops.render.render(write_still=True)\n"
    )
    async with KagentClient(args.url, user_id=args.user, token=os.getenv("KAGENT_TOKEN")) as client:
        sandbox = await client.create_sandbox("kiln-session-0-5", request_id=request_id, ttl_seconds=900)
        (args.output / "sandbox-id.txt").write_text(sandbox.id)
        print(f"sandbox_id={sandbox.id}", flush=True)  # noqa: T201
        try:
            duplicate = await client.create_sandbox("kiln-session-0-5", request_id=request_id, ttl_seconds=900)
            assert duplicate.id == sandbox.id
            await client.upload(sandbox.id, script, "/data/workspace/cube.py")
            process = await client.start_process(
                sandbox.id, ["/opt/blender/blender", "-b", "-t", "1", "--python", "/data/workspace/cube.py"]
            )
            (args.output / "process-id.txt").write_text(process.process_id)
            exit_code = None
            async for event in client.stream_output(sandbox.id, process.process_id):
                if event.HasField("exit"):
                    exit_code = event.exit.exit_code
            assert exit_code == 0, f"Blender failed: {exit_code=}"
            assert (await client.get_process(sandbox.id, process.process_id)).exit_code == 0
            for name in ("cube.blend", "cube.png"):
                size = await client.download(sandbox.id, f"/data/workspace/{name}", args.output / name)
                assert size > 0
                print(f"{name}: {size} bytes", flush=True)  # noqa: T201
            assert (args.output / "cube.png").read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
            before = hashlib.sha256((args.output / "cube.blend").read_bytes()).digest()
            await client.suspend_sandbox(sandbox.id)
            await client.resume_sandbox(sandbox.id)
            await client.download(sandbox.id, "/data/workspace/cube.blend", args.output / "restored.blend")
            assert hashlib.sha256((args.output / "restored.blend").read_bytes()).digest() == before
            print("Blender exit=0; suspend/resume SHA256 matched", flush=True)  # noqa: T201
        finally:
            await client.delete_sandbox(sandbox.id)
        session = await client.create_session("3dagnt", request_id=request_id)
        (args.output / "session-id.txt").write_text(session.id)
        repeated = await client.create_session("3dagnt", request_id=request_id)
        assert repeated.id == session.id
        print(f"session_id={session.id}", flush=True)  # noqa: T201
        for text in (
            "请记住本次 SDK 测试标记 SDK-BLENDER-OK。只回复确认，不调用工具。",
            "请返回刚才的测试标记，不调用工具。",
        ):
            state = None
            artifacts = {}
            async for event in client.stream_message(session.id, text, message_id=str(uuid4())):
                if event.HasField("task"):
                    state = event.task.status.state
                if event.HasField("status_update"):
                    state = event.status_update.status.state
                if event.HasField("artifact_update"):
                    update = event.artifact_update
                    artifact_id = update.artifact.artifact_id
                    text_parts = "".join(part.text for part in update.artifact.parts if part.HasField("text"))
                    artifacts[artifact_id] = (artifacts.get(artifact_id, "") if update.append else "") + text_parts
            assert state == a2a.TASK_STATE_COMPLETED, f"A2A task did not complete: {state=}"
            print("reply=" + "".join(artifacts.values()), flush=True)  # noqa: T201
        assert "SDK-BLENDER-OK" in "".join(artifacts.values())
        print("PASS: session reuse and A2A streaming", flush=True)  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
