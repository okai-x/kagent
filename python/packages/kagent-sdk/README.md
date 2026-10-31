# kagent Python SDK

Async Python client for the current kagent Session, A2A, Checkpoint, and Sandbox APIs.
This is a source package in this repository, not a published PyPI release.
It uses gRPC directly; it does not shell out to the kagent CLI.

From the repository root, install both matching packages:

```sh
uv venv .venv-sdk
uv pip install --python .venv-sdk/bin/python ./python/packages/kagent-proto ./python/packages/kagent-sdk
```

## Talk to an existing agent

```python
import asyncio
from uuid import uuid4
from kagent.sdk import KagentClient


async def main():
    async with KagentClient(user_id="admin") as client:
        # Persist this ID BEFORE calling; reuse it with the same input on retry.
        request_id = str(uuid4())
        session = await client.create_session("3dagnt", request_id=request_id)
        print("Session:", session.id)
        # Reuse session.id in later calls to continue this conversation.
        async for event in client.stream_message(
            session.id, "Describe your Blender execution environment.", message_id=str(uuid4())
        ):
            print(event)


asyncio.run(main())
```

A2A events are the upstream protobuf messages, including task/status/artifact
events. Stream completion alone does not mean task success: inspect task status,
including failed, canceled, and input-required states. No SDK prompt is appended.

## Structured turns and task recovery

Use upstream `a2a.Part` values to preserve separate text, structured context,
and image parts. `stream_message` remains the text-only shorthand.

```python
from contextlib import aclosing
from a2a.types import a2a_pb2 as a2a

parts = [
    a2a.Part(text="Make the selected part red."),
    a2a.Part(raw=selection_png, media_type="image/png", filename="selection.png"),
]
async with aclosing(client.stream_parts(session_id, parts, message_id=saved_message_id)) as stream:
    async for event in stream:
        if event.HasField("task"):
            saved_task_id = event.task.id  # Persist before relying on reconnection.

task = await client.get_task(session_id, saved_task_id)
page = await client.list_tasks(session_id, page_size=20, history_length=0)
if page.next_page_token:
    next_page = await client.list_tasks(session_id, page_size=20, page_token=page.next_page_token, history_length=0)
async with aclosing(client.subscribe_task(session_id, saved_task_id)) as stream:
    async for event in stream:
        process_event(event)
```

These methods resolve A2A routing from the session's agent and forward the same
identity and deadline as session RPCs. Task lists are scoped to the session and
return one page. An omitted `history_length` leaves the limit to the server;
zero explicitly excludes history. Pass `task_id` to `stream_parts` only when
continuing an existing task waiting for input.

Use `aclosing` when leaving an iterator early. Closing an observer only closes
its gRPC stream; it does not send `CancelTask`. To request cancellation explicitly,
call `await client.cancel_task(session_id, saved_task_id)`. Task cancellation does
not establish that independently launched sandbox processes have stopped.

If sending fails before a task ID arrives, the outcome can be unknown. The SDK
does not invent a message-ID lookup or retry the mutation. Reconcile with the
platform before another send; retaining a message ID alone does not guarantee
server-side deduplication of continuation messages.

## Export native Session history

```python
page = await client.export_session_history(session_id, page_size=100)
save_cutoff(page.cutoff)  # Persist before publishing this page's events.
while True:
    save_native_events(page.events)  # Preserve event.data bytes without decoding/re-encoding.
    if not page.has_more:
        break
    page = await client.export_session_history(session_id, cutoff=page.cutoff, after_sequence=page.next_after_sequence)
```

The caller implements durable `save_cutoff` and `save_native_events`. Reuse the
saved cutoff with cursor zero to restart the same history prefix; later appends
are excluded. Calls preserve gRPC status and never automatically retry. Pages
target 2 MiB and may contain one event up to 8 MiB including identity metadata.
An oversized event fails with `RESOURCE_EXHAUSTED`, without truncation or omission.

Only the owner or authorized control-plane callers can export; shares and runtime
Actors cannot. This copies stored event bytes, including unknown protobuf fields,
not original model HTTP traffic or private runtime state. The cutoff does not
retain a deleted Session or prove task completion. Archive before source deletion
and keep schema/version information with the exported bytes.

## Read a Session Actor workspace

A Harness can run CPU Blender beside the agent runtime in the same Actor. The
model uses its native file/bash tools under `/data/workspace`; no independent
Sandbox is required. The product reads an output after the actual latest task is
terminal and settled:

```python
from contextlib import aclosing

chunks = []
async with aclosing(client.read_workspace_file(session_id, "result.glb", terminal_task_id)) as stream:
    async for chunk in stream:
        chunks.append(chunk)
content = b"".join(chunks)  # Publish only after successful EOF.
```

The service checks ownership, the latest settled terminal head and concurrent
runtime access, with a 64 MiB file limit. Paths are canonical relative paths
under `/data/workspace`; symlinks and non-regular files are rejected. Successful
EOF confirms runtime cleanup and snapshot completion. Closing the iterator
cancels the read RPC; it does not send `CancelTask` or delete the Session.

The SDK preserves gRPC status and structured error details without retrying.
Discard all partial bytes on any error. Only `NOT_FOUND` with
`google.rpc.ErrorInfo(reason="WORKSPACE_FILE_NOT_FOUND", domain="kagent.dev")`
means the file is absent after confirmed cleanup. Ordinary `NOT_FOUND` may mean
the Session is missing and must not be treated as an optional output. An
`UNAVAILABLE` or `FAILED_PRECONDITION` response is not a successful empty file.

## Execute Blender directly

```python
async with KagentClient(user_id="admin") as client:
    sandbox = await client.create_sandbox(
        "kiln-session-0-5", request_id="persist-a-unique-request-id", ttl_seconds=3600
    )
    await client.upload(sandbox.id, "scene.py", "/data/workspace/scene.py")
    process = await client.start_process(
        sandbox.id, ["/opt/blender/blender", "-b", "-t", "1", "--python", "/data/workspace/scene.py"]
    )
    print("Process:", process.process_id)
    exit_code = None
    async for event in client.stream_output(sandbox.id, process.process_id):
        if event.HasField("exit"):
            exit_code = event.exit.exit_code
    if exit_code != 0:
        raise RuntimeError(f"Blender did not complete successfully: {exit_code=}")
    await client.download(sandbox.id, "/data/workspace/scene.blend", "scene.blend")
    await client.suspend_sandbox(sandbox.id)
    # Resume preserves /data, but process IDs do not survive suspension.
    await client.resume_sandbox(sandbox.id)
    await client.delete_sandbox(sandbox.id)
```

The sandbox has an independent lifecycle. For agent tool access, the trusted
product service must pass `session_id=session.id` to `create_sandbox`. The binding
is immutable and the Session must have the same owner. The server verifies the
Actor's signed Session identity; passing a Sandbox ID in a prompt grants no
access. An Actor can get/list and use guest operations only for its bound
Sandboxes, and cannot manage their lifecycle. The unbound example above is
accessible only to its product owner. Retain useful files before expiry or
deletion. Image/template creation is an operator task.

Before expiry, the product owner can call
`await client.renew_sandbox(sandbox.id, expires_at)` with a timezone-aware Python
`datetime`. The absolute deadline is idempotent and can only extend the lease,
within the server's maximum TTL. Renewal cannot revive an expired Sandbox.

## Authentication and lower-level access

`user_id="admin"` is for the local insecure-auth deployment. In production use
`KagentClient("https://your-controller", token=...)`; the token is a kagent
credential, not an OpenRouter model key. No tokens are stored in files by the SDK.
For rotating projected service-account tokens, pass `token_file="/mounted/token"`
instead of `token`; the SDK reads the file for each RPC. Use controller auth mode
`service-account` with a configured authority and a `kagent-invocation` audience.
The endpoint must support native HTTP/2 gRPC, not only browser gRPC-Web.

Public generated stubs are available as `sessions`, `checkpoints`, `sandboxes`, `processes`,
`files`, and `a2a`. Pass `**client.call_options()` to preserve authentication and
deadlines. For process/file RPCs pass `**client.call_options(sandbox_id)` instead.
These stubs expose pagination, deletion, process signals/stdin, and task queries
without duplicating the underlying contracts.
Checkpoint creation and forking use the generated `checkpoints_pb2` requests;
pass the expected terminal head task ID when creating a checkpoint. A session
checkpoint does not snapshot an independent sandbox's files.

`timeout` is the per-RPC deadline, including streams. A timeout does not roll
back a create or stop a remote process. There are no automatic mutation retries;
especially never blindly retry StartProcess. Retain the process ID and query it.
Download errors preserve an existing destination file. Large files transfer in
chunks; uploads are not transactional on the remote side.

## Runnable example

```sh
.venv-sdk/bin/python python/packages/kagent-sdk/examples/three_d.py \
  --session YOUR_SESSION_ID --message "Make the cube red and render it again"

.venv-sdk/bin/python python/packages/kagent-sdk/examples/three_d.py \
  --sandbox YOUR_SANDBOX_ID --download /data/workspace/cube.blend --output cube.blend
```

Use `--request-id` when creating a new session. The CLI prints IDs and raw A2A
events as JSON so callers can inspect statuses and tool results.

Tests: `python -m pytest python/packages/kagent-sdk/tests`.

## Package generation and release

`make proto-lint proto-generate proto-breaking` regenerates the Python contracts
with the repository's pinned Buf plugins. Generated files are not edited manually.
The release workflow versions and builds both `kagent-proto` and `kagent-sdk`;
the SDK requires exactly the matching `kagent-proto` version. Adding these
packages to the workflow does not publish a release by itself.

Build both packages into an external directory for local wheel verification:

```sh
cd python
uv build --package kagent-proto --out-dir /tmp/kagent-sdk-dist
uv build --package kagent-sdk --out-dir /tmp/kagent-sdk-dist
```

Explicit live integration test (uses the configured model, creates a temporary
sandbox, retains its downloaded artifacts and a new test session):

```sh
.venv-sdk/bin/python python/packages/kagent-sdk/examples/smoke.py \
  --user admin@kagent.dev --output .plans/python-sdk-smoke
```

This verifies idempotent creation, Blender CPU rendering, file transfers,
suspend/resume with matching SHA256, and two A2A turns in the same session.
