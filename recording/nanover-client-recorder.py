#!/usr/bin/env python

"""
A NanoVer client that records the trajectory and the state updates.

To use this script, you need nanover-core and nanover-mdanalysis installed.
Look at https://github.com/IRL2/nanover-protocol for instructions. You also
need aiofiles that can be installed with ``pip install aiofiles``.

Run the script as `python nanover-rec.py recording` to save the state in
`recording.state` and the trajectory in `recording.traj`. The `--address` and
`--port` let you set the server's IP and port, respectivelly. By default, the
script will attempt to connect to a server running on localhost on the default
port (38801).
"""

import asyncio
import argparse
import time
import grpc.aio
import aiofiles
from nanover.mdanalysis import recordings
from nanover.protocol.trajectory import (
    GetFrameRequest,
    TrajectoryServiceStub,
)
from nanover.protocol.state import StateStub, SubscribeStateUpdatesRequest


async def record_stream(stream, outfile, start_time):
    await write_header(outfile)
    async for frame in stream:
        elapsed = perf_counter_µs() - start_time
        frame_bytes = frame.SerializeToString()
        frame_size = len(frame_bytes)
        record = (
            elapsed.to_bytes(16, "little", signed=False)
            + frame_size.to_bytes(8, "little", signed=False)
            + frame_bytes
        )
        await outfile.write(record)


async def record_trajectory(stub, outfile, start_time):
    request = GetFrameRequest()
    stream = stub.SubscribeLatestFrames(request)
    await record_stream(stream, outfile, start_time)


async def record_state(stub, outfile, start_time):
    request = SubscribeStateUpdatesRequest()
    stream = stub.SubscribeStateUpdates(request)
    await record_stream(stream, outfile, start_time)


def perf_counter_µs():
    return int(time.perf_counter_ns() / 1000)


async def write_header(target_file):
    await target_file.write(recordings.MAGIC_NUMBER.to_bytes(8, "little", signed=False))
    await target_file.write((2).to_bytes(8, "little", signed=False))


async def record_from_server(address, state_file, trajectory_file):
    start_time = perf_counter_µs()
    channel = grpc.aio.insecure_channel(address)
    await channel.channel_ready()
    state_stub = StateStub(channel)
    trajectory_stub = TrajectoryServiceStub(channel)
    state_recorder = record_state(state_stub, state_file, start_time)
    trajectory_recorder = record_trajectory(
        trajectory_stub, trajectory_file, start_time
    )
    await asyncio.gather(state_recorder, trajectory_recorder)


def handle_user_input():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="localhost")
    parser.add_argument("--port", default=38801, type=int)
    parser.add_argument("outfile_stem")
    args = parser.parse_args()
    address = f"{args.address}:{str(args.port)}"
    state_path = f"{args.outfile_stem}.state"
    trajectory_path = f"{args.outfile_stem}.traj"
    return address, state_path, trajectory_path


async def main():
    address, state_path, trajectory_path = handle_user_input()
    async with aiofiles.open(state_path, "wb") as state_file:
        async with aiofiles.open(trajectory_path, "wb") as trajectory_file:
            await record_from_server(address, state_file, trajectory_file)


if __name__ == "__main__":
    asyncio.run(main())
