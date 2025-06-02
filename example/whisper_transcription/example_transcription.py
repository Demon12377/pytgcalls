from ai_model import AIModel
from pyrogram import Client
from pyrogram import idle

from pytgcalls import filters
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioQuality
from pytgcalls.types import Device
from pytgcalls.types import Direction
from pytgcalls.types import RecordStream
from pytgcalls.types import StreamFrames

AUDIO_QUALITY = AudioQuality.HIGH

# Define output templates
OUTPUT_AUDIO_TEMPLATE = "participant_{participant_id}_audio_segment_{index:04d}_ts{ts}.mp4"
OUTPUT_VIDEO_TEMPLATE = "participant_{participant_id}_video_segment_ch{ch_or_auto}_q{q_or_auto}_idx{index:04d}_ts{ts}.mp4"

model = AIModel(AUDIO_QUALITY)

app = Client(
    'py-tgcalls',
    api_id=1,
    api_hash='1',
)

call_py = PyTgCalls(app)
TARGET_ENTITY_FOR_STREAM = 'druzyab'
call_py.start()
call_py.record(
    TARGET_ENTITY_FOR_STREAM,
    RecordStream(
        True,
        AUDIO_QUALITY,
    ),
)


@call_py.on_update(
    filters.stream_frame(
        Direction.INCOMING,
        Device.MICROPHONE,
    ),
)
async def audio_data(_: PyTgCalls, update: StreamFrames):
    # Transcribe just one user
    stt = model.transcribe(update.frames[0].frame)
    if stt:
        print(stt, flush=True)

idle()
