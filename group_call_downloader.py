import asyncio
import os
import time
import signal
import pathlib
import json
from typing import Optional, List, Dict, Any # <--- ДОБАВЛЕН Optional и List, Dict, Any

from telethon import TelegramClient, functions, types
from telethon.errors import RPCError
# from telethon.errors.rpcerrorlist import TelethonNotInCallError # This specific error doesn't exist, will remove usage

import pytgcalls
from pytgcalls.types import GroupCallConfig

try:
    from pytgcalls.exceptions import PyTgCallsError, NoActiveGroupCall, ClientNotStarted, NotInCallError as PyTgCallsNotInCallError
except ImportError:
    PyTgCallsError = Exception; NoActiveGroupCall = PyTgCallsError
    ClientNotStarted = PyTgCallsError; PyTgCallsNotInCallError = PyTgCallsError

API_ID = 1
API_HASH = '1'
SESSION_NAME = 'my_telethon_rtmp_session_v2'
TARGET_ENTITY_FOR_STREAM = 'druzyab'
BASE_DOWNLOAD_FOLDER = "rtmp_stream_full_download" # Base folder for all call recordings
# Templates are now for filenames, not full paths including participant/type.
OUTPUT_AUDIO_TEMPLATE = "audio_segment_{index:04d}_ts{ts}.mp4"
OUTPUT_VIDEO_TEMPLATE = "stream_{type}_ch{ch_or_auto}_q{q_or_auto}_idx{index:04d}_ts{ts}.mp4"

SEGMENT_LIMIT_BYTES = 1 * 1024 * 1024
MAX_SEGMENTS_TO_DOWNLOAD = 120
ASSUMED_SEGMENT_DURATION_MS = 1000 # Попробуем с 4 секундами, как и задержка
DELAY_BETWEEN_SEGMENT_REQUESTS_S = 1 # Задержка между запросами сегментов
INITIAL_TIMESTAMP_RETRY_DELAY_S = 3 # Задержка, если начальный timestamp = 0
MAX_INITIAL_TIMESTAMP_ATTEMPTS = 10

keep_running = True
telethon_client_global: TelegramClient | None = None
pytg_app_global: pytgcalls.PyTgCalls | None = None
pytg_mtproto_bridge: object | None = None

target_chat_id_int_global: int | None = None
active_streams_global: List[dict[str, any]] = [] # Глобальная переменная для активных видео SSRC

async def get_all_active_video_ssrcs(chat_id: int) -> List[dict[str, any]]:
    global pytg_app_global
    print(f" Попытка получить все активные видео SSRC для chat_id={chat_id}...")
    active_ssrc_list = []
    if not pytg_app_global:
        print("  Ошибка: pytg_app_global не инициализирован для get_all_active_video_ssrcs.")
        return active_ssrc_list

    try:
        participants = await pytg_app_global.get_participants(chat_id)
        if participants is None:
            print(f"  Ошибка: Не удалось получить список участников для chat_id={chat_id} (get_participants вернул None).")
            return []
        if not participants:
            print(f"  В чате {chat_id} нет участников (список пуст).")
            return []

        print(f"  Найдено {len(participants)} участников в чате {chat_id}.")
        for p_index, participant in enumerate(participants):
            participant_id = getattr(participant, 'user_id', f'UnknownPID_{p_index}')
            first_name = getattr(participant, 'first_name', 'N/A')
            print(f"    Processing participant: {participant_id} ({first_name})")

            current_participant_ssrcs_summary = []
            has_any_stream_for_this_participant = False

            # Обработка видео источников
            try:
                if hasattr(participant, 'video_info') and participant.video_info:
                    if hasattr(participant.video_info, 'sources') and participant.video_info.sources:
                        for source_idx, source in enumerate(participant.video_info.sources):
                            if hasattr(source, 'ssrc'):
                                ssrc_info = {'participant_id': participant_id, 'ssrc': source.ssrc, 'type': 'video'}
                                active_ssrc_list.append(ssrc_info)
                                current_participant_ssrcs_summary.append(f"Video SSRC: {source.ssrc}")
                                has_any_stream_for_this_participant = True
                            else:
                                print(f"      ПРЕДУПРЕЖДЕНИЕ: Видео источник #{source_idx + 1} для участника {participant_id} не имеет атрибута 'ssrc'.")
                    else:
                        print(f"      ПРЕДУПРЕЖДЕНИЕ: Участник {participant_id} имеет video_info, но список источников (video_info.sources) пуст или отсутствует.")
                # else: # Optional: log if no video_info at all
                #     print(f"      Участник {participant_id} не имеет video_info.")
            except AttributeError as e:
                print(f"      ПРЕДУПРЕЖДЕНИЕ: Ошибка атрибута при доступе к video_info или его содержимому для участника {participant_id}: {e}")
            except Exception as e:
                print(f"      ПРЕДУПРЕЖДЕНИЕ: Неожиданная ошибка при обработке video_info для участника {participant_id}: {type(e).__name__} - {e}")

            # Обработка источников презентации (демонстрации экрана)
            try:
                if hasattr(participant, 'presentation_info') and participant.presentation_info:
                    if hasattr(participant.presentation_info, 'sources') and participant.presentation_info.sources:
                        for source_idx, source in enumerate(participant.presentation_info.sources):
                            if hasattr(source, 'ssrc'):
                                ssrc_info = {'participant_id': participant_id, 'ssrc': source.ssrc, 'type': 'presentation'}
                                active_ssrc_list.append(ssrc_info)
                                current_participant_ssrcs_summary.append(f"Presentation SSRC: {source.ssrc}")
                                has_any_stream_for_this_participant = True
                            else:
                                print(f"      ПРЕДУПРЕЖДЕНИЕ: Источник презентации #{source_idx + 1} для участника {participant_id} не имеет атрибута 'ssrc'.")
                    else:
                         print(f"      ПРЕДУПРЕЖДЕНИЕ: Участник {participant_id} имеет presentation_info, но список источников (presentation_info.sources) пуст или отсутствует.")
                # else: # Optional: log if no presentation_info at all
                #     print(f"      Участник {participant_id} не имеет presentation_info.")
            except AttributeError as e:
                print(f"      ПРЕДУПРЕЖДЕНИЕ: Ошибка атрибута при доступе к presentation_info или его содержимому для участника {participant_id}: {e}")
            except Exception as e:
                print(f"      ПРЕДУПРЕЖДЕНИЕ: Неожиданная ошибка при обработке presentation_info для участника {participant_id}: {type(e).__name__} - {e}")

            if has_any_stream_for_this_participant:
                print(f"      Найденные SSRC для участника {participant_id}: {'; '.join(current_participant_ssrcs_summary)}")
            else:
                print(f"      Участник {participant_id} не имеет активных видео или презентационных потоков с корректным SSRC.")

    except Exception as e:
        print(f"  КРИТИЧЕСКАЯ ОШИБКА при вызове get_participants или основной обработке списка участников: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc()
        return []

    if active_ssrc_list:
        print(f"  Общее количество найденных активных SSRC по всем участникам: {len(active_ssrc_list)}. Список: {active_ssrc_list}")
    else:
        print("  Активные видео SSRC не найдены.")
    return active_ssrc_list

async def get_initial_timestamp(chat_id: int, bridge: object, target_call_id: int, active_streams: List[Dict[str, Any]]) -> int:
    target_ssrcs_set = set()
    if active_streams:
        for stream_info in active_streams:
            if 'ssrc' in stream_info:
                target_ssrcs_set.add(stream_info['ssrc'])

    print(f" Попытка получить начальную метку времени для chat_id={chat_id}, call_id={target_call_id}. Целевые SSRC: {target_ssrcs_set if target_ssrcs_set else 'не указаны (попытка для аудио/общих)'}...")

    for attempt in range(MAX_INITIAL_TIMESTAMP_ATTEMPTS):
        if not keep_running: return -1
        try:
            all_channels_info = await bridge.get_all_stream_channels_info(chat_id, target_call_id)
            if not all_channels_info:
                print(f"  Попытка {attempt + 1}: Нет информации о каналах от bridge.get_all_stream_channels_info. Ожидание...")
                await asyncio.sleep(INITIAL_TIMESTAMP_RETRY_DELAY_S); continue

            relevant_timestamps = []

            # 1. Ищем метки для любого из целевых SSRC (видео/презентация)
            if target_ssrcs_set:
                for ch_info in all_channels_info:
                    if ch_info.channel in target_ssrcs_set and ch_info.last_timestamp_ms > 0:
                        relevant_timestamps.append(ch_info.last_timestamp_ms)

            # 2. Если не найдено по целевым SSRC, ищем для аудиоканала (канал 0)
            if not relevant_timestamps:
                print(f"  Попытка {attempt + 1}: Не найдено меток для целевых SSRC {target_ssrcs_set}. Поиск для аудиоканала (0)...")
                for ch_info in all_channels_info:
                    if ch_info.channel == 0 and ch_info.last_timestamp_ms > 0: # Аудио часто канал 0
                        relevant_timestamps.append(ch_info.last_timestamp_ms)

            # 3. Если все еще не найдено, берем минимальную из всех каналов с scale=0 (основные потоки)
            if not relevant_timestamps:
                 print(f"  Попытка {attempt + 1}: Не найдено меток для аудиоканала. Поиск для любых каналов с scale=0...")
                 relevant_timestamps = [ch.last_timestamp_ms for ch in all_channels_info if ch.scale == 0 and ch.last_timestamp_ms > 0]

            if relevant_timestamps:
                initial_ts = min(relevant_timestamps)
                print(f"  Начальная/актуальная метка времени определена как: {initial_ts}")
                return initial_ts

            print(f"  Попытка {attempt + 1}: Релевантные метки времени не найдены или все 0. Ожидание...")
            await asyncio.sleep(INITIAL_TIMESTAMP_RETRY_DELAY_S)
        except Exception as e:
            print(f"  Попытка {attempt + 1}: Ошибка при получении метки времени: {type(e).__name__} - {e}. Ожидание...")
            await asyncio.sleep(INITIAL_TIMESTAMP_RETRY_DELAY_S)
    print("  Не удалось получить начальную метку времени после нескольких попыток.")
    return -1

async def download_entire_stream_segmented():
    global keep_running, telethon_client_global, pytg_app_global, pytg_mtproto_bridge
    global target_chat_id_int_global, active_streams_global, DOWNLOAD_FOLDER # Allow reassignment of DOWNLOAD_FOLDER

    from datetime import datetime # Import for unique folder naming

    # Create a unique directory for this specific call
    call_specific_folder_name = f"{TARGET_ENTITY_FOR_STREAM.replace('@', '').replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    # Update DOWNLOAD_FOLDER to be this specific call's folder
    DOWNLOAD_FOLDER = os.path.join(BASE_DOWNLOAD_FOLDER, call_specific_folder_name)

    ensure_dir_exists(DOWNLOAD_FOLDER)
    print(f"Сегменты этого звонка будут сохраняться в: ./{DOWNLOAD_FOLDER}/")

    if telethon_client_global is None:
        telethon_client_global = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    try:
        if not telethon_client_global.is_connected():
            await telethon_client_global.connect()
        if not await telethon_client_global.is_user_authorized():
            print("Telethon клиент: не авторизован.")
            return
        print("Telethon клиент: подключен и авторизован.")
        me_entity = await telethon_client_global.get_me()
        if not me_entity:
            print("Не удалось me_entity.")
            return
        me_input_peer = await telethon_client_global.get_input_entity(me_entity)
        print(f"Действуем от: {getattr(me_entity, 'username', me_entity.id)}")
    except Exception as e:
        print(f"Ошибка Telethon: {type(e).__name__} - {e}")
        return

    if pytg_app_global is None:
        try:
            pytg_app_global = pytgcalls.PyTgCalls(telethon_client_global, cache_duration=0)
        except Exception as e:
            print(f"Ошибка PyTgCalls init: {type(e).__name__} - {e}")
            return
    print(f"PyTgCalls клиент (v: {pytgcalls.__version__}) инициализирован.")

    chat_title = str(TARGET_ENTITY_FOR_STREAM)
    input_group_call_for_api: types.InputGroupCall | None = None
    try:
        print(f"Получение информации о entity для '{TARGET_ENTITY_FOR_STREAM}'...")
        chat_entity_obj = await telethon_client_global.get_entity(TARGET_ENTITY_FOR_STREAM)
        target_chat_id_int_global = chat_entity_obj.id
        chat_title = getattr(chat_entity_obj, 'title', str(target_chat_id_int_global))
        print(f"Цель: '{chat_title}' (ID: {target_chat_id_int_global})")

        if hasattr(pytg_app_global, '_app') and hasattr(pytg_app_global._app, 'mtproto_client'):
            pytg_mtproto_bridge = pytg_app_global._app.mtproto_client
        else:
            print("ПРЕДУПРЕЖДЕНИЕ: не удалось получить mtproto_bridge.")
            return

        input_group_call_for_api = await pytg_mtproto_bridge.get_call(target_chat_id_int_global)
        if input_group_call_for_api:
            print(f"PyTgCalls/Telethon видит стрим в '{chat_title}'. ID: {input_group_call_for_api.id}, Access Hash: {input_group_call_for_api.access_hash}")
        else:
            print(f"ВНИМАНИЕ: PyTgCalls/Telethon НЕ видит активного стрима в '{chat_title}'!")
            return
    except Exception as e:
        print(f"Ошибка получения информации о стриме: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc()
        return

    try:
        if pytg_app_global is None:
            print("Ошибка: pytg_app_global не инициализирован.")
            return
        if not telethon_client_global.is_connected():
            print("Telethon клиент не был подключен. Попытка подключения...")
            await telethon_client_global.connect()
            if not telethon_client_global.is_connected():
                print("Не удалось подключить Telethon.")
                return
        print("Запуск PyTgCalls (если еще не запущен)...")
        if not getattr(pytg_app_global, '_is_running', False):
            await pytg_app_global.start()
        print("PyTgCalls запущен.")
    except Exception as e:
        print(f"Ошибка pytg_app_global.start(): {type(e).__name__} - {e}")
        return

    connection_successful_flag = False
    try:
        print(f"Тихое присоединение к стриму в '{chat_title}' (ID: {target_chat_id_int_global})...")
        if not all([me_input_peer, target_chat_id_int_global is not None]):
            print("Ошибка: me_input_peer или ID чата не определен.")
            return
        call_config = GroupCallConfig(join_as=me_input_peer, auto_start=False)
        await pytg_app_global.play(target_chat_id_int_global, None, config=call_config)
        print(f"Вызов app.play(stream=None) для {target_chat_id_int_global} выполнен.")
        connection_successful_flag = True
        print(f"Ожидание {DELAY_BETWEEN_SEGMENT_REQUESTS_S + 2} секунд после присоединения...")
        await asyncio.sleep(DELAY_BETWEEN_SEGMENT_REQUESTS_S + 2)
    except NoActiveGroupCall:
        print(f"Ошибка: Нет активного группового звонка в чате {chat_title}.")
        return
    except ClientNotStarted:
        print(f"Ошибка: Клиент PyTgCalls не был запущен перед вызовом play.")
        return
    except PyTgCallsNotInCallError:
        print(f"Ошибка: PyTgCalls не смог присоединиться к звонку (PyTgCallsNotInCallError).")
        return
    except PyTgCallsError as e:
        print(f"Ошибка PyTgCalls при app.play: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc()
        return
    except Exception as e:
        print(f"Непредвиденная ошибка при app.play: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc()
        return

    if not (connection_successful_flag and input_group_call_for_api and pytg_mtproto_bridge and             hasattr(pytg_mtproto_bridge, 'download_stream') and hasattr(pytg_mtproto_bridge, 'get_stream_timestamp')):
        print("Не выполнены условия для начала скачивания сегментов.")
        return

    print(f"\n--- Попытка получить начальную метку времени ---")
    if input_group_call_for_api is None: # Should ideally not happen due to check above, but good for safety
        print("Ошибка: input_group_call_for_api не инициализирован. Невозможно получить метку времени.")
        return

    current_req_ts = await get_initial_timestamp(target_chat_id_int_global, pytg_mtproto_bridge, input_group_call_for_api.id, active_streams_global)
    if current_req_ts == -1:
        print("Не удалось получить начальную метку времени для скачивания. Выход.")
        if pytg_app_global and hasattr(pytg_app_global, 'leave_call') and target_chat_id_int_global is not None:
            try:
                await pytg_app_global.leave_call(target_chat_id_int_global)
            except: # pylint: disable=bare-except
                pass
        return

    print(f"\n--- Начало циклического скачивания с ts={current_req_ts} (до {MAX_SEGMENTS_TO_DOWNLOAD} сегментов) ---")
    downloaded_segment_count = 0
    video_quality_to_request = 2

    while keep_running and downloaded_segment_count < MAX_SEGMENTS_TO_DOWNLOAD:
        if target_chat_id_int_global is None: # Safety check
            print("Критическая ошибка: target_chat_id_int_global не установлен перед началом основного цикла. Выход.")
            keep_running = False
            break
        print(f"\n--- Обновление списка активных потоков для ts={current_req_ts} (Общий сегмент #{downloaded_segment_count + 1}) ---")
        active_streams_global = await get_all_active_video_ssrcs(target_chat_id_int_global)

        if not active_streams_global and downloaded_segment_count > 0:
            print(f"  На ts={current_req_ts} не обнаружено активных видео/презентационных потоков, хотя ранее скачивание было. Ожидание появления потоков или завершения звонка...")
        elif not active_streams_global:
            print(f"  Нет активных видео/презентационных потоков в {chat_title} для начала загрузки. Попытка получить аудио.")

        print(f"\n--- Скачивание данных для общего сегмента #{downloaded_segment_count + 1} (запрос с ts={current_req_ts}) ---")

        audio_segment_data = None
        video_segment_data = None # Defined at this scope for the loop
        download_succeeded_this_iteration = False
        attempt_ts_advance_due_to_time_small = False

        print(f"  Попытка скачать АУДИО с ts={current_req_ts}")
        try:
            audio_segment_data = await pytg_mtproto_bridge.download_stream(
                chat_id=target_chat_id_int_global,
                input_group_call_to_use=input_group_call_for_api,
                timestamp=current_req_ts, limit=SEGMENT_LIMIT_BYTES,
                video_channel=None, video_quality=None)
            if audio_segment_data:
                audio_subfolder = os.path.join(DOWNLOAD_FOLDER, 'call_audio')
                ensure_dir_exists(audio_subfolder)
                fname_audio = OUTPUT_AUDIO_TEMPLATE.format(
                    index=downloaded_segment_count + 1,
                    ts=current_req_ts
                )
                fpath_audio = os.path.join(audio_subfolder, fname_audio)
                offset = find_ftyp_offset(audio_segment_data)
                if offset != -1:
                    audio_segment_data = audio_segment_data[offset:]
                else:
                    print(f"    АУДИО: ftyp не найден для ts={current_req_ts}, сохраняем как есть.")
                with open(fpath_audio, 'wb') as f:
                    f.write(audio_segment_data)
                print(f"    АУДИО ({len(audio_segment_data)} байт) -> {fpath_audio}")
                download_succeeded_this_iteration = True
            else:
                print(f"    АУДИО (ts={current_req_ts}) -> No data received.")
        except RPCError as e_rpc_aud:
            if "TIME_TOO_SMALL" in str(e_rpc_aud):
                print(f"    АУДИО ОШИБКА (ts={current_req_ts}): TIME_TOO_SMALL.")
                attempt_ts_advance_due_to_time_small = True
            else:
                print(f"    АУДИО RPC ОШИБКА (ts={current_req_ts}): {type(e_rpc_aud).__name__} - {e_rpc_aud}")
        except Exception as e_dl_a:
            print(f"    АУДИО Общая ошибка (ts={current_req_ts}): {type(e_dl_a).__name__} - {e_dl_a}")

        if not keep_running:
            break

        if not active_streams_global:
            print(f"  Нет активных видео/презентационных SSRC для скачивания на этой итерации (ts={current_req_ts}).")

        for stream_info in active_streams_global:
            if not keep_running: break

            p_id = stream_info['participant_id']
            s_src = stream_info['ssrc']
            s_type = stream_info['type']

            print(f"  Попытка скачать {s_type.upper()} для участника {p_id} (SSRC: {s_src}) с ts={current_req_ts}")
            try:
                video_segment_data = await pytg_mtproto_bridge.download_stream(
                    chat_id=target_chat_id_int_global,
                    input_group_call_to_use=input_group_call_for_api,
                    timestamp=current_req_ts,
                    limit=SEGMENT_LIMIT_BYTES,
                    video_channel=s_src, # Corrected from ssrc_for_stream to s_src if this was an error source
                    video_quality=video_quality_to_request
                )
                if video_segment_data:
                    offset = find_ftyp_offset(video_segment_data)
                    if offset != -1:
                        video_segment_data = video_segment_data[offset:]
                    else:
                        print(f"    {s_type.upper()}: ftyp не найден для ts={current_req_ts}, p_id={p_id}, ssrc={s_src}, q={video_quality_to_request}, сохраняем как есть.")

                    fname_video = OUTPUT_VIDEO_TEMPLATE.format(
                        index=downloaded_segment_count + 1,
                        ch_or_auto=str(s_src),
                        q_or_auto=video_quality_to_request,
                        ts=current_req_ts,
                        type=s_type
                    )
                    participant_video_subfolder = os.path.join(DOWNLOAD_FOLDER, str(p_id))
                    ensure_dir_exists(participant_video_subfolder)
                    fpath_video = os.path.join(participant_video_subfolder, fname_video)
                    with open(fpath_video, 'wb') as f:
                        f.write(video_segment_data)
                    print(f"    {s_type.upper()} ({len(video_segment_data)} байт от {p_id}, SSRC {s_src}) -> {fpath_video}")
                    download_succeeded_this_iteration = True
                else:
                    print(f"    {s_type.upper()} (участник {p_id}, SSRC {s_src}, ts={current_req_ts}) -> No data received.")
            except RPCError as e_rpc_vid:
                if "TIME_TOO_SMALL" in str(e_rpc_vid):
                    print(f"    {s_type.upper()} ОШИБКА (участник {p_id}, SSRC {s_src}, ts={current_req_ts}): TIME_TOO_SMALL.")
                    attempt_ts_advance_due_to_time_small = True
                elif "You haven't joined this group call" in str(e_rpc_vid): # Corrected error message check
                    print(f"    {s_type.upper()} RPC ОШИБКА (участник {p_id}, SSRC {s_src}, ts={current_req_ts}): {e_rpc_vid} - Критическая ошибка, возможно, мы были исключены или звонок завершен. Остановка.")
                    keep_running = False
                else:
                    print(f"    {s_type.upper()} RPC ОШИБКА (участник {p_id}, SSRC {s_src}, ts={current_req_ts}): {type(e_rpc_vid).__name__} - {e_rpc_vid}")
            except Exception as e_dl_vid:
                print(f"    {s_type.upper()} Общая ошибка (участник {p_id}, SSRC {s_src}, ts={current_req_ts}): {type(e_dl_vid).__name__} - {e_dl_vid}")

            if not keep_running: break # Break from inner loop

        if not keep_running: # Break from outer loop if needed
            break

        if download_succeeded_this_iteration:
            downloaded_segment_count += 1
            current_req_ts += ASSUMED_SEGMENT_DURATION_MS
        elif attempt_ts_advance_due_to_time_small:
            print(f"  Была ошибка TIME_TOO_SMALL для общего ts={current_req_ts}. Попытка получить новую актуальную метку.")
            # Ensure input_group_call_for_api is not None before using its id
            if input_group_call_for_api:
                 new_ts_after_error = await get_initial_timestamp(target_chat_id_int_global, pytg_mtproto_bridge, input_group_call_for_api.id, active_streams_global)
                 if new_ts_after_error != -1 and new_ts_after_error > current_req_ts:
                     print(f"  Переходим на новую общую метку: {new_ts_after_error}")
                     current_req_ts = new_ts_after_error
                 else:
                     print(f"  Не удалось получить значительно новую метку (или ошибка), пробуем инкремент от {current_req_ts}.")
                     current_req_ts += ASSUMED_SEGMENT_DURATION_MS
            else:
                print("  Не удалось обновить метку времени после TIME_TOO_SMALL: input_group_call_for_api отсутствует.")
                current_req_ts += ASSUMED_SEGMENT_DURATION_MS # Fallback
            await asyncio.sleep(1)
            continue # To the next iteration of the while loop
        else: # No download, not TIME_TOO_SMALL
            print(f"  Сегмент для ts={current_req_ts} не скачан (ни аудио, ни видео) и не TIME_TOO_SMALL.")
            if input_group_call_for_api: # Ensure it's not None
                server_latest_overall_ts_check = await pytg_mtproto_bridge.get_stream_timestamp(target_chat_id_int_global, target_video_channel_id=None, call_id_for_cache=input_group_call_for_api.id)
                if server_latest_overall_ts_check == 0 and downloaded_segment_count > 0:
                     print(f"  Серверная метка 0, а мы уже качали. Вероятно, стрим завершен. Остановка.")
                     keep_running = False
                else:
                     print(f"  Пробуем следующий предполагаемый сегмент: {current_req_ts + ASSUMED_SEGMENT_DURATION_MS}")
                     current_req_ts += ASSUMED_SEGMENT_DURATION_MS
            else:
                print("  Не удалось проверить серверную метку: input_group_call_for_api отсутствует.")
                current_req_ts += ASSUMED_SEGMENT_DURATION_MS # Fallback

        if keep_running and downloaded_segment_count < MAX_SEGMENTS_TO_DOWNLOAD:
            print(f"  Завершили обработку. Скачано общих сегментных групп: {downloaded_segment_count}. Задержка {DELAY_BETWEEN_SEGMENT_REQUESTS_S}с...")
            await asyncio.sleep(DELAY_BETWEEN_SEGMENT_REQUESTS_S)
        elif downloaded_segment_count >= MAX_SEGMENTS_TO_DOWNLOAD:
            print(f"  Достигнут лимит в {MAX_SEGMENTS_TO_DOWNLOAD} общих сегментных групп.")

    print("\n--- Циклическое скачивание завершено ---")
    if pytg_app_global and hasattr(pytg_app_global, 'leave_call') and target_chat_id_int_global is not None:
                print(f"    ВИДЕО ({len(video_segment_data)} байт) -> {fpath_video}")
                download_succeeded_this_iteration = True
            else:
                print(f"    ВИДЕО (канал={video_channel_to_request_now}, q={video_quality_to_request}, ts={current_req_ts}) -> None.")
        except RPCError as e_rpc_vid:
        try:
            print(f"Попытка покинуть звонок в {chat_title}...")
            if getattr(pytg_app_global, '_is_running', False):
                active_calls = await pytg_app_global.calls # type: ignore
                if target_chat_id_int_global in active_calls:
                    await pytg_app_global.leave_call(target_chat_id_int_global)
                    print("Успешно покинули звонок.")
                else:
                    print(f"Звонок в чате {target_chat_id_int_global} не был активен для PyTgCalls (при попытке leave_call).")
            else:
                print("PyTgCalls не был запущен, пропуск leave_call.")
        except (PyTgCallsNotInCallError, NoActiveGroupCall):
            print("Уже не в звонке или звонок неактивен (при попытке leave_call).")
        except Exception as e_leave:
            print(f"Ошибка при выходе из звонка: {type(e_leave).__name__} - {e_leave}")

def find_ftyp_offset(data: bytes) -> int:
    ftyp_marker = b'ftyp'
    try:
        current_pos = 0
        while current_pos < len(data) - 7:
            box_size_bytes = data[current_pos : current_pos+4]
            if len(box_size_bytes) < 4: break
            box_size = int.from_bytes(box_size_bytes, 'big')
            box_type_bytes = data[current_pos+4 : current_pos+8]
            if len(box_type_bytes) < 4: break
            box_type = box_type_bytes
            if box_size == 0: # Box extends to EOF
                if box_type == ftyp_marker: return current_pos
                break
            if box_size < 8: break # Invalid size
            if box_type == ftyp_marker:
                return current_pos
            if current_pos + box_size > len(data) : break
            current_pos += box_size

        # Fallback: simple find, then check if it's part of a valid box structure
        ftyp_marker_pos = data.find(ftyp_marker)
        if ftyp_marker_pos != -1 and ftyp_marker_pos >=4: # ftyp should be preceded by its size
            try:
                potential_size_bytes = data[ftyp_marker_pos-4:ftyp_marker_pos]
                if len(potential_size_bytes) < 4: return -1
                potential_size = int.from_bytes(potential_size_bytes, 'big')
                # Check if this ftyp is likely a valid box start
                if potential_size >= 8 and (ftyp_marker_pos - 4 + potential_size) <= len(data) :
                    return ftyp_marker_pos - 4
            except:
                pass # Not a valid box start
    except Exception as e:
        print(f"Ошибка в find_ftyp_offset: {e}")
        pass
    return -1

def ensure_dir_exists(dir_path: str):
    """Создает директорию, если она не существует."""
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)

def signal_handler_main(sig, frame):
    global keep_running
    try: # Для *nix систем, где signal.Signals доступен
        signal_name = signal.Signals(sig).name
    except AttributeError: # Для Windows, где signal.Signals(sig).name может не работать для SIGINT/SIGTERM
        signal_name = str(sig)
    print(f"\n[SIGNAL_HANDLER] Сигнал {signal_name}. Завершение...")
    if not keep_running:
        print("[SIGNAL_HANDLER] Повторный сигнал, принудительный выход.")
        os._exit(1)
    keep_running = False

if __name__ == "__main__":
    # No longer checking API_ID/API_HASH here as they are now set to 1 by default per instructions
    # if API_ID == 1 or API_HASH == '1':
    #     print("ЗАМЕНИТЕ API_ID и API_HASH!")
    #     exit()
    signal.signal(signal.SIGINT, signal_handler_main)
    signal.signal(signal.SIGTERM, signal_handler_main)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        print("[LAUNCHER] Запуск...")
        loop.run_until_complete(download_entire_stream_segmented())
    except KeyboardInterrupt:
        print("\n[LAUNCHER] Прерывание пользователем (KeyboardInterrupt).")
        keep_running = False
    except SystemExit:
        print("[LAUNCHER] SystemExit.")
    except RuntimeError as e:
        if "Event loop is closed" in str(e) or "cannot schedule new futures" in str(e):
            print(f"[LAUNCHER] Ошибка цикла событий: {e}")
        else:
            print(f"[LAUNCHER] Общая ошибка выполнения: {type(e).__name__} - {e}")
            import traceback
            traceback.print_exc()
    else:  # This block executes if the try block completes without raising an exception.
        print("[LAUNCHER] Main task completed without exceptions.")
    finally:
        print("[LAUNCHER_FINALLY] Финальная очистка...")

        async def cleanup_async_resources():
            global pytg_app_global, telethon_client_global, target_chat_id_int_global

            if pytg_app_global:
                print("[LAUNCHER_FINALLY] Остановка/выход из звонков PyTgCalls...")
                try:
                    if target_chat_id_int_global is not None:
                        if getattr(pytg_app_global, '_is_running', False):
                            active_calls = await pytg_app_global.calls
                            if target_chat_id_int_global in active_calls:
                                print(f"Покидаем звонок в чате {target_chat_id_int_global}...")
                                await pytg_app_global.leave_call(target_chat_id_int_global)
                                print(f"[L_F] Звонок в чате {target_chat_id_int_global} покинут.")
                            else:
                                print(f"[L_F] Звонок в чате {target_chat_id_int_global} не был активен для PyTgCalls.")
                        else:
                            print("[L_F] PyTgCalls не был запущен, пропуск leave_call.")
                except (PyTgCallsNotInCallError, NoActiveGroupCall):
                    print("[L_F] Попытка leave_call, но уже не в звонке или звонок неактивен.")
                except Exception as e_f_leave:
                    print(f"Ошибка pytg_app_global.leave_call() в finally: {type(e_f_leave).__name__} - {e_f_leave}")

            if telethon_client_global and telethon_client_global.is_connected():
                print("[LAUNCHER_FINALLY] Отключение Telethon...")
                try:
                    await telethon_client_global.disconnect()
                    print("[L_F] Telethon клиент отключен.")
                except Exception as e_f_disc:
                    print(f"Ошибка telethon.disconnect() в finally: {type(e_f_disc).__name__} - {e_f_disc}")

        # Attempt to run cleanup_async_resources
        try:
            current_loop_for_cleanup = asyncio.get_event_loop_policy().get_event_loop()
            if current_loop_for_cleanup.is_closed():
                print("[L_F] Основной цикл событий закрыт, запуск очистки в новом цикле.")
                asyncio.run(cleanup_async_resources())
            elif current_loop_for_cleanup.is_running():
                print("[L_F] Основной цикл событий работает, запуск очистки в потокобезопасном режиме.")
                cleanup_future = asyncio.run_coroutine_threadsafe(cleanup_async_resources(), current_loop_for_cleanup)
                cleanup_future.result(timeout=10)
            else:
                print("[L_F] Основной цикл событий не закрыт и не работает, попытка запуска очистки на нем.")
                current_loop_for_cleanup.run_until_complete(asyncio.wait_for(cleanup_async_resources(), timeout=10))
        except RuntimeError as e_rt_cleanup:
            print(f"[L_F] Ошибка RuntimeError при выполнении cleanup_async_resources: {e_rt_cleanup}. Попытка запуска в новом цикле.")
            try:
                asyncio.run(cleanup_async_resources())
            except Exception as e_final_cleanup:
                print(f"[L_F] Ошибка при аварийной попытке очистки в новом цикле: {e_final_cleanup}")
        except Exception as e_cleanup:
            print(f"[L_F] Общая ошибка при выполнении cleanup_async_resources: {e_cleanup}")

        # Final attempt to clean up any remaining asyncio tasks from the original loop if it's still accessible and not closed.
        if loop and not loop.is_closed():
            try:
                tasks = [t for t in asyncio.all_tasks(loop=loop) if not t.done()]
                if tasks:
                    print(f"[L_F] Отмена {len(tasks)} оставшихся задач в исходном цикле...")
                    for task in tasks:
                        task.cancel()
                    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                    print("[L_F] Оставшиеся задачи в исходном цикле отменены.")

                if loop.is_running():
                    loop.stop()
                # It's generally safer to let the loop close when the program exits if it's not explicitly closed elsewhere.
                # loop.close()
            except Exception as e_loop_final_cleanup:
                print(f"[L_F] Ошибка при финальной очистке задач исходного цикла: {e_loop_final_cleanup}")

        print("[LAUNCHER] Скрипт завершен.")
