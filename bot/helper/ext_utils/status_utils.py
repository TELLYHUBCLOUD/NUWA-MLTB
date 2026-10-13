from asyncio import gather
from html import escape
from inspect import iscoroutinefunction
from time import time

from psutil import cpu_percent, disk_usage, virtual_memory
from pyrogram.types import InputRichMessage

from ... import DOWNLOAD_DIR, bot_start_time, status_dict, task_dict, task_dict_lock
from ...core.config_manager import Config
from ..telegram_helper.button_build import ButtonMaker
from .links_utils import is_gdrive_id, is_rclone_path

SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]


class MirrorStatus:
    STATUS_UPLOAD = "Upload"
    STATUS_DOWNLOAD = "Download"
    STATUS_CLONE = "Clone"
    STATUS_QUEUEDL = "QueueDl"
    STATUS_QUEUEUP = "QueueUp"
    STATUS_PAUSED = "Pause"
    STATUS_ARCHIVE = "Archive"
    STATUS_EXTRACT = "Extract"
    STATUS_SPLIT = "Split"
    STATUS_CHECK = "CheckUp"
    STATUS_SEED = "Seed"
    STATUS_SAMVID = "SamVid"
    STATUS_CONVERT = "Convert"
    STATUS_FFMPEG = "FFmpeg"
    STATUS_METADATA = "Metadata"


STATUSES = {
    "ALL": "All",
    "DL": MirrorStatus.STATUS_DOWNLOAD,
    "UP": MirrorStatus.STATUS_UPLOAD,
    "QD": MirrorStatus.STATUS_QUEUEDL,
    "QU": MirrorStatus.STATUS_QUEUEUP,
    "AR": MirrorStatus.STATUS_ARCHIVE,
    "EX": MirrorStatus.STATUS_EXTRACT,
    "SD": MirrorStatus.STATUS_SEED,
    "CL": MirrorStatus.STATUS_CLONE,
    "CM": MirrorStatus.STATUS_CONVERT,
    "SP": MirrorStatus.STATUS_SPLIT,
    "SV": MirrorStatus.STATUS_SAMVID,
    "FF": MirrorStatus.STATUS_FFMPEG,
    "MD": MirrorStatus.STATUS_METADATA,
    "PA": MirrorStatus.STATUS_PAUSED,
    "CK": MirrorStatus.STATUS_CHECK,
}


def _listener(task):
    """Return a task listener without assuming the task is fully initialized."""
    return getattr(task, "listener", None)


async def _task_status(task):
    status_method = getattr(task, "status", None)
    if status_method is None:
        return "Unknown"
    result = status_method()
    if iscoroutinefunction(status_method):
        return await result
    return result


async def get_task_by_gid(gid: str):
    # Never await task updates while holding the shared task lock.
    async with task_dict_lock:
        tasks = list(task_dict.values())

    for task in tasks:
        if hasattr(task, "seeding"):
            update = getattr(task, "update", None)
            if update:
                try:
                    result = update()
                    if hasattr(result, "__await__"):
                        await result
                except Exception:
                    pass
        try:
            if task.gid() == gid:
                return task
        except Exception:
            continue
    return None


async def get_specific_tasks(status, user_id):
    # Snapshot the shared dictionary while locked; do status calls outside the lock.
    async with task_dict_lock:
        tasks = list(task_dict.values())

    if user_id:
        tasks = [
            task
            for task in tasks
            if getattr(_listener(task), "user_id", None) == user_id
        ]

    if status == "All":
        return tasks

    statuses = await gather(*(_task_status(task) for task in tasks), return_exceptions=True)
    result = []
    known_statuses = set(STATUSES.values())
    for task, task_status in zip(tasks, statuses):
        if isinstance(task_status, Exception):
            continue
        if task_status == status or (
            status == MirrorStatus.STATUS_DOWNLOAD and task_status not in known_statuses
        ):
            result.append(task)
    return result


async def get_all_tasks(req_status: str, user_id):
    return await get_specific_tasks(req_status, user_id)


def get_readable_file_size(size_in_bytes):
    try:
        size = float(size_in_bytes or 0)
    except (TypeError, ValueError):
        return "0B"
    if size <= 0:
        return "0B"

    index = 0
    while size >= 1024 and index < len(SIZE_UNITS) - 1:
        size /= 1024
        index += 1
    return f"{size:.2f}{SIZE_UNITS[index]}"


def get_readable_time(seconds: int):
    try:
        seconds = max(0, int(seconds))
    except (TypeError, ValueError, OverflowError):
        seconds = 0
    periods = (("d", 86400), ("h", 3600), ("m", 60), ("s", 1))
    result = ""
    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            result += f"{period_value}{period_name}"
    return result or "0s"


def time_to_seconds(time_duration):
    try:
        parts = str(time_duration).split(":")
        if len(parts) == 3:
            hours, minutes, seconds = map(float, parts)
        elif len(parts) == 2:
            hours = 0
            minutes, seconds = map(float, parts)
        elif len(parts) == 1:
            hours = minutes = 0
            seconds = float(parts[0])
        else:
            return 0
        if hours < 0 or minutes < 0 or seconds < 0:
            return 0
        return hours * 3600 + minutes * 60 + seconds
    except (TypeError, ValueError):
        return 0


def speed_string_to_bytes(size_text: str):
    """Convert values such as 512K, 2.5MB/s or 1GB/s to bytes."""
    try:
        text = str(size_text).strip().lower().replace("/s", "").replace(" ", "")
        if not text:
            return 0
        multipliers = (
            ("tb", 1024**4),
            ("t", 1024**4),
            ("gb", 1024**3),
            ("g", 1024**3),
            ("mb", 1024**2),
            ("m", 1024**2),
            ("kb", 1024),
            ("k", 1024),
            ("b", 1),
        )
        for suffix, multiplier in multipliers:
            if text.endswith(suffix):
                number = text[: -len(suffix)]
                return max(0, float(number) * multiplier)
        return float(text)
    except (TypeError, ValueError):
        return 0


def get_progress_bar_string(pct):
    try:
        if isinstance(pct, str):
            pct = pct.strip().rstrip("%")
        percentage = min(100.0, max(0.0, float(pct)))
    except (TypeError, ValueError):
        percentage = 0.0
    full_blocks = min(12, int(percentage / (100 / 12)))
    return f"[{('■' * full_blocks)}{('□' * (12 - full_blocks))}]"


def _get_elapsed_time(task):
    try:
        start_time = getattr(task, "start_time", 0)
        listener = _listener(task)
        if isinstance(start_time, (int, float)) and start_time > 0:
            elapsed = time() - start_time
        else:
            message = getattr(listener, "message", None)
            date = getattr(message, "date", None)
            elapsed = time() - date.timestamp() if date else 0
        return get_readable_time(max(0, int(elapsed)))
    except Exception:
        return "0s"


def _get_upload_dest(task):
    try:
        listener = _listener(task)
        if not listener or getattr(listener, "is_leech", False):
            return "Telegram"
        if getattr(listener, "is_gofile", False):
            return "GoFile"
        if getattr(listener, "is_buzzheavier", False):
            return "BuzzHeavier"
        if getattr(listener, "is_clone", False):
            return "GDrive Clone"
        destination = getattr(listener, "up_dest", "")
        if not destination:
            return "Telegram"
        if is_gdrive_id(destination):
            return "Google Drive"
        if is_rclone_path(destination):
            return "Rclone"
        return escape(str(destination))
    except Exception:
        return "Telegram"


def _get_engine_name(task):
    tool = getattr(task, "tool", "")
    try:
        from ...modules.stats import commands
    except Exception:
        commands = {}

    engine_names = {
        "qbittorrent": ("qBittorrent", "qBit"),
        "aria2": ("aria2", "Aria2"),
        "yt-dlp": ("yt-dlp", "yt-dlp"),
        "sabnzbd": ("SABnzbd+", "Sabnzbd"),
        "rclone": ("rclone", "Rclone"),
        "ffmpeg": ("ffmpeg", "FFmpeg"),
        "7z": ("7z", "7z"),
    }
    if tool in engine_names:
        command_key, fallback = engine_names[tool]
        version = commands.get(command_key)
        if isinstance(version, str) and version and not version.startswith(
            ("Error", "Exception", "Version")
        ):
            return escape(f"{fallback} v{version}")
        return fallback

    other_names = {
        "telegram": "Telegram",
        "jdownloader": "JDownloader",
        "gofile": "GoFile",
        "buzzheavier": "BuzzHeavier",
        "gDriveApi": "Google Drive",
    }
    return escape(other_names.get(tool, tool.capitalize() if tool else "Unknown"))


def _get_user_display(task):
    listener = _listener(task)
    if not listener:
        return "User"
    user = getattr(listener, "user", None)
    user_id = getattr(listener, "user_id", 0)
    name = getattr(user, "first_name", None) or getattr(user, "title", None)
    name = str(name or (user_id if user_id else "User"))
    safe_name = escape(name)
    if user_id:
        return f"<a href='tg://user?id={int(user_id)}'>{safe_name}</a> ({int(user_id)})"
    return safe_name


def _safe_call(task, method_name, default="—"):
    try:
        method = getattr(task, method_name, None)
        return escape(str(method() if callable(method) else default))
    except Exception:
        return escape(str(default))


def _system_stats():
    try:
        cpu = f"{cpu_percent()}%"
    except Exception:
        cpu = "N/A"
    try:
        ram = f"{virtual_memory().percent}%"
    except Exception:
        ram = "N/A"
    try:
        free_disk = get_readable_file_size(disk_usage(DOWNLOAD_DIR).free)
    except (OSError, TypeError, ValueError):
        free_disk = "N/A"
    try:
        uptime = get_readable_time(time() - bot_start_time)
    except (TypeError, ValueError):
        uptime = "N/A"
    return cpu, ram, free_disk, uptime


async def get_readable_message(sid, is_user, page_no=1, status="All", page_step=1):
    tasks = await get_specific_tasks(status, sid if is_user else None)
    task_count = len(tasks)
    if not task_count:
        if status == "All":
            return None, None
        return InputRichMessage(html=f"No Active {escape(str(status))} Tasks!<br><br>"), None

    try:
        status_limit = max(1, int(Config.STATUS_LIMIT or 10))
    except (TypeError, ValueError):
        status_limit = 10
    pages = (task_count + status_limit - 1) // status_limit
    try:
        page_no = int(page_no)
    except (TypeError, ValueError):
        page_no = 1
    page_no = ((page_no - 1) % pages) + 1

    # Keep the existing status state in sync, but don't assume it is initialized.
    try:
        if sid in status_dict and isinstance(status_dict[sid], dict):
            status_dict[sid]["page_no"] = page_no
    except (TypeError, KeyError):
        pass

    start_position = (page_no - 1) * status_limit
    if pages > 1:
        msg = f"<h3>Tasks: {task_count}/{status_limit} | Page: {page_no}/{pages}</h3>"
    else:
        msg = f"<h3>Tasks: {task_count}/{status_limit}</h3>"

    for index, task in enumerate(tasks[start_position : start_position + status_limit], start=1):
        listener = _listener(task)
        if not listener:
            continue

        if status == "All":
            try:
                task_status = await _task_status(task)
            except Exception:
                task_status = "Unknown"
        else:
            task_status = status
        safe_status = escape(str(task_status))

        message = getattr(listener, "message", None)
        link = getattr(message, "link", None) if getattr(listener, "is_super_chat", False) else None
        if link:
            safe_link = escape(str(link), quote=True)
            msg += f"<hr><b>{index + start_position}. <a href='{safe_link}'>{safe_status}</a></b>"
        else:
            msg += f"<b>{index + start_position}. {safe_status}</b>"

        subname = getattr(listener, "subname", None)
        if subname:
            msg += f"<br><i>{escape(str(subname))}</i>"

        progress_value = getattr(listener, "progress", None)
        if task_status not in (MirrorStatus.STATUS_SEED, MirrorStatus.STATUS_QUEUEUP) and progress_value:
            try:
                progress = task.progress()
            except Exception:
                progress = progress_value
            msg += f"<br>{get_progress_bar_string(progress)} » {escape(str(progress))}"

            if subname:
                subsize = f"/{get_readable_file_size(getattr(listener, 'subsize', 0))}"
                files_to_proceed = getattr(listener, "files_to_proceed", None) or []
                proceed_count = getattr(listener, "proceed_count", 0)
                count = f"{proceed_count}/{len(files_to_proceed) or '?'}"
            else:
                subsize = count = ""

            msg += f"<br><b>Processed:</b> {_safe_call(task, 'processed_bytes')}{subsize}"
            if count:
                msg += f"<br><b>Count:</b> {escape(str(count))}"
            msg += f"<br><b>Size:</b> {_safe_call(task, 'size')}"
            msg += f"<br><b>Speed:</b> {_safe_call(task, 'speed')}"
            msg += f"<br><b>ETA:</b> {_safe_call(task, 'eta')}"

            if task_status == MirrorStatus.STATUS_DOWNLOAD and (
                getattr(listener, "is_torrent", False) or getattr(listener, "is_qbit", False)
            ):
                try:
                    seeders = task.seeders_num()
                    leechers = task.leechers_num()
                    if seeders is not None and leechers is not None:
                        msg += f"<br><b>Seeders:</b> {escape(str(seeders))} | <b>Leechers:</b> {escape(str(leechers))}"
                except Exception:
                    pass

            msg += f"<br><b>Elapsed:</b> {_get_elapsed_time(task)}"
            msg += f"<br><b>Upload:</b> {_get_upload_dest(task)}"
            msg += f"<br><b>Engine:</b> {_get_engine_name(task)}"
            msg += f"<br><b>User:</b> {_get_user_display(task)}"
        elif task_status == MirrorStatus.STATUS_SEED:
            msg += f"<br><b>Size:</b> {_safe_call(task, 'size')}"
            msg += f"<br><b>Speed:</b> {_safe_call(task, 'seed_speed')}"
            msg += f"<br><b>Uploaded:</b> {_safe_call(task, 'uploaded_bytes')}"
            msg += f"<br><b>Ratio:</b> {_safe_call(task, 'ratio')}"
            msg += f" | <b>Time:</b> {_safe_call(task, 'seeding_time')}"
            msg += f"<br><b>Elapsed:</b> {_get_elapsed_time(task)}"
            msg += f"<br><b>Engine:</b> {_get_engine_name(task)}"
            msg += f"<br><b>User:</b> {_get_user_display(task)}"
        else:
            msg += f"<br><b>Size:</b> {_safe_call(task, 'size')}"
            msg += f"<br><b>Elapsed:</b> {_get_elapsed_time(task)}"
            msg += f"<br><b>Upload:</b> {_get_upload_dest(task)}"
            msg += f"<br><b>Engine:</b> {_get_engine_name(task)}"
            msg += f"<br><b>User:</b> {_get_user_display(task)}"

        try:
            gid = escape(str(task.gid()), quote=True)
        except Exception:
            continue
        msg += f"<br><tg-button type='callback_data' data='cancel canconf {gid}' style='danger'>Cancel Task</tg-button>"

        tool = getattr(task, "tool", "")
        is_selectable = (
            (tool == "aria2" and getattr(listener, "is_torrent", False))
            or tool in ("qbittorrent", "sabnzbd")
        )
        if is_selectable and task_status in (
            MirrorStatus.STATUS_DOWNLOAD,
            MirrorStatus.STATUS_PAUSED,
            MirrorStatus.STATUS_QUEUEDL,
        ):
            msg += f" | <tg-button type='callback_data' data='sel select {gid}' style='primary'>Select Files</tg-button>"

        if task_status != MirrorStatus.STATUS_UPLOAD and (
            Config.QUEUE_ALL or Config.QUEUE_DOWNLOAD or Config.QUEUE_UPLOAD
        ):
            force_upload = getattr(listener, "force_upload", False)
            force_run = getattr(listener, "force_run", False)
            if task_status == MirrorStatus.STATUS_QUEUEDL:
                msg += f" | <tg-button type='callback_data' data='force run {gid}' style='success'>ForceRun</tg-button>"
                msg += f" | <tg-button type='callback_data' data='force fd {gid}' style='success'>ForceDl</tg-button>"
                if not force_upload:
                    msg += f" | <tg-button type='callback_data' data='force fu {gid}' style='success'>ForceUp</tg-button>"
            elif not force_upload and (task_status == MirrorStatus.STATUS_DOWNLOAD or not force_run):
                msg += f" | <tg-button type='callback_data' data='force fu {gid}' style='success'>ForceUp</tg-button>"
        msg += "<br><br>"

    buttons = ButtonMaker()
    if not is_user:
        buttons.data_button("📜", f"status {sid} ov", position="header")
    if task_count > status_limit:
        buttons.data_button("<<", f"status {sid} pre", position="header")
        buttons.data_button(">>", f"status {sid} nex", position="header")
        if task_count > 30:
            for page in (1, 2, 4, 6, 8, 10, 15):
                buttons.data_button(page, f"status {sid} ps {page}", position="footer")
    if status != "All" or task_count > 20:
        for label, status_value in STATUSES.items():
            if status_value != status:
                buttons.data_button(label, f"status {sid} st {status_value}")
    buttons.data_button("♻️", f"status {sid} ref", position="header", style="green")
    button = buttons.build_menu(8)

    cpu, ram, free_disk, uptime = _system_stats()
    msg += f"<b>CPU:</b> {cpu} | <b>FREE:</b> {free_disk}"
    msg += f"<br><b>RAM:</b> {ram} | <b>UPTIME:</b> {uptime}"
    return InputRichMessage(html=msg), button
