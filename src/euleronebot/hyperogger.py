import datetime
import logging
import os
import re
import sys
import traceback
from pathlib import Path
from typing import Any, ClassVar

from .utils import NerdICONs, color_txt, rgb

__all__ = ["Levels", "Logger"]

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_PRIVATE_USE_RE = re.compile("[\ue000-\uf8ff]")  # Nerd Font 图标，文件里不需要


class _FileSink:
    """
    把日志同时写进文件：每行写完立刻刷新，所以可以用 tail -f 之类实时查看，
    重定向 / 管道场景下也不会因为缓冲而迟迟看不到输出。

    - 位置：当前工作目录下的 logs/euler-YYYY-MM-DD.log（和 appconfig.json 同一目录），按天分文件，
      只保留最近 ``KEEP_DAYS`` 天；
    - 内容：去掉颜色码和图标字符的纯文本，UTF-8；
    - 环境变量 ``EULER_LOG_FILE=0`` 可以关闭；
    - 写文件出任何问题都只会关闭文件日志，绝不影响程序本身和控制台输出。
    """

    KEEP_DAYS = 14

    def __init__(self) -> None:
        self._file = None
        self._day: datetime.date | None = None
        self._enabled = os.environ.get("EULER_LOG_FILE", "1").strip().lower() not in ("0", "false", "off", "no")

    def write(self, text: str) -> None:
        if not self._enabled:
            return
        try:
            today = datetime.date.today()
            if self._file is None or today != self._day:
                self._open(today)
            line = _PRIVATE_USE_RE.sub("", _ANSI_RE.sub("", text)).rstrip("\n")
            self._file.write(line + "\n")  # type: ignore[union-attr]
            self._file.flush()  # type: ignore[union-attr]
        except Exception as e:  # noqa: BLE001
            self._enabled = False
            print(f"[euler.log] 写日志文件失败，已关闭文件日志: {e!r}", file=sys.stderr)

    def _open(self, today: datetime.date) -> None:
        if self._file is not None:
            try:
                self._file.close()
            except Exception:  # noqa: BLE001
                pass
        directory = Path("logs")
        directory.mkdir(parents=True, exist_ok=True)
        self._file = open(directory / f"euler-{today.isoformat()}.log", "a", encoding="utf-8")  # noqa: SIM115
        self._day = today
        self._prune(directory, today)

    def _prune(self, directory: Path, today: datetime.date) -> None:
        for f in directory.glob("euler-*.log"):
            try:
                day = datetime.date.fromisoformat(f.stem.removeprefix("euler-"))
            except ValueError:
                continue
            if (today - day).days > self.KEEP_DAYS:
                try:
                    f.unlink()
                except OSError:
                    pass


_file_sink = _FileSink()


def _emit(content: str) -> None:
    """控制台和日志文件一起输出，并且立刻刷新"""
    print(content, flush=True)
    _file_sink.write(content)


class Levels:
    def __init__(self, nf_icons: NerdICONs):
        self.nf_icons = nf_icons
        self.TRACE = color_txt(f"|{nf_icons.nf_cod_debug_breakpoint_log} Trace    |", rgb(184, 255, 254))
        self.INFO = color_txt(f"|{nf_icons.nf_fa_circle_info} Info     |", rgb(90, 221, 225))
        self.WARNING = color_txt(f"|{nf_icons.nf_fa_warn} Warning  |", rgb(82, 171, 237))
        self.ERROR = color_txt(f"|{nf_icons.nf_cod_error} Error    |", rgb(255, 48, 70))
        self.CRITICAL = color_txt(f"|{nf_icons.nf_cod_bracket_error} Critical |", rgb(178, 33, 48))
        self.DEBUG = color_txt(f"|{nf_icons.nf_cod_debug_alt} Debug    |", rgb(93, 227, 144))

        self.level_nums = {
            self.DEBUG: -2,
            self.TRACE: -1,
            self.INFO: 0,
            self.WARNING: 1,
            self.ERROR: 2,
            self.CRITICAL: 3,
        }

        self.level_names = {
            "TRACE": self.TRACE,
            "INFO": self.INFO,
            "WARNING": self.WARNING,
            "ERROR": self.ERROR,
            "CRITICAL": self.CRITICAL,
            "DEBUG": self.DEBUG,
        }


class Logger:
    running_loggers: ClassVar[dict[str, "Logger"]] = {}

    def __init__(self, use_nf: bool = True, key: str | None = None):
        self.levels = Levels(NerdICONs(use_nf))
        self._use_nf = use_nf
        self._key = key
        self.log_level = self.levels.INFO
        self.log_level_text = "INFO"

    @classmethod
    def create(cls, key: str, level: str, use_nf: bool = True):
        c = cls(use_nf, key=key)
        c.set_level(level)
        cls.running_loggers[key] = c
        return c

    @classmethod
    def fetch(cls, key: str) -> "Logger":
        if key not in cls.running_loggers:
            cls.running_loggers[key] = cls(key=key)
        return cls.running_loggers[key]

    def set_level(self, level: str):
        if level in self.levels.level_names:
            self.log_level = self.levels.level_names[level]
            self.log_level_text = level
        else:
            self.log("未知的日志等级", self.levels.ERROR)

        return self

    @staticmethod
    def format_exec():
        exc_type, exc_value, exc_traceback = sys.exc_info()
        formatted = color_txt("\nTraceback: \n\n", rgb(255, 47, 47))
        tb_frames = traceback.extract_tb(exc_traceback)
        file_txt = color_txt("File", rgb(85, 173, 238))
        line_txt = color_txt("line", rgb(85, 173, 238))
        for frame in tb_frames:
            filename, lineno, func_name, code = frame
            formatted += (
                f"  {file_txt} {color_txt(filename, rgb(104, 255, 244))},"
                f" {line_txt} {color_txt(str(lineno), rgb(215, 255, 255))},"
                f" in {color_txt(func_name, rgb(70, 172, 107))}\n"
                f"      {color_txt(code, rgb(255, 255, 255))}\n\n"
            )
        formatted += f"{color_txt(exc_type.__name__, rgb(255, 47, 47))}: "  # type: ignore
        formatted += color_txt(exc_value, rgb(255, 255, 255)) + "\n"  # type: ignore

        return formatted

    def register_hook(self) -> None:
        def hook(_exc_t: Any, _exc_v: Any, _exc_tb: Any) -> None:
            self.error(self.format_exec())

        sys.excepthook = hook

    def name_custom(self, name: str) -> "Logger":
        new_logger = type(self)(self._use_nf, key=self._key)
        new_logger._set_name_bind(name)
        return new_logger

    def _set_name_bind(self, name: str) -> None:
        def nlog(message: str, level: str):
            patch = color_txt(f"[{name}]", rgb(97, 192, 224))
            self._log(f"{patch} {color_txt(message, rgb(215, 255, 255))}", level)

        self.log = nlog

    def _effective_level(self) -> str:
        if self._key and self._key in Logger.running_loggers:
            return Logger.running_loggers[self._key].log_level_text
        return self.log_level_text

    def _log(self, message: str, level: str) -> None:
        if self.levels.level_nums[level] < self.levels.level_nums[self.levels.level_names[self._effective_level()]]:
            return
        time = color_txt(
            self.levels.nf_icons.nf_weather_time_4 + " " + str(datetime.datetime.now())[:-4],
            rgb(65, 128, 176),
        )
        if "\n" in message:
            listed = message.split("\n")
            for idx, i in enumerate(listed):
                if idx == 0:
                    listed[0] = "\n"
                    content = f" {time} {level} {color_txt(i, rgb(215, 255, 255))}"
                else:
                    content = " " * int((len(f"{time}{level}") - 2) / 2) + color_txt(i, rgb(215, 255, 255))
                _emit(content)
        else:
            content = f" {time} {level} {color_txt(message, rgb(215, 255, 255))}"
            _emit(content)

    def log(self, message: str, level: str) -> None:
        self._log(message, level)

    def info(self, message: Any) -> None:
        self.log(str(message), self.levels.INFO)

    def warning(self, message: Any) -> None:
        self.log(str(message), self.levels.WARNING)

    def error(self, message: Any) -> None:
        self.log(str(message), self.levels.ERROR)

    def critical(self, message: Any) -> None:
        self.log(str(message), self.levels.CRITICAL)

    def debug(self, message: Any) -> None:
        self.log(str(message), self.levels.DEBUG)

    def trace(self, message: Any) -> None:
        self.log(str(message), self.levels.TRACE)

    def set_handler(self) -> None:
        custom_logger = self

        class StdHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                msg = self.format(record)
                patch = color_txt(f"[{record.name}]", rgb(97, 192, 224))
                msg = f"{patch} {color_txt(msg, rgb(215, 255, 255))}"
                match record.levelno:
                    case logging.CRITICAL:
                        custom_logger.critical(msg)
                    case logging.ERROR:
                        custom_logger.error(msg)
                    case logging.WARNING:
                        custom_logger.warning(msg)
                    case logging.INFO:
                        custom_logger.info(msg)
                    case logging.DEBUG:
                        custom_logger.trace(msg)
                    case _:
                        custom_logger.debug(msg)

        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(StdHandler())
