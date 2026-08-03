r"""
Discord Presence Relay - SENDER
Run this on the GAMING PC (the one WITHOUT Discord open).

It creates a fake \\.\pipe\discord-ipc-0 named pipe so games think Discord
is running locally. It performs the handshake, decodes SET_ACTIVITY frames,
and forwards the activity payload over a plain TCP socket to the receiver
running on the PC that actually has Discord open.

Requirements:
    pip install pywin32

Windows only.
"""

import json
import logging
import queue
import socket
import struct
import threading
import time
import tkinter as tk
from tkinter import ttk, scrolledtext

import win32file
import win32pipe
import winerror
import pywintypes

PIPE_NAME = r"\\.\pipe\discord-ipc-0"
LOG_FILE = "relay_sender.log"

OP_HANDSHAKE = 0
OP_FRAME = 1
OP_CLOSE = 2
OP_PING = 3
OP_PONG = 4

HEADER = struct.Struct("<II")          # opcode, length      (Discord IPC framing)
NET_HEADER = struct.Struct(">I")       # length prefix        (our own TCP framing)


# --------------------------------------------------------------------------- #
# Logging: standard logging module, mirrored into the GUI via a queue handler
# --------------------------------------------------------------------------- #

log_queue: "queue.Queue[str]" = queue.Queue()


class QueueLogHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(self.format(record))


logger = logging.getLogger("relay_sender")
logger.setLevel(logging.DEBUG)

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_file_handler)

_queue_handler = QueueLogHandler()
_queue_handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", "%H:%M:%S"))
logger.addHandler(_queue_handler)


# --------------------------------------------------------------------------- #
# Pipe <-> TCP relay worker
# --------------------------------------------------------------------------- #

class RelayWorker:
    def __init__(self, host: str, port: int, status_cb):
        self.host = host
        self.port = port
        self.status_cb = status_cb   # callback(str) to update GUI status label
        self._stop = threading.Event()
        self._thread = None
        self._sock = None
        self._sock_lock = threading.Lock()

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        with self._sock_lock:
            if self._sock:
                try:
                    self._sock.close()
                except OSError:
                    pass
        self.status_cb("Stopped")

    # -- TCP side ------------------------------------------------------- #

    def _ensure_connected(self) -> bool:
        with self._sock_lock:
            if self._sock is not None:
                return True
        try:
            s = socket.create_connection((self.host, self.port), timeout=3)
            with self._sock_lock:
                self._sock = s
            logger.info("Connected to receiver %s:%s", self.host, self.port)
            return True
        except OSError as e:
            logger.warning("Could not reach receiver %s:%s (%s)", self.host, self.port, e)
            return False

    def _send(self, payload: dict):
        if not self._ensure_connected():
            return
        data = json.dumps(payload).encode("utf-8")
        try:
            with self._sock_lock:
                self._sock.sendall(NET_HEADER.pack(len(data)) + data)
        except OSError as e:
            logger.warning("Send failed, dropping connection: %s", e)
            with self._sock_lock:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

    # -- Pipe side -------------------------------------------------------- #

    def _run(self):
        while not self._stop.is_set():
            handle = None
            try:
                handle = win32pipe.CreateNamedPipe(
                    PIPE_NAME,
                    win32pipe.PIPE_ACCESS_DUPLEX,
                    win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT,
                    1, 65536, 65536, 0, None,
                )
            except pywintypes.error as e:
                logger.error("Could not create pipe (%s). Is Discord or another relay already running?", e)
                time.sleep(3)
                continue

            self.status_cb("Waiting for a game to connect...")
            logger.info("Waiting for a game to connect to %s", PIPE_NAME)
            try:
                win32pipe.ConnectNamedPipe(handle, None)
            except pywintypes.error as e:
                logger.error("ConnectNamedPipe failed: %s", e)
                win32file.CloseHandle(handle)
                continue

            self.status_cb("Game connected - forwarding")
            logger.info("Game connected to the pipe")
            client_id = None
            try:
                client_id = self._handle_session(handle)
            except Exception:
                logger.exception("Error while handling session")
            finally:
                try:
                    win32file.CloseHandle(handle)
                except pywintypes.error:
                    pass
                if client_id:
                    self._send({"type": "clear", "client_id": client_id})
                self.status_cb("Game disconnected - waiting...")
                logger.info("Game disconnected")

    def _read_exact(self, handle, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            _, chunk = win32file.ReadFile(handle, n - len(buf))
            if not chunk:
                raise ConnectionError("pipe closed")
            buf += chunk
        return buf

    def _write_frame(self, handle, opcode: int, payload: dict):
        data = json.dumps(payload).encode("utf-8")
        win32file.WriteFile(handle, HEADER.pack(opcode, len(data)) + data)

    def _handle_session(self, handle):
        client_id = None
        while not self._stop.is_set():
            header = self._read_exact(handle, 8)
            opcode, length = HEADER.unpack(header)
            payload_bytes = self._read_exact(handle, length) if length else b""
            payload = json.loads(payload_bytes) if payload_bytes else {}

            if opcode == OP_HANDSHAKE:
                client_id = payload.get("client_id")
                logger.info("Handshake from client_id=%s", client_id)
                self._write_frame(handle, OP_FRAME, {
                    "cmd": "DISPATCH",
                    "evt": "READY",
                    "nonce": None,
                    "data": {
                        "v": 1,
                        "config": {"api_endpoint": "//discord.com/api",
                                   "cdn_host": "cdn.discordapp.com",
                                   "environment": "production"},
                        "user": {"id": "0", "username": "relay",
                                 "discriminator": "0000", "avatar": None},
                    },
                })

            elif opcode == OP_FRAME:
                cmd = payload.get("cmd")
                nonce = payload.get("nonce")
                if cmd == "SET_ACTIVITY":
                    activity = (payload.get("args") or {}).get("activity")
                    logger.info("SET_ACTIVITY: %s", _summarize(activity))
                    self._send({"type": "activity", "client_id": client_id, "activity": activity})
                    self._write_frame(handle, OP_FRAME, {
                        "cmd": "SET_ACTIVITY", "evt": None, "nonce": nonce, "data": activity,
                    })
                else:
                    # Unhandled command - ack politely so the client doesn't hang.
                    self._write_frame(handle, OP_FRAME, {
                        "cmd": cmd, "evt": None, "nonce": nonce, "data": {},
                    })

            elif opcode == OP_CLOSE:
                break


def _summarize(activity):
    if not activity:
        return "(cleared)"
    return f"details={activity.get('details')!r} state={activity.get('state')!r}"


# --------------------------------------------------------------------------- #
# GUI
# --------------------------------------------------------------------------- #

class App:
    def __init__(self, root):
        self.root = root
        root.title("Discord Presence Relay - Sender")
        root.geometry("520x420")
        root.minsize(480, 360)

        frm = ttk.Frame(root, padding=10)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Receiver IP:").grid(row=0, column=0, sticky="w")
        self.ip_var = tk.StringVar(value="192.168.1.100")
        ttk.Entry(frm, textvariable=self.ip_var, width=20).grid(row=0, column=1, sticky="w", padx=5)

        ttk.Label(frm, text="Port:").grid(row=0, column=2, sticky="w")
        self.port_var = tk.StringVar(value="23456")
        ttk.Entry(frm, textvariable=self.port_var, width=8).grid(row=0, column=3, sticky="w", padx=5)

        self.start_btn = ttk.Button(frm, text="Start", command=self.toggle)
        self.start_btn.grid(row=0, column=4, padx=5)

        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(frm, textvariable=self.status_var, foreground="#0a6").grid(
            row=1, column=0, columnspan=5, sticky="w", pady=(8, 4))

        self.log_box = scrolledtext.ScrolledText(frm, state="disabled", height=18, wrap="word")
        self.log_box.grid(row=2, column=0, columnspan=5, sticky="nsew")
        frm.rowconfigure(2, weight=1)
        for c in range(5):
            frm.columnconfigure(c, weight=1 if c == 1 else 0)

        self.worker = None
        self._poll_log()

    def toggle(self):
        if self.worker is None:
            try:
                port = int(self.port_var.get())
            except ValueError:
                self.status_var.set("Invalid port")
                return
            self.worker = RelayWorker(self.ip_var.get().strip(), port, self.status_var.set)
            self.worker.start()
            self.start_btn.config(text="Stop")
        else:
            self.worker.stop()
            self.worker = None
            self.start_btn.config(text="Start")

    def _poll_log(self):
        try:
            while True:
                line = log_queue.get_nowait()
                self.log_box.configure(state="normal")
                self.log_box.insert("end", line + "\n")
                self.log_box.see("end")
                self.log_box.configure(state="disabled")
        except queue.Empty:
            pass
        self.root.after(150, self._poll_log)


if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()
