"""
Discord Presence Relay - RECEIVER
Run this on the PC that has Discord open.

Listens on a TCP port for activity data forwarded by the sender (running on
the gaming PC), and applies it to your local Discord client using pypresence
- a normal, legitimate IPC client, so no raw pipe forwarding happens here.

Requirements:
    pip install pypresence

Works on Windows, macOS, Linux (wherever Discord + pypresence run).
"""

import json
import logging
import queue
import socket
import struct
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext

from pypresence import Presence

LOG_FILE = "relay_receiver.log"
NET_HEADER = struct.Struct(">I")

# Fields pypresence's Presence.update() accepts. We flatten Discord's raw
# "activity" JSON (from SET_ACTIVITY) into these before calling update().
_UPDATE_KEYS = {
    "state", "details", "start", "end", "large_image", "large_text",
    "small_image", "small_text", "party_id", "party_size", "join",
    "spectate", "match", "buttons", "instance",
}


def activity_to_kwargs(activity: dict) -> dict:
    if not activity:
        return {}
    kwargs = {}
    if "details" in activity:
        kwargs["details"] = activity.get("details")
    if "state" in activity:
        kwargs["state"] = activity.get("state")
    ts = activity.get("timestamps") or {}
    if "start" in ts:
        kwargs["start"] = ts["start"]
    if "end" in ts:
        kwargs["end"] = ts["end"]
    assets = activity.get("assets") or {}
    if "large_image" in assets:
        kwargs["large_image"] = assets["large_image"]
    if "large_text" in assets:
        kwargs["large_text"] = assets["large_text"]
    if "small_image" in assets:
        kwargs["small_image"] = assets["small_image"]
    if "small_text" in assets:
        kwargs["small_text"] = assets["small_text"]
    party = activity.get("party") or {}
    if "id" in party:
        kwargs["party_id"] = party["id"]
    size = party.get("size")
    if isinstance(size, (list, tuple)) and len(size) == 2:
        kwargs["party_size"] = list(size)
    if activity.get("buttons"):
        kwargs["buttons"] = activity["buttons"]
    return {k: v for k, v in kwargs.items() if k in _UPDATE_KEYS or True}


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

log_queue: "queue.Queue[str]" = queue.Queue()


class QueueLogHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(self.format(record))


logger = logging.getLogger("relay_receiver")
logger.setLevel(logging.DEBUG)

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_file_handler)

_queue_handler = QueueLogHandler()
_queue_handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", "%H:%M:%S"))
logger.addHandler(_queue_handler)


# --------------------------------------------------------------------------- #
# Server
# --------------------------------------------------------------------------- #

class ReceiverServer:
    def __init__(self, host: str, port: int, status_cb):
        self.host = host
        self.port = port
        self.status_cb = status_cb
        self._stop = threading.Event()
        self._srv_sock = None
        self._thread = None
        self._presences = {}          # client_id -> Presence
        self._presences_lock = threading.Lock()

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._srv_sock:
            try:
                self._srv_sock.close()
            except OSError:
                pass
        with self._presences_lock:
            for cid, rpc in self._presences.items():
                try:
                    rpc.close()
                except Exception:
                    pass
            self._presences.clear()
        self.status_cb("Stopped")

    def _run(self):
        try:
            self._srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._srv_sock.bind((self.host, self.port))
            self._srv_sock.listen(5)
        except OSError as e:
            logger.error("Could not bind %s:%s (%s)", self.host, self.port, e)
            self.status_cb(f"Bind failed: {e}")
            return

        self.status_cb(f"Listening on {self.host}:{self.port}")
        logger.info("Listening on %s:%s", self.host, self.port)
        while not self._stop.is_set():
            try:
                conn, addr = self._srv_sock.accept()
            except OSError:
                break
            logger.info("Sender connected from %s", addr)
            self.status_cb(f"Connected: {addr[0]}")
            threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()

    def _handle_conn(self, conn: socket.socket):
        try:
            while not self._stop.is_set():
                header = self._recv_exact(conn, 4)
                if header is None:
                    break
                (length,) = NET_HEADER.unpack(header)
                body = self._recv_exact(conn, length)
                if body is None:
                    break
                try:
                    msg = json.loads(body)
                except json.JSONDecodeError:
                    logger.warning("Bad JSON from sender, dropping message")
                    continue
                self._handle_message(msg)
        except OSError as e:
            logger.info("Sender connection ended: %s", e)
        finally:
            try:
                conn.close()
            except OSError:
                pass
            self.status_cb("Waiting for sender...")

    @staticmethod
    def _recv_exact(conn, n):
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    def _handle_message(self, msg: dict):
        mtype = msg.get("type")
        client_id = msg.get("client_id")
        if not client_id:
            logger.warning("Message missing client_id, ignoring")
            return

        if mtype == "clear":
            rpc = self._presences.pop(client_id, None)
            if rpc:
                try:
                    rpc.clear()
                except Exception:
                    pass
            logger.info("Cleared presence for client_id=%s", client_id)
            return

        if mtype == "activity":
            activity = msg.get("activity")
            rpc = self._get_presence(client_id)
            if rpc is None:
                return
            if not activity:
                try:
                    rpc.clear()
                except Exception as e:
                    logger.warning("clear() failed: %s", e)
                return
            kwargs = activity_to_kwargs(activity)
            try:
                rpc.update(**kwargs)
                logger.info("Updated presence: details=%r state=%r",
                            kwargs.get("details"), kwargs.get("state"))
            except Exception as e:
                logger.warning("update() failed (%s), reconnecting...", e)
                self._presences.pop(client_id, None)

    def _get_presence(self, client_id: str):
        with self._presences_lock:
            rpc = self._presences.get(client_id)
            if rpc:
                return rpc
            try:
                rpc = Presence(client_id)
                rpc.connect()
                self._presences[client_id] = rpc
                logger.info("Connected to local Discord as client_id=%s", client_id)
                return rpc
            except Exception as e:
                logger.error("Could not connect to local Discord (%s). Is Discord running?", e)
                return None


def local_ips():
    ips = ["0.0.0.0"]
    try:
        hostname = socket.gethostname()
        for ip in socket.gethostbyname_ex(hostname)[2]:
            if ip not in ips:
                ips.append(ip)
    except OSError:
        pass
    return ips


# --------------------------------------------------------------------------- #
# GUI
# --------------------------------------------------------------------------- #

class App:
    def __init__(self, root):
        self.root = root
        root.title("Discord Presence Relay - Receiver")
        root.geometry("560x440")
        root.minsize(500, 380)

        frm = ttk.Frame(root, padding=10)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Listen IP:").grid(row=0, column=0, sticky="w")
        self.ip_var = tk.StringVar(value="0.0.0.0")
        self.ip_combo = ttk.Combobox(frm, textvariable=self.ip_var, values=local_ips(), width=17)
        self.ip_combo.grid(row=0, column=1, sticky="w", padx=5)

        ttk.Label(frm, text="Port:").grid(row=0, column=2, sticky="w")
        self.port_var = tk.StringVar(value="23456")
        ttk.Entry(frm, textvariable=self.port_var, width=8).grid(row=0, column=3, sticky="w", padx=5)

        self.start_btn = ttk.Button(frm, text="Start", command=self.toggle)
        self.start_btn.grid(row=0, column=4, padx=5)

        ttk.Label(frm, text="(Client ID comes automatically from each game's handshake)",
                  foreground="#888").grid(row=1, column=0, columnspan=5, sticky="w", pady=(2, 6))

        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(frm, textvariable=self.status_var, foreground="#0a6").grid(
            row=2, column=0, columnspan=5, sticky="w", pady=(0, 4))

        self.log_box = scrolledtext.ScrolledText(frm, state="disabled", height=18, wrap="word")
        self.log_box.grid(row=3, column=0, columnspan=5, sticky="nsew")
        frm.rowconfigure(3, weight=1)
        for c in range(5):
            frm.columnconfigure(c, weight=1 if c == 1 else 0)

        self.server = None
        self._poll_log()

    def toggle(self):
        if self.server is None:
            try:
                port = int(self.port_var.get())
            except ValueError:
                self.status_var.set("Invalid port")
                return
            self.server = ReceiverServer(self.ip_var.get().strip(), port, self.status_var.set)
            self.server.start()
            self.start_btn.config(text="Stop")
        else:
            self.server.stop()
            self.server = None
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
