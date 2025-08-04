import asyncio
import http.server
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

try:
  import websockets
  import websockets.exceptions
  import websockets.legacy
  import websockets.legacy.server

  HAS_WEBSOCKETS = True
except ImportError as e:
  HAS_WEBSOCKETS = False
  _WEBSOCKETS_IMPORT_ERROR = e

from pylabrobot.__version__ import STANDARD_FORM_JSON_VERSION
from pylabrobot.resources import Resource

logger = logging.getLogger("pylabrobot")


class Visualizer:
  def __init__(
    self,
    resource: Resource,
    ws_host: str = "127.0.0.1",
    ws_port: int = 2121,
    fs_host: str = "127.0.0.1",
    fs_port: int = 1337,
    open_browser: bool = True,
  ):
    self.setup_finished = False
    self._root_resource = resource
    resource.register_did_assign_resource_callback(self._handle_resource_assigned_callback)
    resource.register_did_unassign_resource_callback(self._handle_resource_unassigned_callback)

    def register_state_update(res):
      res.register_state_update_callback(
        lambda _: self._handle_state_update_callback(res)
      )
      for child in res.children:
        register_state_update(child)
    register_state_update(resource)

    self.fs_host = fs_host
    self.fs_port = fs_port
    self.open_browser = open_browser
    self._httpd: Optional[http.server.HTTPServer] = None
    self._fst: Optional[threading.Thread] = None
    self.ws_host = ws_host
    self.ws_port = ws_port
    self._id = 0
    self._websocket: Optional["websockets.legacy.server.WebSocketServerProtocol"] = None
    self._loop: Optional[asyncio.AbstractEventLoop] = None
    self._t: Optional[threading.Thread] = None
    self._stop_: Optional[asyncio.Future] = None
    self.received: List[dict] = []

  @property
  def websocket(self) -> "websockets.legacy.server.WebSocketServerProtocol":
    if self._websocket is None:
      raise RuntimeError("No websocket connection has been established.")
    return self._websocket

  @property
  def loop(self) -> asyncio.AbstractEventLoop:
    if self._loop is None:
      raise RuntimeError("Event loop has not been started.")
    return self._loop

  @property
  def t(self) -> threading.Thread:
    if self._t is None:
      raise RuntimeError("Event loop has not been started.")
    return self._t

  @property
  def stop_(self) -> asyncio.Future:
    if self._stop_ is None:
      raise RuntimeError("Event loop has not been started.")
    return self._stop_

  def _generate_id(self):
    self._id += 1
    return f"{self._id % 10000:04}"

  async def handle_event(self, event: str, data: dict):
    if event == "ping":
      await self.websocket.send(json.dumps({"event": "pong"}))

  async def _socket_handler(self, websocket: "websockets.legacy.server.WebSocketServerProtocol"):
    while True:
      try:
        message = await websocket.recv()
      except (websockets.exceptions.ConnectionClosed, asyncio.CancelledError):
        return
      data = json.loads(message)
      self.received.append(data)
      if data.get("event") == "ready":
        self._websocket = websocket
        await self._send_resources_and_state()
      if "event" in data:
        await self.handle_event(data.get("event"), data)
      else:
        logger.warning("Unhandled message: %s", message)

  def _assemble_command(self, event: str, data: Dict[str, Any]) -> Tuple[str, str]:
    id_ = self._generate_id()
    return json.dumps({
      "id": id_, "version": STANDARD_FORM_JSON_VERSION, "data": data, "event": event
    }), id_

  def has_connection(self) -> bool:
    return self._websocket is not None

  async def send_command(
    self, event: str, data: Optional[Dict[str, Any]] = None, wait_for_response: bool = True
  ) -> Optional[dict]:
    if data is None: data = {}
    serialized_data, id_ = self._assemble_command(event=event, data=data)
    if wait_for_response and not self.has_connection():
      raise RuntimeError("Cannot wait for response when no websocket connection is established.")
    if self.has_connection():
      await self.websocket.send(serialized_data)
      if wait_for_response:
        while True:
          if len(self.received) > 0:
            message = self.received.pop()
            if "id" in message and message["id"] == id_:
              break
          await asyncio.sleep(0.1)
        if not message["success"]:
          raise RuntimeError(f"Error during event {event}: {message.get('error', 'unknown error')}")
        return message
    return None

  @property
  def httpd(self) -> http.server.HTTPServer:
    if self._httpd is None:
      raise RuntimeError("The HTTP server has not been started yet.")
    return self._httpd

  @property
  def fst(self) -> threading.Thread:
    if self._fst is None:
      raise RuntimeError("The file server thread has not been started yet.")
    return self._fst

  async def setup(self):
    if self.setup_finished:
      raise RuntimeError("The visualizer has already been started.")
    await self._run_ws_server()
    self._run_file_server()
    self.setup_finished = True

  async def _run_ws_server(self):
    if not HAS_WEBSOCKETS:
      raise RuntimeError(f"Websockets not installed. Import error: {_WEBSOCKETS_IMPORT_ERROR}")
    async def run_server():
      self._stop_ = self.loop.create_future()
      while True:
        try:
          async with websockets.legacy.server.serve(
            self._socket_handler, self.ws_host, self.ws_port
          ):
            print(f"Websocket server started at ws://{self.ws_host}:{self.ws_port}")
            lock.release()
            await self.stop_
            break
        except asyncio.CancelledError: pass
        except OSError: self.ws_port += 1
    def start_loop():
      self.loop.run_until_complete(run_server())
    lock = threading.Lock()
    lock.acquire()
    self._loop = asyncio.new_event_loop()
    self._t = threading.Thread(target=start_loop, daemon=True)
    self.t.start()
    while lock.locked(): time.sleep(0.001)

  def _run_file_server(self):
    dirname = os.path.dirname(__file__)
    path = os.path.join(dirname, ".")
    if not os.path.exists(path):
      raise RuntimeError("Could not find Visualizer files.")
    in_colab = 'google.colab' in __import__('sys').modules
    ws_url_for_js, fs_proxy_url = "", ""
    if in_colab:
      from google.colab.output import eval_js
      from urllib.parse import urlparse
      fs_proxy_url = eval_js(f'google.colab.kernel.proxyPort({self.fs_port})')
      ws_proxy_url_http = eval_js(f'google.colab.kernel.proxyPort({self.ws_port})')
      parsed_url = urlparse(ws_proxy_url_http)
      ws_url_for_js = f"wss://{parsed_url.netloc}{parsed_url.path}"

    def start_server(lock):
      ws_host, ws_port, fs_host, fs_port = (
        self.ws_host, self.ws_port, self.fs_host, self.fs_port)
      class QuietSimpleHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs): super().__init__(*args, directory=path, **kwargs)
        def log_message(self, format, *args): pass
        def do_GET(self) -> None:
          if self.path == "/":
            with open(os.path.join(path, "index.html"), "r", encoding="utf-8") as f:
              content = f.read()
            content = content.replace("{{ ws_url }}", ws_url_for_js)
            content = content.replace("{{ fs_host }}", fs_host)
            content = content.replace("{{ fs_port }}", str(fs_port))
            content = content.replace("{{ ws_host }}", ws_host)
            content = content.replace("{{ ws_port }}", str(ws_port))
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(content.encode("utf-8"))
          else:
            super().do_GET()
      while True:
        try:
          self._httpd = http.server.HTTPServer((self.fs_host, self.fs_port), QuietSimpleHTTPRequestHandler)
          if not in_colab:
            print(f"File server started at http://{self.fs_host}:{self.fs_port}")
          lock.release()
          break
        except OSError: self.fs_port += 1
      self.httpd.serve_forever()

    lock = threading.Lock()
    lock.acquire()
    self._fst = threading.Thread(name="visualizer_fs", target=start_server, args=(lock,), daemon=True)
    self.fst.start()
    while lock.locked(): time.sleep(0.001)
    if in_colab:
      print(f"Visualizer is running. Please open this URL in a new tab: {fs_proxy_url}")
    elif self.open_browser:
      import webbrowser
      webbrowser.open(f"http://{self.fs_host}:{self.fs_port}")

  async def stop(self):
    if self._httpd is not None:
      self.httpd.shutdown()
      self.httpd.server_close()
      self._httpd = None
    if self._fst is not None: self._fst = None
    if self.has_connection() and self._loop and self._loop.is_running():
      try:
        await self.send_command("stop", wait_for_response=False)
      except (websockets.exceptions.ConnectionClosedOK, websockets.exceptions.ConnectionClosedError):
        pass
      self.loop.call_soon_threadsafe(self.stop_.set_result, "done")
      self._t = None
    self.received.clear()
    self._websocket = None
    if self._loop and not self._loop.is_running():
      self._loop.close()
    self._loop = None
    self._stop_ = None
    self.setup_finished = False

  async def _send_resources_and_state(self):
    await self.send_command("set_root_resource", {"resource": self._root_resource.serialize()}, wait_for_response=False)
    state: Dict[str, Any] = {}
    def save_resource_state(resource: Resource):
      if hasattr(resource, "tracker"):
        resource_state = resource.tracker.serialize()
        if resource_state is not None:
          state[resource.name] = resource_state
      for child in resource.children:
        save_resource_state(child)
    save_resource_state(self._root_resource)
    await self.send_command("set_state", state, wait_for_response=False)

  def _handle_resource_assigned_callback(self, resource: Resource):
    def register_state_update(res: Resource):
      res.register_state_update_callback(lambda _: self._handle_state_update_callback(res))
      for child in res.children: register_state_update(child)
    register_state_update(resource)
    data = {
      "resource": resource.serialize(),
      "state": resource.serialize_all_state(),
      "parent_name": (resource.parent.name if resource.parent else None),
    }
    fut = self.send_command(event="resource_assigned", data=data, wait_for_response=False)
    asyncio.run_coroutine_threadsafe(fut, self.loop)

  def _handle_resource_unassigned_callback(self, resource: Resource):
    data = {"resource_name": resource.name}
    fut = self.send_command(event="resource_unassigned", data=data, wait_for_response=False)
    asyncio.run_coroutine_threadsafe(fut, self.loop)

  def _handle_state_update_callback(self, resource: Resource):
    data = {resource.name: resource.serialize_state()}
    fut = self.send_command(event="set_state", data=data, wait_for_response=False)
    asyncio.run_coroutine_threadsafe(fut, self.loop)