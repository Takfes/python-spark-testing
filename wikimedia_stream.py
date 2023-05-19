from socketIO_client import SocketIO, BaseNamespace
import json


class WikiNamespace(BaseNamespace):
    def on_change(self, change):
        print(json.dumps(change))

    def on_connect(self):
        self.emit("subscribe", "en.wikipedia.org")


socketIO = SocketIO("stream.wikimedia.org", 80)
socketIO.define(WikiNamespace, "/rc")

while True:
    socketIO.wait(10)
