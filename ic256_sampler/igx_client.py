# install: pip install websocket-client msgpack // python3

import websocket
import time
import msgpack
import requests


# usage
# ip = "10.11.25.105"
# client = IGXWebsocketClient(ip)
# client.startCollect(delay=0.1, duration=10, fields=fields, onMessage=onMessageEvent)
class IGXField:
    def __init__(self, client, path):
        self.client = client
        self.path = path
        self.datum = [None, None]
        self.datums = []

    def getPath(self):
        return self.path

    def setValue(self, value):
        self.client.sendSetEvent({self.path: value})

    def toggle(self):
        self.setValue(True)
        time.sleep(0.2)
        self.setValue(False)

    def get(self, message):
        """Extract this field data from message."""
        if "data" not in message:
            return []
        data = message["data"]
        return data.get(self.path, [])

    def update(self, message):
        self.datums = self.get(message)
        if self.datums:
            self.datum = self.datums[-1]

    def getDatums(self):
        return self.datums
    
    def clearDatums(self):
        """Clear the datums list after reading to avoid re-processing old data."""
        self.datums = []

    def getValue(self):
        return self.datum[0]

    def getTime(self):
        return self.datum[1]

    def isNull(self):
        return self.datum[1] is None

    def isEqual(self, v):
        return self.getValue() == v

    def isNotEqual(self, v):
        return not self.isEqual(v)


class Component:
    def __init__(self, client, path):
        self.client = client
        self.path = path

    def child(self, name):
        return Component(self.client, self.path + "/" + name)

    def io(self, name):
        return IGXIO(self.client, self.path + "/" + name)


class IGXIO:
    def __init__(self, client, path):
        self.path = path
        self.valueField = IGXField(client, path + "/value")

    def getPath(self):
        return self.path

    def getValueField(self):
        return self.valueField

    def getValue(self):
        return self.valueField.getValue()

    def setValue(self, value):
        return self.valueField.setValue(value)

    def getTime(self, message):
        return self.valueField.getTime()

    def isNull(self):
        return self.valueField.isNull()

    def isEqual(self, v):
        return self.valueField.isEqual(v)

    def isNotEqual(self, v):
        return self.valueField.isNotEqual(v)

    def expectEqual(self, v):
        if self.isNotEqual(v):
            print(
                "Error Equal io:",
                self.getPath(),
                " value:",
                self.getValue(),
                ", expect:",
                v,
            )

    def expectNotEqual(self, v):
        if self.isEqual(v):
            print(
                "Error Not Equal, io:",
                self.getPath(),
                " value:",
                self.getValue(),
                ", not expect:",
                v,
            )


class ButtonIO(IGXIO):
    def toggle(self):
        self.valueField.toggle()


class UploadIO(ButtonIO):
    def __init__(self, client, path, target=""):
        super().__init__(client, path)
        self.target = f"http://{client.getHostIP()}{target}"

    def setTarget(self, target):
        self.target = f"http://{self.valueField.client.getHostIP()}{target}"

    def upload(self, filePath):
        with open(filePath, "rb") as file:
            content = file.read()
            requests.put(self.target, content)
            super().toggle()


class IGXWebsocketClient:
    def __init__(self, ip=""):
        self.ip = ip
        self.subscribedFields = {}
        self.ws = self._create_connection() if ip else ""
    
    def _create_connection(self):
        """Create websocket connection, trying MessagePack subprotocol first."""
        try:
            return websocket.create_connection(
                f"ws://{self.ip}",
                subprotocols=["mpack"],
                timeout=5
            )
        except (websocket.WebSocketException, ValueError, OSError):
            return websocket.create_connection(f"ws://{self.ip}", timeout=5)

    def sendEventData(self, event, data=None):
        if not self.ws or not self.ws.connected:
            return

        try:
            message = {"event": event, "data": data}
            packed = msgpack.packb(message, use_bin_type=True)
            self.ws.send(packed)
        except (ConnectionAbortedError, ConnectionResetError, OSError):
            if not self.ws.connected:
                try:
                    self.reconnect()
                    if self.ws.connected:
                        message = {"event": event, "data": data}
                        packed = msgpack.packb(message, use_bin_type=True)
                        self.ws.send(packed)
                except Exception:
                    pass  # Keep-alive thread will handle reconnection

    def sendSubscribeEvent(self, fields):
        self.sendEventData("subscribe", {key: False for key in fields.keys()})

    def sendSubscribeFields(self, fields):
        self.subscribedFields = fields
        field_data = {f.getPath(): b for f, b in fields.items()}
        self.sendEventData("subscribe", field_data)

    def sendSubscribeIOs(self, ios):
        fields = {io.getValueField(): b for io, b in ios.items()}
        self.sendSubscribeFields(fields)

    def sendGetEventMessage(self):
        self.sendEventData("get")

    def sendSetEvent(self, data):
        self.sendEventData("set", data)

    def waitRecv(self):
        """Wait for and receive a message from the websocket."""
        if not self.ws:
            return {}

        try:
            dm = self.ws.recv()
            if isinstance(dm, bytes):
                m = msgpack.unpackb(dm, raw=False)
            elif isinstance(dm, str):
                m = msgpack.unpackb(dm.encode('latin1'), raw=False)
            else:
                return {}
            
            return m if isinstance(m, dict) and m else {}
        except (websocket.WebSocketTimeoutException, TimeoutError, OSError):
            return {}
        except Exception as e:
            print(f"error: msgpack.unpack - {type(e).__name__}: {str(e)}")
            return {}

    def getAndWaitReponse(self):
        self.sendGetEventMessage()
        return self.waitRecv()

    def updateSubscribedFields(self):
        response = self.getAndWaitReponse()
        for field in self.subscribedFields.keys():
            field.update(response)

    def updateSubscribedIOs(self):
        self.updateSubscribedFields()

    def reconnect(self):
        """Reconnect to the websocket and re-subscribe to fields."""
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        
        self.ws = self._create_connection()
        if self.subscribedFields:
            self.sendSubscribeFields(self.subscribedFields)

    def close(self):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def getHostIP(self):
        return self.ip

    def field(self, path):
        return IGXField(self, path)

    def io(self, path):
        return IGXIO(self, path)

    def component(self, path):
        return Component(self, path)

    def buttonIO(self, path):
        return ButtonIO(self, path)

    def uploadIO(self, path, target=""):
        return UploadIO(self, path, target)

    def startCollect(self, delay, duration, fields, onMessage):
        """Start data collection. If duration < 0, run forever."""
        print(f"Starting collection at {self.ip} for {duration} seconds")

        self.sendSubscribeEvent(fields)
        self.sendGetEventMessage()
        start = time.time()

        while duration > 0 and time.time() - start < duration:
            response = self.waitRecv()
            onMessage(response.get("event"), response.get("data"))
            time.sleep(delay)
            self.sendGetEventMessage()

        self.close()
