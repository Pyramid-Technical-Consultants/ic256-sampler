# install: pip install websocket-client // python3

import websocket
import time
import json
from pprint import pprint
import requests
import threading


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

    # extract this field data from message
    def get(self, message):
        if "data" in message:
            data = message["data"]
            if self.path in data:
                return data[self.path]
            return []
        return []

    def update(self, message):
        self.datums = self.get(message)
        if len(self.datums) > 0:
            self.datum = self.datums[-1]  # last element

    def getDatums(self):
        return self.datums

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
        return self.getValueField().getValue()

    def setValue(self, value):
        return self.getValueField().setValue(value)

    def getTime(self, message):
        return self.getValueField().getTime()

    def isNull(self):
        return self.getValueField().isNull()

    def isEqual(self, v):
        return self.getValueField().isEqual(v)

    def isNotEqual(self, v):
        return self.getValueField().isNotEqual(v)

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
    def __init__(self, client, path):
        super().__init__(client, path)

    def toggle(self):
        self.valueField.toggle()


class UploadIO(ButtonIO):
    def __init__(self, client, path, target=""):
        super().__init__(client, path)
        self.target = "http://" + client.getHostIP() + target

    def setTarget(self, target):
        self.target = "http://" + super().client.getHostIP() + target

    def upload(self, filePath):
        with open(filePath, "rb") as file:
            content = file.read()
            requests.put(self.target, content)
            super().toggle()


class IGXWebsocketClient:
    def __init__(self, ip=""):
        self.ip = ip
        self.subscribedFields = {}
        if ip == "":
            self.ws = ""
        else:
            self.ws = websocket.create_connection("ws://" + self.ip)

    def sendEventData(self, event, data=None):
        if self.ws == "":
            return

        if self.ws.connected:
            self.ws.send(json.dumps({"event": event, "data": data}))
        else:
            print("Reconnecting :", self.ip)
            self.reconnect()

    def sendSubscribeEvent(self, fields):
        self.sendEventData("subscribe", {key: False for key in fields.keys()})

    def sendSubscribeFields(self, fields):

        self.subscribedFields = fields

        fieldJson = {}
        for f, b in fields.items():
            fieldJson[f"{f.getPath()}"] = b

        self.sendEventData("subscribe", fieldJson)

    def sendSubscribeIOs(self, ios):

        fields = {}

        for io, b in ios.items():
            fields[io.getValueField()] = b

        self.sendSubscribeFields(fields)

    def sendGetEventMessage(self):
        self.sendEventData("get")

    def sendSetEvent(self, data):
        self.sendEventData("set", data)

    def waitRecv(self):
        """Wait for and receive a message from the websocket."""
        if self.ws == "":
            return {}

        try:
            dm = self.ws.recv()
            m = json.loads(dm)
            if isinstance(m, dict) and bool(m):
                return m
            else:
                return {}
        except (websocket.WebSocketException, json.JSONDecodeError, ValueError, AttributeError) as e:
            print(f"error: json.load - {type(e).__name__}: {str(e)}")
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
        self.ws = websocket.create_connection("ws://" + self.ip)
        self.sendSubscribeFields(self.subscribedFields)

    def close(self):
        self.ws.close()

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

    # duration < 0: run forever
    def startCollect(self, delay, duration, fields, onMessage):

        print("Starting collection at", self.ip, " for", duration, "seconds")

        self.sendSubscribeEvent(fields)
        self.sendGetEventMessage()
        start = time.time()

        while duration > 0 and time.time() - start < duration:
            response = self.waitRecv()
            onMessage(response["event"], response["data"])
            time.sleep(delay)
            self.sendGetEventMessage()

        self.close()
