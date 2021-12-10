import asyncio
import json
import logging
import socket
import uuid

import backoff
import requests
import _thread
from websocket import create_connection
import websocket
from webexteamssdk import WebexTeamsAPI

DEFAULT_DEVICE_URL = "https://wdm-a.wbx2.com/wdm/api/v1"

DEVICE_DATA = {
    "deviceName": "pywebsocket-client",
    "deviceType": "DESKTOP",
    "localizedModel": "python",
    "model": "python",
    "name": "python-spark-client",
    "systemName": "python-spark-client",
    "systemVersion": "0.1"
}


class WebexWebsocketClient(object):
    def __init__(self,
                 access_token,
                 device_url=DEFAULT_DEVICE_URL,
                 on_message=None,
                 on_card_action=None):
        self.access_token = access_token
        self.teams = WebexTeamsAPI(access_token=access_token)
        self.device_url = device_url
        self.device_info = None
        self.on_message = on_message
        self.on_card_action = on_card_action
        self.websocket = None
        self.on_open = None

    def _process_incoming_websocket_message(self, ws, msg):
        """
        Handle websocket data.
        :param msg: The raw websocket message
        """
        #print("Print message: -----------------")
        #print(str(msg))
        #print("End message: -----------------")
        data = json.loads(msg)
        #print("JsonLoad......")
        #print(str(data))
        #print("JsonEnd......")
        if data['data']['eventType'] == 'conversation.activity':
            activity = data['data']['activity']
            if activity['verb'] == 'post':
                logging.debug(f"activity={activity}")

                message_base_64_id = self._get_base64_message_id(activity)
                webex_message = self.teams.messages.get(message_base_64_id)
                logging.debug(f"webex_message from message_base_64_id: {webex_message}")
                if self.on_message:
                    # ack message first
                    self._ack_message(message_base_64_id, ws)
                    # Now process it with the handler
                    self.on_message(webex_message, activity)
            elif activity['verb'] == 'cardAction':
                logging.debug(f"activity={activity}")

                message_base_64_id = self._get_base64_message_id(activity)
                attachment_actions = self.teams.attachment_actions.get(message_base_64_id)
                logging.info(f"attachment_actions from message_base_64_id: {attachment_actions}")
                if self.on_card_action:
                    # ack message first
                    self._ack_message(message_base_64_id, ws)
                    # Now process it with the handler
                    self.on_card_action(attachment_actions, activity)
            else:
                logging.debug(f"activity verb is: {activity['verb']} ")

    def _get_base64_message_id(self, activity):
        """
        In order to geo-locate the correct DC to fetch the message from, you need to use the base64 Id of the
        message.
        @param activity: incoming websocket data
        @return: base 64 message id
        """
        activity_id = activity['id']
        logging.debug(f"activity verb=post. message id={activity_id}")
        conversation_url = activity['target']['url']
        conv_target_id = activity['target']['id']
        verb = "messages" if activity['verb'] == "post" else "attachment/actions"
        conversation_message_url = conversation_url.replace(f"conversations/{conv_target_id}",
                                                            f"{verb}/{activity_id}")
        headers = {"Authorization": f"Bearer {self.access_token}"}
        conversation_message = requests.get(conversation_message_url,
                                            headers=headers).json()
        logging.debug(f"conversation_message={conversation_message}")
        return conversation_message['id']

    def _ack_message(self, message_id, ws):
        """
        Ack that this message has been processed. This will prevent the
        message coming again.
        @param message_id: activity message 'id'
        """
        logging.debug(f"WebSocket ack message with id={message_id}")
        ack_message = {'type': 'ack',
                       'messageId': message_id}
        ws.send(json.dumps(ack_message))
        logging.info(f"WebSocket ack message with id={message_id}. Complete.")

    def _get_device_info(self, check_existing=True):
        """
        Get device info from Webex Cloud.

        If it doesn't exist, one will be created.
        """
        if check_existing:
            logging.debug('Getting device list')
            try:
                resp = self.teams._session.get(f"{self.device_url}/devices")
                for device in resp['devices']:
                    if device['name'] == DEVICE_DATA['name']:
                        self.device_info = device
                        logging.debug(f"device_info: {self.device_info}")
                        return device
            except Exception as wdmException:
                logging.warning(f"wdmException: {wdmException}")

            logging.info('Device does not exist, creating')

        resp = self.teams._session.post(f"{self.device_url}/devices", json=DEVICE_DATA)
        if resp is None:
            raise Exception("could not create WDM device")
        self.device_info = resp
        logging.debug(f"self.device_info: {self.device_info}")
        return resp

    def _on_error(self, ws, error):
        print(error)

    def _on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")

    def _on_open(self, ws):
        print("### Login ###")
        msg = {'id': str(uuid.uuid4()),
                       'type': 'authorization',
                       'data': {'token': 'Bearer ' + self.access_token}}
        ws.send(json.dumps(msg))

    def run(self):
        websocket.enableTrace(True)
        if self.device_info is None:
            if self._get_device_info() is None:
                logging.error('could not get/create device info')
                raise Exception("No WDM device info")
        ws_url = self.device_info['webSocketUrl']
        logging.info(f"Opening websocket connection to {ws_url}")
        ws = websocket.WebSocketApp(ws_url,
                              on_open=self._on_open,                              
                              on_message=self._process_incoming_websocket_message)                              

        ws.run_forever()
