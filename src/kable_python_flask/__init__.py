import sys
import signal
import requests
from datetime import datetime
from cachetools import TTLCache
from functools import wraps
from threading import Timer
from flask import request, jsonify


KABLE_ENVIRONMENT_HEADER_KEY = 'KABLE-ENVIRONMENT'
KABLE_CLIENT_ID_HEADER_KEY = 'KABLE-CLIENT-ID'
KABLE_CLIENT_SECRET_HEADER_KEY = 'KABLE-CLIENT-SECRET'
X_CLIENT_ID_HEADER_KEY = 'X-CLIENT-ID'
X_API_KEY_HEADER_KEY = 'X-API-KEY'
X_USER_ID_KEY = 'X-USER-ID'


class Kable:

    def __init__(self, config):

        print("Initializing Kable")

        if config is None:
            raise RuntimeError(
                "[KABLE] Failed to initialize Kable: config not provided")

        if "environment" not in config:
            raise RuntimeError(
                '[KABLE] Failed to initialize Kable: environment not provided')

        if "client_id" not in config:
            raise RuntimeError(
                '[KABLE] Failed to initialize Kable: client_id not provided')

        if "client_secret" not in config:
            raise RuntimeError(
                '[KABLE] Failed to initialize Kable: client_secret not provided')

        if "base_url" not in config:
            raise RuntimeError(
                '[KABLE] Failed to initialize Kable: base_url not provided')

        self.environment = config["environment"]
        self.kableClientId = config["client_id"]
        self.kableClientSecret = config["client_secret"]
        self.baseUrl = config["base_url"]

        if "debug" in config:
            self.debug = config["debug"]
            if self.debug:
                print("[KABLE] Starting Kable with debug enabled")
        else:
            self.debug = False

        if "max_queue_size" in config:
            self.maxQueueSize = config["max_queue_size"]
            if self.maxQueueSize > 100:
                self.maxQueueSize = 100
        else:
            self.maxQueueSize = 10  # flush after 10 requests queued

        if "disable_cache" in config:
            disableCache = config["disable_cache"]
            if disableCache:
                self.maxQueueSize = 1
        print("[KABLE] Starting Kable with max_queue_size " +
              str(self.maxQueueSize))

        if "record_authentication" in config:
            self.recordAuthentication = config["record_authentication"]
            if self.recordAuthentication is False:
                print(
                    "[KABLE] Starting Kable with record_authentication disabled, authentication requests will not be recorded")
        else:
            self.recordAuthentication = True

        self.queueFlushInterval = 10  # 10 seconds
        self.queueFlushTimer = None
        self.queue = []

        self.validCache = TTLCache(maxsize=1000, ttl=30)
        self.invalidCache = TTLCache(maxsize=1000, ttl=30)

        url = f"{self.baseUrl}/api/authenticate"
        headers = {
            KABLE_ENVIRONMENT_HEADER_KEY: self.environment,
            KABLE_CLIENT_ID_HEADER_KEY: self.kableClientId,
            X_CLIENT_ID_HEADER_KEY: self.kableClientId,
            X_API_KEY_HEADER_KEY: self.kableClientSecret,
            KABLE_CLIENT_SECRET_HEADER_KEY: self.kableClientSecret,
        }

        try:
            response = requests.post(url=url, headers=headers)
            status = response.status_code
            if status == 200:
                self.startFlushQueueTimer()

                try:
                    signal.signal(signal.SIGINT, self.exitGracefully)
                    signal.signal(signal.SIGTERM, self.exitGracefully)
                except:
                    print("")

                print("[KABLE] Kable initialized successfully")

            elif status == 401:
                print("[KABLE] Failed to initialize Kable: Unauthorized")

            else:
                print(
                    f"[KABLE] Failed to initialize Kable: Something went wrong [{status}]")

        except Exception as e:
            print("[KABLE] Failed to initialize Kable: Something went wrong")

    def record(self, data):
        if self.debug:
            print("[KABLE] Received data to record")

        clientId = None
        if 'clientId' in data:
            clientId = data['clientId']
            del data['clientId']

        customerId = None
        if 'customerId' in data:
            customerId = data['customerId']
            del data['customerId']

        self.enqueueMessage(clientId=clientId,
                            customerId=customerId, data=data)

    def authenticate(self, api):
        @wraps(api)
        def decoratedApi(*args, **kwargs):

            if self.debug:
                print("[KABLE] Received request to authenticate")

            headers = request.headers
            clientId = headers[X_CLIENT_ID_HEADER_KEY] if X_CLIENT_ID_HEADER_KEY in headers else None
            secretKey = headers[X_API_KEY_HEADER_KEY] if X_API_KEY_HEADER_KEY in headers else None

            if self.environment is None or self.kableClientId is None:
                return jsonify({"message": "Unauthorized. Failed to initialize Kable: Configuration invalid"}), 500

            if clientId is None or secretKey is None:
                return jsonify({"message": "Unauthorized"}), 401

            if secretKey in self.validCache:
                if self.validCache[secretKey] == clientId:
                    if self.debug:
                        print("[KABLE] Valid Cache Hit")
                    if self.recordAuthentication:
                        self.enqueueMessage(clientId, None, {})
                    return api(*args)

            if secretKey in self.invalidCache:
                if self.invalidCache[secretKey] == clientId:
                    if self.debug:
                        print("[KABLE] Invalid Cache Hit")
                    return jsonify({"message": "Unauthorized"}), 401

            if self.debug:
                print("Authenticating at server")

            url = f"{self.baseUrl}/api/authenticate"
            headers = {
                KABLE_ENVIRONMENT_HEADER_KEY: self.environment,
                KABLE_CLIENT_ID_HEADER_KEY: self.kableClientId,
                X_CLIENT_ID_HEADER_KEY: clientId,
                X_API_KEY_HEADER_KEY: secretKey,
            }
            try:
                response = requests.post(url=url, headers=headers)
                status = response.status_code
                if status == 200:
                    self.validCache.__setitem__(secretKey, clientId)
                    if self.recordAuthentication:
                        self.enqueueMessage(clientId, None, {})
                    return api(*args)
                else:
                    if status == 401:
                        self.invalidCache.__setitem__(secretKey, clientId)
                        return jsonify({"message": "Unauthorized"}), 401
                    else:
                        print("[KABLE] Unexpected " + status +
                              " response from Kable authenticate. Please update your SDK to the latest version immediately")
                        return jsonify({"message": "Something went wrong"}), 500

            except Exception as e:
                return jsonify({"message": "Something went wrong"}), 500

        return decoratedApi

    def enqueueMessage(self, clientId, customerId, data):
        event = {}
        event['environment'] = self.environment
        event['kableClientId'] = self.kableClientId
        event['clientId'] = clientId
        event['customerId'] = customerId
        event['timestamp'] = datetime.utcnow().isoformat()

        event['data'] = data

        library = {}
        library['name'] = 'kable-python-flask'
        library['version'] = '2.4.1'

        event['library'] = library

        self.queue.append(event)
        if len(self.queue) >= self.maxQueueSize:
            self.flushQueue()

    def flushQueue(self):
        if self.debug:
            print("[KABLE] Flushing Kable event queue...")

        events = self.queue
        self.queue = []
        count = len(events)
        if count > 0:
            if self.debug:
                print(f'[KABLE] Sending {count} batched events to server')

            url = f"{self.baseUrl}/api/events"
            headers = {
                KABLE_ENVIRONMENT_HEADER_KEY: self.environment,
                KABLE_CLIENT_ID_HEADER_KEY: self.kableClientId,
                X_CLIENT_ID_HEADER_KEY: self.kableClientId,
                X_API_KEY_HEADER_KEY: self.kableClientSecret
            }
            try:
                response = requests.post(
                    url=url, headers=headers, json=events)
                status = response.status_code
                if status == 200:
                    print(
                        f'[KABLE] Successfully sent {count} events to Kable server')
                else:
                    print(
                        f'[KABLE] Failed to send {count} events to Kable server')
                    for event in events:
                        print(f'[KABLE] Kable Event (Error): {event}')

            except Exception as e:
                print(f'Failed to send {count} events to Kable server')
                for event in events:
                    print(f'[KABLE] Kable Event (Error): {event}')

        else:
            if self.debug:
                print('[KABLE] ...no Kable events to flush...')

    def startFlushQueueTimer(self):
        if self.debug:
            print('[KABLE] Starting time-based queue poller')
        self.queueFlushTimer = Kable.RepeatedTimer(
            self.queueFlushInterval, self.flushQueue)
        self.queueFlushTimer.start()

    def exitGracefully(self, *args):  # args is necessary for signal handler
        if self.debug:
            print(f'[KABLE] Kable shutting down')
        self.flushQueue()
        self.queueFlushTimer.stop()
        sys.exit(0)

    class RepeatedTimer(object):

        def __init__(self, interval, function):  # , *args, **kwargs):
            self._timer = None
            self.interval = interval
            self.function = function
            # self.args = args
            # self.kwargs = kwargs
            self.is_running = False
            self.start()

        def _run(self):
            self.is_running = False
            self.start()
            self.function()  # *self.args, **self.kwargs)

        def start(self):
            if not self.is_running:
                self._timer = Timer(self.interval, self._run)
                self._timer.daemon = True
                self._timer.start()
                self.is_running = True

        def stop(self):
            self._timer.cancel()
            self.is_running = False
