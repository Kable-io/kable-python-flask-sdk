import sys
import signal
import requests
import uuid
from xml.etree.ElementTree import VERSION
from datetime import datetime
from cachetools import TTLCache
from functools import wraps
from threading import Timer
from flask import request, abort


KABLE_ENVIRONMENT_HEADER_KEY = 'KABLE-ENVIRONMENT'
KABLE_CLIENT_ID_HEADER_KEY = 'KABLE-CLIENT-ID'
X_CLIENT_ID_HEADER_KEY = 'X-CLIENT-ID'
X_API_KEY_HEADER_KEY = 'X-API-KEY'
X_REQUEST_ID_HEADER_KEY = 'X-REQUEST-ID'


class Kable:

    def __init__(self, config):

        print("Initializing Kable")

        if config is None:
            raise RuntimeError(
                "Failed to initialize Kable: config not provided")

        if "environment" not in config:
            raise RuntimeError(
                'Failed to initialize Kable: environment not provided')

        if "client_id" not in config:
            raise RuntimeError(
                'Failed to initialize Kable: client_id not provided')

        if "client_secret" not in config:
            raise RuntimeError(
                'Failed to initialize Kable: client_secret not provided')

        if "base_url" not in config:
            raise RuntimeError(
                'Failed to initialize Kable: base_url not provided')

        self.environment = config["environment"]
        self.kableClientId = config["client_id"]
        self.kableClientSecret = config["client_secret"]
        self.baseUrl = config["base_url"]

        self.queueFlushInterval = 10  # 10 seconds
        self.queueFlushTimer = None
        self.queueFlushMaxCount = 20  # 20 requests
        self.queueFlushMaxPoller = None
        self.queue = []

        self.validCache = TTLCache(maxsize=1000, ttl=30)
        self.invalidCache = TTLCache(maxsize=1000, ttl=30)

        self.kableEnvironment = "live" if self.environment == "live" else "test"

        url = f"https://{self.kableEnvironment}.kableapi.com/api/authenticate"
        # url = "http://localhost:8080/api/authenticate"
        headers = {
            KABLE_ENVIRONMENT_HEADER_KEY: self.environment,
            KABLE_CLIENT_ID_HEADER_KEY: self.kableClientId,
            X_CLIENT_ID_HEADER_KEY: self.kableClientId,
            X_API_KEY_HEADER_KEY: self.kableClientSecret,
        }

        try:
            response = requests.post(url=url, headers=headers)
            status = response.status_code
            if (status == 200):
                self.startFlushQueueOnTimer()
                self.startFlushQueueIfFullTimer()

                self.kill = False
                try:
                    signal.signal(signal.SIGINT, self.exitGracefully)
                    signal.signal(signal.SIGTERM, self.exitGracefully)
                except:
                    print("")

                print("Kable initialized successfully")

            elif status == 401:
                print("Failed to initialize Kable: Unauthorized")

            else:
                print(f"Failed to initialize Kable: Something went wrong [{status}]")

        except Exception as e:
            print("Failed to initialize Kable: Something went wrong")

    def authenticate(self, api):
        @wraps(api)
        def decoratedApi(*args, **kwargs):
            headers = request.headers

            clientId = headers[X_CLIENT_ID_HEADER_KEY] if X_CLIENT_ID_HEADER_KEY in headers else None
            secretKey = headers[X_API_KEY_HEADER_KEY] if X_API_KEY_HEADER_KEY in headers else None
            # TODO: generate this uuid
            requestId = str(uuid.uuid4())

            self.enqueueMessage(clientId, requestId, request)

            if self.environment is None or self.kableClientId is None:
                abort(500, {
                      "message": "Unauthorized. Failed to initialize Kable: Configuration invalid"})

            if clientId is None or secretKey is None:
                abort(401, {"message": "Unauthorized"})

            if secretKey in self.validCache:
                # response.headers[X_REQUEST_ID_HEADER_KEY] = requestId
                return api(*args)

            if secretKey in self.invalidCache:
                # print("Invalid Cache Hit")
                abort(401, {"message": "Unauthorized"})

            # print("Authenticating at server")

            url = f"https://{self.kableEnvironment}.kableapi.com/api/authenticate"
            # url = "http://localhost:8080/api/authenticate"
            headers = {
                KABLE_ENVIRONMENT_HEADER_KEY: self.environment,
                KABLE_CLIENT_ID_HEADER_KEY: self.kableClientId,
                X_CLIENT_ID_HEADER_KEY: clientId,
                X_API_KEY_HEADER_KEY: secretKey,
                X_REQUEST_ID_HEADER_KEY: requestId
            }
            try:
                response = requests.post(url=url, headers=headers)
                status = response.status_code
                if (status == 200):
                    self.validCache.__setitem__(secretKey, clientId)
                    # response.headers[X_REQUEST_ID_HEADER_KEY] = requestId
                    return api(*args)
                else:
                    if status == 401:
                        self.invalidCache.__setitem__(secretKey, clientId)
                        abort(401, {"message": "Unauthorized"})
                    else:
                        print("Unexpected " + status +
                              " response from Kable authenticate. Please update your SDK to the latest version immediately")
                        abort(500, {"message": "Something went wrong"})

            except Exception as e:
                abort(500, {"message": "Something went wrong"})

        return decoratedApi

    def enqueueMessage(self, clientId, requestId, req):
        message = {}
        message['library'] = 'kable-python-flask'
        message['libraryVersion'] = VERSION
        message['created'] = datetime.utcnow().isoformat()
        message['requestId'] = requestId

        message['environment'] = self.environment
        message['kableClientId'] = self.kableClientId
        message['clientId'] = clientId

        request = {}
        request['url'] = req.url
        request['method'] = req.method
        # headers
        # body
        message['request'] = request

        self.queue.append(message)

    def flushQueue(self):
        if self.queueFlushTimer is not None:
            # print('Stopping time-based queue poller')
            self.queueFlushTimer.cancel()
            self.queueFlushTimer = None

        if self.queueFlushMaxPoller is not None:
            # print('Stopping size-based queue poller')
            self.queueFlushMaxPoller.cancel()
            self.queueFlushMaxPoller = None

        messages = self.queue
        self.queue = []
        count = len(messages)
        if (count > 0):
            # print(f'Sending {count} batched requests to server')

            url = f"https://{self.kableEnvironment}.kableapi.com/api/requests"
            # url = "http://localhost:8080/api/requests"
            headers = {
                KABLE_ENVIRONMENT_HEADER_KEY: self.environment,
                KABLE_CLIENT_ID_HEADER_KEY: self.kableClientId,
                X_CLIENT_ID_HEADER_KEY: self.kableClientId,
                X_API_KEY_HEADER_KEY: self.kableClientSecret
            }
            try:
                response = requests.post(
                    url=url, headers=headers, json=messages)
                status = response.status_code
                if (status == 200):
                    print(
                        f'Successfully sent {count} messages to Kable server')
                else:
                    print(f'Failed to send {count} messages to Kable server')

            except Exception as e:
                print(f'Failed to send {count} messages to Kable server')
        # else:
        #     print('...no messages to flush...')

        if self.kill:
            sys.exit(0)
        else:
            self.startFlushQueueOnTimer()
            self.startFlushQueueIfFullTimer()

    def startFlushQueueOnTimer(self):
        # print('Starting time-based queue poller')
        self.queueFlushTimer = Timer(
            self.queueFlushInterval, self.flushQueue).start()

    def startFlushQueueIfFullTimer(self):
        # print('Starting size-based queue poller')
        self.queueMaxPoller = Timer(1, self.flushQueueIfFull).start()

    def flushQueueIfFull(self):
        messages = self.queue
        if len(messages) >= self.queueFlushMaxCount:
            self.flushQueue()

    def exitGracefully(self, *args):
        print(f'Kable will shut down gracefully within {self.queueFlushInterval} seconds')
        self.kill = True
        self.flushQueue()


def configure(config):
    return Kable(config)
