import logging
import requests
from queue import Queue
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

        self.log = logging.getLogger("kable")

        self.log.info("Initializing Kable")

        if config is None:
            raise RuntimeError(
                "Failed to initialize Kable: config not provided")

        if "environment" not in config:
            raise RuntimeError(
                'Failed to initialize Kable: environment not provided')

        if "clientId" not in config:
            raise RuntimeError(
                'Failed to initialize Kable: clientId not provided')

        if "clientSecret" not in config:
            raise RuntimeError(
                'Failed to initialize Kable: clientSecret not provided')

        if "baseUrl" not in config:
            raise RuntimeError(
                'Failed to initialize Kable: baseUrl not provided')

        self.environment = config["environment"]
        self.kableClientId = config["clientId"]
        self.kableClientSecret = config["clientSecret"]
        self.baseUrl = config["baseUrl"]

        self.queueFlushInterval = 10 # 10 seconds
        self.queueFlushTimer = None
        self.queueFlushMaxCount = 20 # 20 requests
        self.queueFlushMaxPoller = None
        self.queue = Queue(1000) # leaving extra room in case client gets a flurry of requests -- I don't want to cause a memory issue, queue will block at 1000

        self.validCache = TTLCache(maxsize=1000, ttl=30)
        self.invalidCache = TTLCache(maxsize=1000, ttl=30)


        self.kableEnvironment = "live" if self.environment == "live" else "test"

        url = "http://localhost:8080/api/authenticate"
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

                self.log.info("Kable initialized successfully")

            else:
                if status == 401:
                    self.log.error("Failed to initialize Kable: Unauthorized")
                else:
                    self.log.error("Failed to initialize Kable: Something went wrong")
        except:
            self.log.error("Failed to initialize Kable: Something went wrong")



    def authenticate(self, api):
        @wraps(api)
        def decoratedApi(*args, **kwargs):
            headers = request.headers

            clientId = headers[X_CLIENT_ID_HEADER_KEY] if X_CLIENT_ID_HEADER_KEY in headers else None
            secretKey = headers[X_API_KEY_HEADER_KEY] if X_API_KEY_HEADER_KEY in headers else None
            # TODO: generate this uuid
            requestId = "GENERATE A UUID"

            self.enqueueMessage(clientId, requestId, request)

            if self.environment is None or self.kableClientId is None:
                abort(500, {"message": "Unauthorized. Failed to initialize Kable: Configuration invalid"})

            if clientId is None or secretKey is None:
                abort(401, {"message": "Unauthorized"})


            if secretKey in self.validCache:
                self.log.debug("Valid Cache Hit")
                return api(*args)

            if secretKey in self.invalidCache:
                self.log.debug("Invalid Cache Hit")
                abort(401, {"message": "Unauthorized"})

            self.log.debug("Authenticating at server")
            url = "http://localhost:8080/api/authenticate"
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
                    return api(*args)
                else:
                    if status == 401:
                        self.invalidCache.__setitem__(secretKey, clientId)
                        abort(401, {"message": "Unauthorized"})
                    else:
                        self.log.warn("Unexpected " + status + " response from Kable authenticate. Please update your SDK to the latest version immediately")
                        abort(500, {"message": "Something went wrong"})

            except:
                abort(500, {"message": "Something went wrong"})

        return decoratedApi



    def enqueueMessage(self, clientId, requestId, req):
        message = {}
        message['library'] = 'kable-python'
        message['library_version'] = VERSION
        message['created'] = datetime.utcnow()
        message['request_id'] = requestId

        message['environment'] = self.environment
        message['kable_client_id'] = self.kableClientId
        message['client_id'] = clientId

        request = {}
        request['url'] = req.url
        request['method'] = req.method
        # headers
        # body
        message['request'] = request

        self.queue.put(message)



    def flushQueue(self):
        if self.queueFlushTimer is not None:
            self.log.debug('Stopping time-based queue poller')
            self.queueFlushTimer.cancel()
            self.queueFlushTimer = None

        if self.queueFlushMaxPoller is not None:
            self.log.debug('Stopping size-based queue poller')
            self.queueFlushMaxPoller.cancel()
            self.queueFlushMaxPoller = None

        self.log.debug('Sending batched requests to server (approximately ' + str(self.queue.qsize()) + ' requests)')
        self.queue.join()
        self.log.debug('Finished sending requests to server')

        self.startFlushQueueOnTimer()
        self.startFlushQueueIfFullTimer()



    def startFlushQueueOnTimer(self):
        self.log.debug('Starting time-based queue poller')
        self.queueFlushTimer = Timer(self.queueFlushInterval, self.flushQueue).start()



    def startFlushQueueIfFullTimer(self):
        self.log.debug('Starting size-based queue poller')
        self.queueMaxPoller = Timer(1, self.flushQueueIfFull).start()



    def flushQueueIfFull(self):
        if self.queue.qsize() >= self.queueFlushMaxCount:
            self.flushQueue()








# # def decorator(*args, **kwargs):
# def decorator(fn):

#     print("inside the decorator")

#     @wraps(fn)
#     def decorated_function(*args, **kwargs):
#         # config = kwargs["config"]
#         print("BEFORE FUNCTION RUNS")

#         print("api execution")
#         print(args)
#         print(kwargs)
#         # print("Arguments passed to decorator are " + config["greeting"])

#         print("AFTER FUNCTION RUNS")

#     return decorated_function


# # @decorator(config = {"greeting": "hello"})
# @decorator
# def fn(request):
#     print("Inner function, where request is " + request)


# x = fn("hi")
# print(x)
