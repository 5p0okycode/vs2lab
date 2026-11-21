import logging 

import time

import constRPC
import threading

from context import lab_channel


class DBList:
    def __init__(self, basic_list):
        self.value = list(basic_list)

    def append(self, data):
        self.value = self.value + [data]
        return self


class Client:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.client = self.chan.join('client')
        self.server = None
        # self.logger = logging.getLogger('vs2lab.lab2.rpc.Client')
        self.chan.threads = []

    def run(self):
        self.chan.bind(self.client)
        self.server = self.chan.subgroup('server')

    def stop(self):
        for thread in self.chan.threads:
            thread.join()
        self.chan.leave('client')

    def append(self, data, db_list):
        assert isinstance(db_list, DBList)
        msglst = (constRPC.APPEND, data, db_list)  # message payload
        self.chan.send_to(self.server, msglst)  # send msg to server
        msgrcv = self.chan.receive_from(self.server)  # wait for response
        return msgrcv[1]  # pass it to caller
    """
    def _await_ack(self):
        sender, ack = self.chan.receive_from(self.server)
        assert ack == constRPC.OK, 'expected ACK from server'
        self.logger.info('Server {} acknowledged the request.'.format(sender))

    def _await_result(self, callback):
        _, result = self.chan.receive_from(self.server)
        callback(result)
    """


    def append_async(self, data, db_list, callback):
        assert isinstance(db_list, DBList)
        msglst = (constRPC.APPEND, data, db_list)
        self.chan.send_to(self.server, msglst)
        msgack = self.chan.receive_from(self.server)
        if msgack[1] != constRPC.ACK:
            raise RuntimeError(f"Expected ACK, got {msgack!r}")

        def worker():
                    try:
                        msgres = self.chan.receive_from(self.server)
                        callback(msgres[1])
                    except Exception as e:
                        print(f"[worker] error: {e}")

        t = threading.Thread(target=worker, daemon=True)
        self.chan.threads.append(t)
        t.start()

        return constRPC.ACK

class Server:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.server = self.chan.join('server')
        self.timeout = 3

    @staticmethod
    def append(data, db_list):
        assert isinstance(db_list, DBList)  # - Make sure we have a list
        return db_list.append(data)

    def run(self):
        self.chan.bind(self.server)
        while True:
            msgreq = self.chan.receive_from_any(self.timeout)  # wait for any request
            if msgreq is not None:
                client = msgreq[0]  # see who is the caller
                msgrpc = msgreq[1]  # fetch call & parameters
                if constRPC.APPEND == msgrpc[0]:  # check what is being requested
                    result = self.append(msgrpc[1], msgrpc[2])  # do local call
                    self.chan.send_to({client}, result)  # return response
                else:
                    pass  # unsupported request, simply ignore


# immediately send ack through lab_channel
#wait for 10 s after a request 
    def async_run(self):
        self.chan.bind(self.server)
        while True:
            msgreq = self.chan.receive_from_any(self.timeout)
            if msgreq is None:
                continue
            client, msgrpc = msgreq
            if msgrpc[0] != constRPC.APPEND:
                continue

            self.chan.send_to({client}, constRPC.ACK)

            def work(client=client, msgrpc=msgrpc):
                time.sleep(10)
                result = self.append(msgrpc[1], msgrpc[2])
                self.chan.send_to({client}, result)

            threading.Thread(target=work, daemon=True).start()
