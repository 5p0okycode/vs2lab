import rpc
import logging
import time

from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)

cl = rpc.Client()
cl.run()

base_list = rpc.DBList({'foo'})
"""result_list = cl.append_async('bar', base_list, lambda res: print("Callback result: {}".format(res)))

print("result: {}".format(result_list.value))
"""

def on_result(res):
    print("Callback result: {}".format(res.value))

ack = cl.append_async('bar', base_list, on_result)
print("ACK received: {}".format(ack))          # ACK, nicht .value!

wait = 0
while any(t.is_alive() for t in cl.chan.threads):
    time.sleep(1)
    wait += 1
    print("... still waiting ({}s)".format(wait))

cl.stop()
