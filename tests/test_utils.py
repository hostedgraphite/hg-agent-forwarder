import time
import select
from mock import create_autospec, Mock
import random


API_KEY = "12345678-5555-2222-aaaa-661ff1afb5c1"


def reciever_run_shutdown(receiver, s_time):
    time.sleep(s_time)
    receiver.shutdown()


def write_spool(set_ts=None, empty=False, size=10):
    # create spool file with 10 valid metrics lines in it.
    if not set_ts:
        filename = '/var/opt/hg-agent/spool/test.spool.%d' % time.time()
    else:
        filename = '/var/opt/hg-agent/spool/test.spool.%d' % set_ts
    f = open(filename, 'a+')
    if empty == True:
        for _ in range(size):
            line = "\n "
            f.write(line)
        f.close()
        return filename

    for _ in range(size):
        metric = 'metric.%s' % random.choice(['foo', 'bar', 'baz'])
        value = str(random.choice(range(0, 2000)))
        line = "%s %s %s\n" % (metric, value, time.time())
        f.write(line)
    f.close()
    return filename


def setup_tcp_receiver(tcp_receiver):
    tcp_receiver._connections[1] = (tcp_receiver._sock,
                                   ('localhost', 8000),
                                   time.time())
    tcp_receiver._buffers[1] = ""
    tcp_receiver.start()


class MockedTcpRecvSocket(object):
    def __init__(self):
        self._api_key = API_KEY
        self.metrics_sent = 0
        self.metric_count = 0
        self.fileno_res = 0
        self.metric = "%s.%s %s\n"

    def recv(self, len):
        if self.metrics_sent < self.metric_count:
            time.sleep(0.01)
            self.metrics_sent += 1
            return self.metric

    def close(self, *args, **kwargs):
        pass

    def fileno(self, *args, **kwargs):
        return self.fileno_res

    def setsockopt(self, *args, **kwargs):
        pass

    def listen(self, *args, **kwargs):
        pass

    def accept(self, *args, **kwargs):
        return (self, ('localhost', 8000))

    def bind(self, *args, **kwargs):
        pass

    def set_metric(self, name, value, lf='\n'):
        self.metric_count += 1
        self.metric = "%s.%s %s%s" % (self._api_key,
                                      name, value, lf)


class MockedUdpRecvSocket(object):
    def __init__(self, config, *args, **kwargs):
        self._api_key = API_KEY
        self.metrics_sent = 0
        self.metric_count = 0
        self.metric = "%s.%s %s\n"

    def recvfrom(self, len):
        if self.metrics_sent < self.metric_count:
            time.sleep(0.01)
            self.metrics_sent += 1
            return self.metric, ""
        return "", ""

    def close(self, *args, **kwargs):
        pass

    def settimeout(self, *args, **kwargs):
        pass

    def setsockopt(self, *args, **kwargs):
        pass

    def getsockopt(self, *args, **kwargs):
        pass

    def accept(self, *args, **kwargs):
        return (self, ('localhost', 8000))

    def bind(self, *args, **kwargs):
        pass

    def set_metric(self, name, value, lf='\n'):
        self.metric_count += 1
        self.metric = "%s.%s %s%s" % (self._api_key,
                                      name, value, lf)


class Resp:
    status_code = 200

    def raise_for_status(self):
        pass


class FakeSession:
    def __init__(self, *args, **kwargs):
        self.connections = {}
        self.auth = ''
        self.metrics_posted = []
        self.should_fail = False
        self.is_called = False
        self.invalid_posts = []

    def post(self, url, data=None, stream=False, timeout=None):
        if self.should_fail and not self.is_called:
            self.is_called = True
            for line in data.strip("\n").split("\n"):
                self.invalid_posts.append(line)
            raise Exception
        elif self.should_fail and self.is_called:
            pass
        for line in data.strip("\n").split("\n"):
            self.metrics_posted.append(line)
        return Resp()


class FakeSpool:
    def __init__(self):
        self.metrics = []

    def write(self, data):
        data_string = " ".join([data.metric, str(data.value),
                               str(data.timestamp)]) + "\n"
        if data_string == data.to_spool():
            self.metrics.append(data)

    def flush_spools(self):
        return True


mocked_poll = Mock()
mocked_poll.poll.return_value = [(1, True)]
mocked_poll.register.return_value = True
