import glob
import os
import random
import threading
import time

from mock import patch
from pyfakefs import fake_filesystem_unittest

from hg_agent_forwarder.forwarder import MetricForwarder
from hg_agent_forwarder.receiver import MetricReceiverUdp, MetricReceiverTcp
from tests.test_utils import (MockedTcpRecvSocket, API_KEY,
                              mocked_poll, FakeSession, FakeSpool,
                              MockedUdpRecvSocket, reciever_run_shutdown,
                              write_spool, setup_tcp_receiver)


class TestReceiver(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()
        self.fs.create_dir('/var/opt/hg-agent/spool/')

        self.config = {"tcp": {"port": 2003, "host": "localhost"},
                       "apikey": API_KEY,
                       "udp": {"port": 2003, "host": "localhost"},
                       }
        self.test_metric = "foo.bar.baz"
        self.test_metric_tagged = "foo.bar.baz;tag0=val0"
        self.test_metric_openmetrics = 'foo_bar_baz{tag0="val0"}'

        # fcntl.flock() does not play nicely with pyfakefs
        # We don't need the locking it does in tests
        self.fcntl_patch = patch('hg_agent_forwarder.utils.fcntl.flock')
        self.fcntl_patch.start()

    def tearDown(self):
        self.fcntl_patch.stop()


def shutdown(f):
    # The forwarder is a daemon thread, but it can block on spool reads
    # due to how multitail2 works (cf. MetricForwarder.shutdown), so we
    # perform a "best effort" join here.
    f.shutdown()
    f.join(2)


class TestMetricForwarder(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()
        self.fs.create_dir('/var/opt/hg-agent/spool/')
        self.config = {'endpoint_url': "www.test.yolo",
                       'api_key': API_KEY,
                       'spoolglob': "/var/opt/hg-agent/spool/*.spool.*",
                       'batch_timeout': 0.5,
                       'max_batch_size': 10,
                       'interval': 1
                       }
        self.shutdown = threading.Event()

        # fcntl.flock() does not play nicely with pyfakefs
        # We don't need the locking it does in tests
        self.fcntl_patch = patch('hg_agent_forwarder.utils.fcntl.flock')
        self.fcntl_patch.start()

    def test_processing_metrics(self):
        filename = write_spool()
        forwarder = MetricForwarder(self.config, self.shutdown)
        forwarder.request_session = FakeSession()
        forwarder.start()
        while len(forwarder.request_session.metrics_posted) < 10:
            if forwarder.should_send_batch():
                forwarder.forward()
            time.sleep(0.1)
        self.shutdown.set()
        shutdown(forwarder)
        # we only have 10 valid metrics.
        self.assertEqual(len(forwarder.request_session.metrics_posted), 10)
        self.remove_spool(filename)

    def test_invalid_metric_not_processed(self):
        filename = write_spool()
        self.write_invalid_line(filename)
        forwarder = MetricForwarder(self.config, self.shutdown)
        forwarder.request_session = FakeSession()
        forwarder.start()
        while len(forwarder.request_session.metrics_posted) < 10:
            if forwarder.should_send_batch():
                forwarder.forward()
            time.sleep(0.1)
        self.shutdown.set()
        shutdown(forwarder)
        # we only have 10 valid metrics.
        self.assertEqual(len(forwarder.request_session.metrics_posted), 10)
        self.remove_spool(filename)

    @patch('hg_agent_forwarder.forwarder.time.sleep')
    def test_post_failure(self, sleep):
        sleep.return_value = True
        filename = write_spool(size=10)
        forwarder = MetricForwarder(self.config, self.shutdown)
        forwarder.request_session = FakeSession()
        forwarder.request_session.should_fail = True
        forwarder.start()
        while len(forwarder.request_session.metrics_posted) < 10:
            if forwarder.should_send_batch():
                forwarder.forward()
            time.sleep(1)
        self.shutdown.set()
        shutdown(forwarder)
        metrics_posted = forwarder.request_session.metrics_posted
        invalid_posts = forwarder.request_session.invalid_posts
        self.assertTrue(forwarder.request_session.is_called)
        self.assertEqual(len(metrics_posted), 10)
        self.assertEqual(len(invalid_posts), 10)
        self.assertIn(invalid_posts[0], metrics_posted)
        self.remove_spool(filename)

    @patch('hg_agent_forwarder.forwarder.time.sleep')
    def test_empty_post(self, sleep):
        filename = write_spool(empty=True)
        forwarder = MetricForwarder(self.config, self.shutdown)
        forwarder.request_session = FakeSession()
        forwarder.start()
        if forwarder.should_send_batch():
            forwarder.forward()
        time.sleep(0.1)
        self.shutdown.set()
        shutdown(forwarder)
        metrics_posted = forwarder.request_session.metrics_posted
        invalid_posts = forwarder.request_session.invalid_posts
        self.assertFalse(forwarder.request_session.is_called)
        self.assertEqual(len(metrics_posted), 0)
        self.assertEqual(len(invalid_posts), 0)
        self.remove_spool(filename)

    @patch('hg_agent_forwarder.forwarder.time.sleep')
    def test_one_content_post(self, sleep):
        filename = write_spool(size=1)
        forwarder = MetricForwarder(self.config, self.shutdown)
        forwarder.request_session = FakeSession()
        forwarder.start()
        while len(forwarder.request_session.metrics_posted) < 1:
            if forwarder.should_send_batch():
                forwarder.forward()
            time.sleep(0.1)
        self.shutdown.set()
        shutdown(forwarder)
        metrics_posted = forwarder.request_session.metrics_posted
        invalid_posts = forwarder.request_session.invalid_posts
        self.assertFalse(forwarder.request_session.is_called)
        self.assertEqual(len(metrics_posted), 1)
        self.assertEqual(len(invalid_posts), 0)
        self.remove_spool(filename)

    def write_invalid_line(self, filename):
        # writes an invalid metric to this spool
        f = open(filename, 'a+')
        f.write("@@().!+_+!.() op 12345a\n")
        f.close()

    def remove_spool(self, filename):
        os.remove(filename)

    def tearDown(self):
        # ensure we clean up spools even if a test fails.
        for f in glob.glob('tests/test_spool.spool.*'):
            os.remove(f)
        self.fcntl_patch.stop()


class TestMetricReceiverTcp(TestReceiver):
    def setUp(self):
        super(TestMetricReceiverTcp, self).setUp()
        self.config_sock = patch('hg_agent_forwarder.receiver.socket')
        self.mock_sock = self.config_sock.start()
        self.mock_sock.socket.return_value = MockedTcpRecvSocket()
        self.mock_sel = patch('hg_agent_forwarder.receiver.select')
        self.mocked_poll = self.mock_sel.start()
        self.mocked_poll.poll.return_value = mocked_poll
        self.mocked_poll.unregister.return_value = True

    def test_tcp_single_dp(self):
        tcp_receiver = MetricReceiverTcp(self.config)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.set_metric(self.test_metric, 20)
        setup_tcp_receiver(tcp_receiver)
        reciever_run_shutdown(tcp_receiver, 1)
        self.assertEqual(len(my_spool._spools), 1)

    def test_tcp_crlf_dp(self):
        tcp_receiver = MetricReceiverTcp(self.config)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.set_metric(self.test_metric, 20, lf='\r\n')
        setup_tcp_receiver(tcp_receiver)
        reciever_run_shutdown(tcp_receiver, 1)
        self.assertEqual(len(my_spool._spools), 1)

    def test_tcp_multi_dp(self):
        tcp_receiver = MetricReceiverTcp(self.config)
        my_spool = tcp_receiver.spool
        [tcp_receiver._sock.set_metric(self.test_metric, 20) for _ in range(20)]
        setup_tcp_receiver(tcp_receiver)
        reciever_run_shutdown(tcp_receiver, 5)
        self.assertEqual(len(my_spool._spools), 1)

    def test_tcp_multi_dp_low_timeout(self):
        conf = self.config
        conf['tcp']['timeout'] = 0.0001
        tcp_receiver = MetricReceiverTcp(conf)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.set_metric(self.test_metric, 20)
        setup_tcp_receiver(tcp_receiver)
        time.sleep(2)
        tcp_receiver._sock.set_metric(self.test_metric, 20)
        tcp_receiver._sock.set_metric(self.test_metric, 22)
        time.sleep(2)
        reciever_run_shutdown(tcp_receiver, 2)
        self.assertEqual(len(my_spool._spools), 1)

    def test_fileno_1_string(self):
        tcp_receiver = MetricReceiverTcp(self.config)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.fileno_res = 1
        tcp_receiver._sock.set_metric(self.test_metric, 20)
        setup_tcp_receiver(tcp_receiver)
        time.sleep(1)
        tcp_receiver._sock.fileno_res = 0
        tcp_receiver._sock.set_metric(self.test_metric, 20)
        tcp_receiver._sock.fileno_res = 0
        time.sleep(1)
        reciever_run_shutdown(tcp_receiver, 1)
        self.assertEqual(len(my_spool._spools), 1)

    def test_invalid_metric(self):
        tcp_receiver = MetricReceiverTcp(self.config)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.set_metric("@@1.---=++", 20)
        setup_tcp_receiver(tcp_receiver)
        # this won't result in any update.. sleep then shutdown.
        reciever_run_shutdown(tcp_receiver, 1)
        # no valid metric, no spool created.
        self.assertEqual(len(my_spool._spools), 0)

    def test_key_in_metric(self):
        tcp_receiver = MetricReceiverTcp(self.config)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.metric = "%s.%s %s\n" % (API_KEY,
                                                    "foo.bar.baz",
                                                    20)
        tcp_receiver._sock.metric_count += 1
        setup_tcp_receiver(tcp_receiver)
        reciever_run_shutdown(tcp_receiver, 1)
        self.assertEqual(len(my_spool._spools), 1)

    def test_too_many_spools(self):
        tcp_receiver = MetricReceiverTcp(self.config)
        # create too many spools, sleep to ensure new ts
        for t in range(1000, 1011):
            write_spool(t)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.set_metric(self.test_metric, 20)
        setup_tcp_receiver(tcp_receiver)
        time.sleep(1)
        reciever_run_shutdown(tcp_receiver, 1)

        all_spools = my_spool.lookup_spools()
        for ts in all_spools:
            self.assertIsInstance(ts, int)
        self.assertEqual(len(all_spools), 10)

    def test_tcp_tagged_dp(self):
        tcp_receiver = MetricReceiverTcp(self.config)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.set_metric(self.test_metric_tagged, 20)
        setup_tcp_receiver(tcp_receiver)
        reciever_run_shutdown(tcp_receiver, 1)
        self.assertEqual(len(my_spool._spools), 1)

    def test_tcp_openmetrics_dp(self):
        tcp_receiver = MetricReceiverTcp(self.config)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.set_metric(self.test_metric_openmetrics, 20)
        setup_tcp_receiver(tcp_receiver)
        reciever_run_shutdown(tcp_receiver, 1)
        self.assertEqual(len(my_spool._spools), 1)

    def test_too_many_spools_rotate_bytes(self):
        conf = self.config
        conf['spool_rotatesize'] = 10
        tcp_receiver = MetricReceiverTcp(conf)
        # create too many spools, sleep to ensure new ts
        for t in range(1000, 1011):
            write_spool(t)
        my_spool = tcp_receiver.spool
        [tcp_receiver._sock.set_metric(self.test_metric, 20) for _ in range(4)]
        setup_tcp_receiver(tcp_receiver)
        while len(my_spool.lookup_spools()) > 10:
            tcp_receiver._sock.set_metric(self.test_metric, 20)
            time.sleep(0.1)
        reciever_run_shutdown(tcp_receiver, 1)
        self.assertEqual(len(my_spool.lookup_spools()), 10)


class TestMetricReceiverUdp(TestReceiver):
    def setUp(self):
        super(TestMetricReceiverUdp, self).setUp()
        # mock out socket.
        self.config_sock = patch('hg_agent_forwarder.receiver.socket')
        self.mock_sock = self.config_sock.start()
        self.mock_sock.socket.return_value = MockedUdpRecvSocket(self.config)

    def setup_udp_receiver(self, udp_receiver, spool):
        udp_receiver.spool = spool
        udp_receiver.start()

    def test_udp_single_dp(self):
        my_spool = FakeSpool()
        udp_receiver = MetricReceiverUdp(self.config)
        udp_receiver._sock.set_metric(self.test_metric, 20)
        self.setup_udp_receiver(udp_receiver, my_spool)
        reciever_run_shutdown(udp_receiver, 1)

        self.assertEqual(len(my_spool.metrics), 1)
        self.assertEqual(my_spool.metrics[0].metric, "%s.foo.bar.baz" % API_KEY)
        self.assertEqual(my_spool.metrics[0].value, 20)

    def test_udp_crlf_dp(self):
        my_spool = FakeSpool()
        udp_receiver = MetricReceiverUdp(self.config)
        udp_receiver._sock.set_metric(self.test_metric, 20, lf='\r\n')
        self.setup_udp_receiver(udp_receiver, my_spool)
        reciever_run_shutdown(udp_receiver, 1)
        self.assertEqual(len(my_spool.metrics), 1)

    def test_udp_multi_dp(self):
        my_spool = FakeSpool()
        udp_receiver = MetricReceiverUdp(self.config)
        [udp_receiver._sock.set_metric(self.test_metric, 20) for _ in range(10)]
        self.setup_udp_receiver(udp_receiver, my_spool)
        reciever_run_shutdown(udp_receiver, 10)

        self.assertEqual(len(my_spool.metrics), 10)
        self.assertEqual(my_spool.metrics[random.choice(range(10))].metric,
                         "%s.foo.bar.baz" % API_KEY)
        self.assertEqual(my_spool.metrics[random.choice(range(10))].value, 20)


class TestEndtoEnd(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()
        self.config = {"tcp_port": 2003,
                       "api_key": API_KEY,
                       "udp_port": 2003,
                       'endpoint_url': "www.test.yolo",
                       'spoolglob': "/var/opt/hg-agent/spool/*.spool.*",
                       'batch_timeout': 1,
                       'interval': 1
                       }
        self.test_metric = "foo.bar.baz"
        self.tcp_config_sock = patch('hg_agent_forwarder.receiver.socket')
        self.tcp_mock_sock = self.tcp_config_sock.start()
        self.tcp_mock_sock.socket.return_value = MockedTcpRecvSocket()
        self.mock_sel = patch('hg_agent_forwarder.receiver.select')
        self.mocked_poll = self.mock_sel.start()
        self.mocked_poll.poll.return_value = mocked_poll
        self.mocked_poll.unregister.return_value = True
        self.shutdown = threading.Event()

        # fcntl.flock() does not play nicely with pyfakefs
        # We don't need the locking it does in tests
        self.fcntl_patch = patch('hg_agent_forwarder.utils.fcntl.flock')
        self.fcntl_patch.start()

    def tearDown(self):
        self.fcntl_patch.stop()

    def test_tcp_single_dp_spool(self):
        tcp_config_sock = patch('hg_agent_forwarder.receiver.socket')  # noqa: F841
        tcp_mock_sock = self.tcp_config_sock.start()
        tcp_mock_sock.socket.return_value = MockedTcpRecvSocket()
        tcp_receiver = MetricReceiverTcp(self.config)
        my_spool = tcp_receiver.spool
        tcp_receiver._sock.set_metric(self.test_metric, 20)
        setup_tcp_receiver(tcp_receiver)
        forwarder = MetricForwarder(self.config, self.shutdown)
        forwarder.request_session = FakeSession()
        forwarder.start()
        while len(forwarder.request_session.metrics_posted) < 1:
            if forwarder.should_send_batch():
                forwarder.forward()
            time.sleep(0.01)
        tcp_receiver.shutdown()
        self.assertEqual(len(my_spool.lookup_spools()), 1)
        self.shutdown.set()
        shutdown(forwarder)
        # we only have 1 valid metric.
        self.assertEqual(len(my_spool.lookup_spools()), 1)
        self.assertEqual(len(forwarder.request_session.metrics_posted), 1)

    def test_udp_single_dp_spool(self):
        udp_config_sock = patch('hg_agent_forwarder.receiver.socket')
        udp_mock_sock = udp_config_sock.start()
        udp_mock_sock.socket.return_value = MockedUdpRecvSocket(self.config)
        udp_receiver = MetricReceiverUdp(self.config)
        my_spool = udp_receiver.spool
        udp_receiver._sock.set_metric(self.test_metric, 20)

        udp_receiver.start()
        forwarder = MetricForwarder(self.config, self.shutdown)
        forwarder.request_session = FakeSession()
        forwarder.start()
        while len(forwarder.request_session.metrics_posted) < 1:
            if forwarder.should_send_batch():
                forwarder.forward()
            time.sleep(0.01)
        udp_receiver.shutdown()
        self.assertEqual(len(my_spool.lookup_spools()), 1)
        self.shutdown.set()
        shutdown(forwarder)
        # we only have 1 valid metric.
        self.assertEqual(len(my_spool.lookup_spools()), 1)
        self.assertEqual(len(forwarder.request_session.metrics_posted), 1)
