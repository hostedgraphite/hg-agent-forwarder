import threading
import Queue
import os
import logging
import json
import time
import requests
import multitail2
import errno
import tempfile
from requests.auth import HTTPBasicAuth
from utils import Datapoint


class MetricForwarder(threading.Thread):
    '''
    Simple metric data forwarder.
    Forwards data over http, has a simple exponential
    backoff in case of connectivity issues.
    '''
    def __init__(self, config, *args, **kwargs):
        super(MetricForwarder, self).__init__(*args, **kwargs)
        self.config = config
        self.name = "Metric Forwarder"
        self.daemon = True

        self.url = config.get('endpoint_url',
                              'https://agentapi.hostedgraphite.com/api/v1/sink')
        self.api_key = self.config.get('api_key')
        self.progress = self.load_progress_file()
        self.shutdown_e = threading.Event()
        self.spool_reader = SpoolReader('/var/opt/hg-agent/spool/*.spool.*',
                                        progresses=self.progress,
                                        shutdown=self.shutdown_e)
        self.progress_writer = ProgressWriter(self.config,
                                              self.spool_reader,
                                              self.shutdown_e)
        self.progress_writer.start()
        self.error_timestamp = 0
        self.backoff_timeout = 1
        self.backoff_sleep = 0
        self.backoff = False
        self.request_session = requests.Session()
        self.request_session.auth = HTTPBasicAuth(self.api_key, '')
        self.batch = ""
        self.batch_size = 0
        self.batch_time = time.time()
        self.batch_timeout = config.get('batch_timeout', 0.5)
        self.max_batch_size = config.get('max_batch_size', 250)

    def run(self):
        while not self.shutdown_e.is_set():
            try:
                for line in self.spool_reader.read():
                    datapoint = Datapoint(line, self.api_key)
                    if datapoint.validate():
                        self.extend_batch(datapoint)
                    else:
                        logging.error("Invalid line in spool.")
                        # invalid somehow, pass
                        continue
                    if self.should_send_batch():
                        self.forward()
                if self.shutdown_e.is_set():
                    break
            except Exception as e:
                continue

    def extend_batch(self, data):
        '''
        Add a metric to the current metric batch.
        '''
        if not self.batch_time:
            self.batch_time = time.time()

        try:
            metric = data.metric
            value = data.value
            ts = data.timestamp
        except AttributeError:
            # somehow, this dp is invalid, pass it by.
            pass
        else:
            metric_str = "%s %s %s" % (metric, value, ts)
            self.batch = "%s\n%s" % (self.batch, metric_str)
            self.batch_size += 1

    def should_send_batch(self):
        '''
        Check to see if we should send the
        current batch.
        True if timeout is > 10 or batch
        size is reached.
        '''
        now = time.time()
        if (now - self.batch_time) > self.batch_timeout:
            return True
        elif self.batch_size > self.max_batch_size:
            return True
        return False

    def forward(self):
        send_success = False
        while not send_success:
            send_success = self.send_data()
            if not send_success:
                self.backoff = True
                now = time.time()
                if (now - self.error_timestamp) < self.backoff_timeout:
                    # if we've seen errors successively in a second,
                    # log & sleep for a bit.
                    self.backoff_sleep += 1
                    logging.info("Metric sending failed, will try again \
                                 in %s seconds", self.backoff_sleep)
                    time.sleep(self.backoff_sleep)
                self.error_timestamp = now

        if self.backoff:
            self.backoff = False
            self.backoff_sleep = 0
        return True

    def send_data(self):
        try:
            req = self.request_session.post(self.url,
                                            data=self.batch,
                                            stream=False)
            if req.status_code == 429:
                logging.info("Metric forwarding limits hit \
                             please contact support.")
        except Exception as e:
            logging.error("Metric forwarding exception was %s", e)
            return False
        else:
            # reset batch info now that send has succeeded.
            self.batch = ""
            self.batch_size = 0
            self.batch_time = time.time()
        return True

    def shutdown(self):
        self.shutdown_e.set()
        self.progress_writer.join(timeout=0.1)
        self.join(timeout=0.1)

    def load_progress_file(self):
        progress_cfg = self.config.get('progress', {})
        progress = {}
        try:
            progressfile = progress_cfg.get('path',
                                            '/var/opt/hg-agent/spool/progress')
            if progressfile is not None:
                progress = json.load(file(progressfile))

        except (ValueError, IOError, OSError):
            logging.exception(
                'Error loading progress file on startup.'
                'Spool files will be read from end'
            )
        return progress


class SpoolReader(object):
    '''
    Tails files matching a glob.  yields lines from them.
    '''
    def __init__(self, spoolglob, progresses=None, shutdown=None):
        self.progresses = progresses or {}
        self.shutdown_e = shutdown
        self.data_reader = multitail2.MultiTail(spoolglob,
                                                skip_to_end=False,
                                                offsets=progresses)

    def read(self):
        for (filename, byteoffset), line in self.data_reader:
            if self.shutdown_e.is_set():
                break
            line_byte_len = len(bytes(line))
            # + 1 for newline '\n'
            self.progresses[filename] = byteoffset + line_byte_len + 1
            try:
                yield line
            except ValueError:
                logging.error('Could not parse line: %s', line)
                continue


class ProgressWriter(threading.Thread):
    '''
    '''
    def __init__(self, config, spool_reader, shutdown_e, *args, **kwargs):
        super(ProgressWriter, self).__init__(*args, **kwargs)
        self.shutdown_e = shutdown_e
        self._config = config
        self.interval = self._config.get('interval', 10)
        self.spool_reader = spool_reader
        self.final_path = '/var/opt/hg-agent/spool/'

    def run(self):
        while not self.shutdown_e.is_set():
            try:
                self.atomicwrite()
            except Exception as e:
                logging.error("Unhandled exception while writing progress: %s",
                              e)
            time.sleep(self.interval)

    def atomicwrite(self):
        try:
            content = json.dumps(self.spool_reader.progresses)
        except:
            content = {}
        try:
            os.makedirs('/var/opt/hg-agent/spool/', 0755)
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise
        fd, temp_path = tempfile.mkstemp(dir='/var/opt/hg-agent/spool/')
        with os.fdopen(fd, 'w') as fh:
            fh.write(content)
        os.chmod(temp_path, 0644)
        os.rename(temp_path, "%s/%s" % (self.final_path, "progress"))
