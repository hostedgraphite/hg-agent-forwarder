import re
import os
import time
import argparse
import logging
import tempfile
import fcntl
import threading
import signal
import yaml
import glob


class Datapoint(object):
    '''
    Datapoint for valid metric name,
    value and timestamp combination.
    Strips api_keys to allow agent-forwarder config
    to be used instead.
    '''
    regex_metric = "[a-zA-Z0-9:\._$%#=\-\[\]]+"
    regex_value = "-?[0-9]+(\.[0-9]+)?([eE][-,+]?[0-9]+)?"
    regex_ts = "(-1|[0-9]+(.[0-9]+)?)?"
    regex_line = "^(?P<metric>%s)\s+(?P<value>%s)(?P<timestamp>\s+%s)?$" % (
                 regex_metric, regex_value, regex_ts)

    formatre = re.compile(regex_line)

    def __init__(self, line, api_key):
        self.line = line
        self.api_key = api_key

    def key_strip(self):
        try:
            key = self.line[0:36]
            if key == self.api_key:
                # metric was prefixed with their key, strip it off.
                self.line = self.line[37:]
        except IndexError:
            pass
        return True

    def validate(self):
        match = self.formatre.match(self.line)
        if not match:
            return False

        self.metric = match.group("metric")
        self.value = float(match.group("value"))
        self.timestamp = int(time.time())
        timestamp = match.group("timestamp")
        try:
            self.timestamp = int(timestamp.strip())
        except Exception as e:
            # catching any possible ts errors and
            # default back to time.time()
            pass
        return True

    def to_spool(self):
        return " ".join([self.metric, str(self.value),
                        str(self.timestamp)]) + "\n"


def get_args(argv=None):
    '''Parse out and returns script args.'''
    description = 'Metric forwarder for the Hosted Graphite agent.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Use debug logging.')
    parser.add_argument('--config', default='/etc/opt/hg-agent/hg-agent.conf',
                        help='Path to overall hg-agent config.')
    parser.add_argument('--supervisor-config',
                        default='/etc/opt/hg-agent/supervisor.conf',
                        help='Path to supervisor config.')
    args = parser.parse_args(args=argv)
    return args


def init_log(name, debug):
    '''Configure logging.'''
    logger = logging.getLogger()
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)s ' + name + '[%(process)d] %(levelname)s %(message)s'))
    logger.addHandler(handler)


def create_shutdown_event():
    '''Setup signal handlers and return a threading.Event.'''
    shutdown = threading.Event()

    def sighandler(number, frame):
        if number == signal.SIGINT or number == signal.SIGTERM:
            shutdown.set()

    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGTERM, sighandler)
    return shutdown


class SpoolFile:
    '''
    Represents a spool file.
    '''
    def __init__(self, fh, path):
        self.fh = fh
        self.path = path
        self.byteswritten = 0
        self.last_write = time.time()


class Spool:
    '''
    Spool file management.
    '''
    def __init__(self, config):
        self._config = config
        self._spools = {}
        self._all_spools = []
        self._spool_ts = int(time.time())

    def _openSpool(self):
        fd, path = tempfile.mkstemp(suffix=".spool.%d" % self._spool_ts,
                                    prefix='/var/opt/hg-agent/spool/')
        # Lock the file with flock so other processes can tell if it's still
        # in use. The lock will be automatically removed when we either close
        # the file (rotation, shutdown) or when the process exits.
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh = os.fdopen(fd, "a")
        return SpoolFile(fh, path)

    def write(self, datapoint):
        """
        Write this datapoint to a spool file on disk.
        """
        try:
            spool = self._spools[self._spool_ts]
        except KeyError:
            # spool doesn't exist yet, or has been rotated.
            spool = self.add_spool()
        record = datapoint.to_spool()
        spool.fh.write(record)
        spool.last_write = time.time()
        spool.byteswritten += len(record)
        if spool.byteswritten > self._config.get('spool_rotatesize', 10000000):
            # force rotate after 10MB written.
            self.flush_spools()
            self.add_spool()

    def flush_spools(self):
        now = time.time()
        to_del = []
        if not self._all_spools:
            self._all_spools = self.lookup_spools()

        if len(self._all_spools) > self._config.get('max_spool_count', 10):
            self.delete_spool(min(self._all_spools))
        try:
            spool = self._spools[self._spool_ts]
            last_write = spool.last_write
            if (now - last_write) > 1:
                spool.fh.flush()
                os.fsync(int(spool.fh.fileno()))
        except KeyError:
            pass
        except OSError as e:
            logging.error("OS Error: %s", e)
            pass

    def add_spool(self):
        self._spool_ts = int(time.time())
        spool = self._spools[self._spool_ts] = self._openSpool()
        self._all_spools.append(self._spool_ts)
        return spool

    def lookup_spools(self):
        all_spools = []
        for f in glob.glob('/var/opt/hg-agent/spool/*.spool.*'):
            all_spools.append(f.split('.')[2])
        return all_spools

    def delete_spool(self, ts):
        try:
            spool = self._spools[ts]
            spool.fh.flush()
            os.fsync(spool.fh.fileno())
            del self._spools[ts]
        except KeyError:
            pass

        for f in glob.glob('/var/opt/hg-agent/spool/*.spool.%s' % ts):
            logging.info("Deleteing spool file %s" % f)
            try:
                os.remove(f)
                self._all_spools.remove(ts)
            except OSError:
                pass


class LoadFileError(Exception):
    pass


def load_file(name):
    '''Load a file from disk or raise a `LoadFileError` exception.'''
    try:
        with open(name) as f:
            data = f.read()
        return data
    except (OSError, IOError) as e:
        raise LoadFileError(str(e))


def load_config(source, filename):
    '''Return, parsed and validated, the config dict from `filename` or `None`.
    Log stating `source` of the error if there's a problem.'''
    try:
        data = load_file(filename)
        agent_config = yaml.load(data)
    except LoadFileError as e:
        logging.error('%s loading %s: %s', source, filename, e)
        return None
    return agent_config
