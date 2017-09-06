import socket
import threading
import select
import logging
import time
from utils import Datapoint, Spool


class MetricReceiverUdp(threading.Thread):
    def __init__(self, config, *args, **kwargs):
        '''
        UDP endpoint for metrics.
        '''
        super(MetricReceiverUdp, self).__init__(*args, **kwargs)
        self.config = config
        self.name = "UDP Metric Receiver"
        self._sock = self.config_udp_socket()
        self.spool = Spool(self.config)
        self.api_key = config.get('api_key')
        self._keeprunning = True

    def config_udp_socket(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768000)
        bufsize = udp_socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_socket.settimeout(0.1)
        host = self.config.get('udp_host', 'localhost')
        port = int(self.config.get('udp_port', 2003))
        try:
            udp_socket.bind((host, port))
            logging.info("Listening on %s:%d/udp", host, port)
        except socket.error, (errno, e):
            logging.error("Could not bind to %s:%d/udp: %s",
                          host, port, e)
            raise SystemExit(1)
        return udp_socket

    def run(self):
        while self._keeprunning:
            self.spool.flush_spools()
            data, addr = None, None
            try:
                data, addr = self._sock.recvfrom(8192)
            except socket.timeout:
                pass
            except socket.error, (num, msg):
                if num not in [4, 11]:
                    raise
            except TypeError as e:
                logging.error("UDP receiver error was %s", e)

            if data is not None:
                for line in data.strip("\n").split("\n"):
                    line = line.strip()
                    try:
                        datapoint = Datapoint(line, self.api_key)
                        datapoint.key_strip()
                        if datapoint.validate():
                            logging.debug("Putting datapoint.metric %s",
                                          datapoint.metric)
                            self.spool.write(datapoint)
                    except Exception as ex:
                        logging.exception("Failed to process line %s:",
                                          repr(line))

    def shutdown(self):
        self._keeprunning = False


class MetricReceiverTcp(threading.Thread):
    '''
    TCP endpoint for metrics.
    '''

    def __init__(self, config, *args, **kwargs):
        super(MetricReceiverTcp, self).__init__(*args, **kwargs)
        self._keeprunning = True
        self.config = config
        self.name = "TCP Metric Receiver"
        self._sock = self.config_tcp_socket()
        self.spool = Spool(self.config)
        self._poll = select.poll()
        self._poll.register(self._sock.fileno(),
                            select.POLLIN | select.POLLERR)
        self._last_dest_pump = 0
        self._buffers = {}
        self.api_key = config.get('api_key')
        self._connections = {}

    def config_tcp_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host = self.config.get('tcp_host', 'localhost')
        port = int(self.config.get('tcp_port', 2003))
        try:
            sock.bind((host, port))
            sock.listen(int(50))
            logging.info("Listening on %s:%d/tcp", host, port)
        except socket.error, (errno, e):
            logging.error("Could not bind to %s:%d/tcp: %s",
                          host, port, e)
            raise SystemExit(1)
        return sock

    def run(self):
        while self._keeprunning:
            self.spool.flush_spools()
            events = self._poll.poll(100)
            if self._last_dest_pump + 0.1 < time.time():
                self._last_dest_pump = time.time()
                # Check all connections for an inactivity timeout.
                timeout_fds = self._get_timeout_fds()

            for (fd, event) in events:
                try:
                    self._handle(fd, event)
                except Exception, ex:
                    try:
                        self._close(fd)
                    except Exception, ex:
                        pass

            for fd in timeout_fds:
                try:
                    self._close(fd)
                except Exception, ex:
                    pass

    def _get_timeout_fds(self):
        '''
        Returns a list of file descriptors which are exceeding the inactivity
        timeout in the config.
        '''
        timeout = float(60.0)
        timeout_fds = []
        for (fd, (_, (_, _), last_event_ts)) in self._connections.iteritems():
            if time.time() - last_event_ts > timeout:
                timeout_fds.append(fd)
        return timeout_fds

    def _handle(self, fd, event):
        '''
        Called once for each event on a file descriptor.
        Takes care of line buffered reading.
        '''
        if event & select.POLLIN:
            if fd == self._sock.fileno():
                # Incoming data on the main listening socket means this there
                # is a new connection waiting.
                (sock, addr) = self._sock.accept()
                this_fileno = sock.fileno()
                self._connections[this_fileno] = (sock, addr, time.time())
                self._buffers[this_fileno] = ""
                self._poll.register(this_fileno,
                                    select.POLLIN | select.POLLERR)
            else:
                (sock, addr, _) = self._connections[fd]
                buf = sock.recv(4096)
                self._connections[fd] = (sock, addr, time.time())
                if len(buf) == 0:
                    self._close(fd)
                else:
                    # Extend the existing buffer for this connection
                    self._buffers[fd] += buf
                    while "\n" in self._buffers[fd]:
                        # Read a line out of this buffer,
                        # replace the buffer without the
                        # line we just read.
                        this_buf = self._buffers[fd]
                        (line, self._buffers[fd]) = this_buf.split("\n", 1)
                        try:
                            self._process(line)
                        except Exception, ex:
                            logging.exception("Failed to process line %s: %s",
                                              repr(line), ex)

                    if len(self._buffers[fd]) > 1024:
                        # More than 1kb in the buffer and still no newline?
                        # This can't be valid data, kill the connection.
                        self._close(fd)
        else:
            # Some kind of error on this socket. Just kill the connection.
            self._close(fd)

    def _process(self, line):
        '''
        Process a line and write it to a spool.
        '''
        datapoint = Datapoint(line, self.api_key)
        datapoint.key_strip()
        if datapoint.validate():
            logging.debug("Putting datapoint.metric %s",
                          datapoint.metric)
            self.spool.write(datapoint)
        return

    def _close(self, fd):
        try:
            (sock, _, _) = self._connections[fd]
        except KeyError:
            # This connection doesn't exist anymore.
            return
        sock.close()
        self._poll.unregister(fd)
        del self._connections[fd]
        del self._buffers[fd]

    def shutdown(self):
        [self._close(fd) for fd in self._connections.keys()]
        self._keeprunning = False
