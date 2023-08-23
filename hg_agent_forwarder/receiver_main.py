import logging
import time

from hg_agent_forwarder.receiver import MetricReceiverUdp, MetricReceiverTcp
from hg_agent_forwarder.utils import get_args, init_log, create_shutdown_event, load_config


def main():
    args = get_args()
    config = load_config('recv', str(args.config))
    init_log('hg-agent-forwarder', args.debug)
    shutdown = create_shutdown_event()

    receivers = []
    udp_recv = MetricReceiverUdp(config)
    receivers.append(udp_recv)

    tcp_recv = MetricReceiverTcp(config)
    receivers.append(tcp_recv)

    for receiver in receivers:
        receiver.start()
        logging.info("Started thread for %s", receiver)

    while not shutdown.is_set():
        time.sleep(5)

    for receiver in receivers:
        while receiver.is_alive():
            receiver.shutdown()
            receiver.join(timeout=0.1)
            time.sleep(0.1)
    logging.info("Metric receivers closed.")


if __name__ == '__main__':
    main()
