import time
import logging
from utils import get_args, init_log, create_shutdown_event, load_config
from forwarder import MetricForwarder


def main():
    args = get_args()
    config = load_config('forwarder', str(args.config))
    init_log('hg-agent-forwarder', args.debug)
    shutdown = create_shutdown_event()

    metric_forwarder = MetricForwarder(config)
    metric_forwarder.start()
    while not shutdown.is_set():
        if metric_forwarder.should_send_batch():
            metric_forwarder.forward()
        time.sleep(5)

    logging.debug('Caught shutdown event')

    # Shutdown forwarder
    while metric_forwarder.is_alive():
        metric_forwarder.shutdown()
        metric_forwarder.join(timeout=0.1)
        time.sleep(0.1)

    logging.info("Metric forwarder closed.")

if __name__ == '__main__':
    main()
