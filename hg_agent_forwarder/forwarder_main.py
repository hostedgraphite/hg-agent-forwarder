import time
import logging
from utils import get_args, init_log, create_shutdown_event, load_config
from forwarder import MetricForwarder


def main():
    args = get_args()
    config = load_config('forwarder', str(args.config))
    init_log('hg-agent-forwarder', args.debug)
    shutdown = create_shutdown_event()
    logging.info("Metric forwarder starting.")

    metric_forwarder = MetricForwarder(config, shutdown)
    metric_forwarder.start()
    while not shutdown.is_set():
        time.sleep(5)

    logging.info("Metric forwarder shutting down")
    metric_forwarder.shutdown()
    logging.info("Metric forwarder finished.")

if __name__ == '__main__':
    main()
