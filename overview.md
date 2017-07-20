# HG-agent-forwarder

The Hosted Graphite Agent Forwarder consists of 2 main parts.

1. metric forwarder
2. metric receiver(s)


### Metric Forwarder:

A metric is pulled from spools, the forwarder will attempt to complete a POST with the data to the http\_api\_url (currently https://www.hostedgraphite.com/api/v1/sink),
Should requests fail consistently to this url, exponential backoff will kick in. This occurrs in
incrementing 5s intervals. We retry on the same datapoint which initially failed, leaving everything else in a set of spool files which will subsequently be read and forward once whatever network failures have passed.

- Sending metrics to hg over HTTPS, using python-requests.Session to manage connections.
- Tailing of spool files with multitail2 and using that to maintain a spool progress file.
- Using the requests.session to re-use connections.


### Metric Receiver:

UDP/TCP interfaces, recvs datapoint for a metric, performs some validation and writes the data to the spool file.

- Spools are rotated once they reach 10MB (configurable).
- Once we have 10 spool files (also configurable), we'll remove the oldest one.


Using this setup of spooling data as we receive it (and also maintaining a progress file), the agent can then recover from any periods of time when there may be be some kind of network connectivity issues preventing metrics being sent to hostedgraphite.com.
This also allows the agent to pick up where it left off whenever it has been restarted or if it were to be stopped at some point in time and started again later.

