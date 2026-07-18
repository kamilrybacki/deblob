# greenwindow collector runtime: curl (fetch) + jq (unwrap records from API
# envelopes before producing to Redpanda). Tiny, non-root, no shell state.
FROM alpine:3.20
RUN apk add --no-cache curl jq
# runs the mounted /scripts/collect.sh as a non-root numeric uid (set on the pod)
ENTRYPOINT ["/bin/sh"]
