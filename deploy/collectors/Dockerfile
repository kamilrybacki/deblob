# greenwindow collector runtime: curl (fetch) + jq (unwrap records) + websocat
# (WebSocket sources — the Bluesky Jetstream ATProto firehose, Deblob's "final
# boss"). Tiny, non-root, no shell state. :1 = curl+jq; :2 adds websocat.
FROM alpine:3.20
RUN apk add --no-cache curl jq ca-certificates \
    && wget -qO /usr/local/bin/websocat \
       https://github.com/vi/websocat/releases/download/v1.13.0/websocat.x86_64-unknown-linux-musl \
    && echo "8f84c57103d33ab73888707041765e0e7e6a43a91fbb6e1828cd5eabc19ae32c  /usr/local/bin/websocat" | sha256sum -c - \
    && chmod +x /usr/local/bin/websocat \
    && /usr/local/bin/websocat --version
# runs the mounted /scripts/collect.sh as a non-root numeric uid (set on the pod)
ENTRYPOINT ["/bin/sh"]
