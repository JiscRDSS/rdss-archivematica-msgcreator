FROM golang:1.10-alpine3.8

ARG APP_PATH=github.com/JiscRDSS/rdss-archivematica-msgcreator

RUN addgroup -g 333 -S archivematica && adduser -u 333 -S -G archivematica archivematica

ADD ./ /go/src/$APP_PATH

WORKDIR /go/src/$APP_PATH

RUN go install .

USER archivematica

ENTRYPOINT ["/go/bin/rdss-archivematica-msgcreator"]
