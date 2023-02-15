# build stage
FROM golang:1.20.1-alpine AS build-env
RUN apk --no-cache add build-base git gcc

RUN mkdir -p /build
COPY ./ /build/

WORKDIR /build/
RUN go version
#RUN git checkout main && go build -o steward
RUN go build -o stewardwriter

# final stage
FROM alpine

RUN apk update && apk add curl

WORKDIR /app
COPY --from=build-env /build/stewardwriter /app/

ENV MESSAGE_FULL_PATH "/app/message.json"
ENV SOCKET_FULL_PATH "./tmp/steward.sock"
ENV INTERVAL 60
ENV WATCH_FOLDER ""

CMD ["ash","-c","/app/stewardwriter\
    -messageFullPath=$MESSAGE_FULL_PATH\
    -socketFullPath=$SOCKET_FULL_PATH\
    -interval=$INTERVAL\
    -watchFolder=$WATCH_FOLDER\
    "]
