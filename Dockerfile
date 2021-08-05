FROM golang:1.15.3

WORKDIR /go/src/server
COPY . .

RUN go get -d
RUN go install

CMD [ "sharded-kvs" ]