FROM golang:1.15.3

WORKDIR /go/src/cse138_assignment3
COPY ./cse138_assignment3.go .

RUN go get -d
RUN go install

CMD [ "cse138_assignment3" ]