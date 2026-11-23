(defpackage #:mq-parity
  (:use #:cl)
  (:export #:*amqp-host*
           #:*amqp-port*
           #:*kafka-bootstrap-default*
           #:env-off-p
           #:live-requested-p
           #:tcp-reachable-p
           #:amqp-reachable-p
           #:kafka-bootstrap
           #:kafka-compose-broker-p
           #:kafka-broker-endpoint
           #:kafka-live-ready-p
           #:kafka-driver-available-p
           #:librdkafka-available-p
           #:make-live-kafka-backend
           #:destroy-live-kafka-backend
           #:call-with-live-kafka
           #:kafka-produce-consume-canary
           #:kafka-nack-redelivery-canary
           #:kafka-group-resume-canary
           #:call-with-amqp-backend
           #:amqp-publish-consume
           #:print-matrix))

(in-package #:mq-parity)
