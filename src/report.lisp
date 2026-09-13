(in-package #:mq-parity)

(defun print-matrix ()
  (format t "~&mq-parity matrix~%")
  (format t "  AMQP × RabbitMQ: live when ~a:~a is reachable (skip otherwise)~%"
          *amqp-host* *amqp-port*)
  (format t "  Kafka × Redpanda: live when MQ_PARITY_KAFKA_BOOTSTRAP is set ~
or docker compose ps shows a broker, and librdkafka is loadable~%")
  (format t "  PARITY=0 or MQ_PARITY=0 forces skip even if the daemon is up~%"))
