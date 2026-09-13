(in-package #:mq-parity/tests)

(deftest probe-returns-boolean
  (ok (member (tcp-reachable-p *amqp-host* *amqp-port*) '(t nil))))

(deftest rabbitmq-publish-consume
  (cond
    ((not (live-requested-p "MQ_PARITY"))
     (skip "PARITY=0 or MQ_PARITY=0 — live AMQP canary skipped"))
    ((not (tcp-reachable-p *amqp-host* *amqp-port*))
     (skip (format nil "RabbitMQ unreachable at ~a:~a — live canary skipped"
                   *amqp-host* *amqp-port*)))
    (t
     (let ((body (amqp-publish-consume :payload "hi-parity")))
       (ok (equalp (map '(vector (unsigned-byte 8)) #'char-code "hi-parity")
                   body))))))
