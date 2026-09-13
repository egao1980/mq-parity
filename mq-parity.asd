(defsystem "mq-parity"
  :version "0.1.0"
  :description "Interop canary: mq-protocol AMQP vs RabbitMQ and Kafka vs Redpanda"
  :author "egao1980"
  :license "MIT"
  :depends-on ("mq-protocol"
               "mq-backend-amqp"
               "mq-backend-kafka"
               "cffi"
               "usocket"
               "uiop")
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "probe")
               (:file "amqp")
               (:file "kafka")
               (:file "report"))
  :in-order-to ((test-op (test-op "mq-parity/tests"))))

(defsystem "mq-parity/tests"
  :depends-on ("mq-parity" "rove")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "amqp")
               (:file "kafka"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "mq-parity tests failed"))))
