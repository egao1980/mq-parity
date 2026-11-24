(in-package #:mq-parity)

;;; AMQP 0-9-1 canary against a live RabbitMQ. The connection, handshake
;;; and Basic.Get come from mq-backend-amqp (0.2.0); this file only drives
;;; a publish / poll / ack round trip.

(defun %amqp-credentials (user password)
  (values (or user (%env "MQ_PARITY_USER" "guest"))
          (or password (%env "MQ_PARITY_PASSWORD" "guest"))))

(defun call-with-amqp-backend (fn &key (host *amqp-host*) (port *amqp-port*)
                                    user password (vhost "/"))
  "Call FN with an mq-backend-amqp backend connected to HOST:PORT."
  (multiple-value-bind (user password) (%amqp-credentials user password)
    (mq-backend-amqp:with-amqp-backend (backend :host host :port port :user user
                                                :password password :vhost vhost)
      (funcall fn backend))))

(defun amqp-publish-consume (&key (host *amqp-host*)
                               (port *amqp-port*)
                               (payload "mq-parity"))
  "Declare an exclusive queue, publish via mq-protocol, poll and ack it.
→ the body octets that came back."
  (call-with-amqp-backend
   (lambda (backend)
     (let ((q (mq-backend-amqp:amqp-declare-queue backend "" :exclusive t :auto-delete t)))
       (mq-protocol:publish backend q (mq-protocol:make-mq-message :key "parity"
                                                                   :payload payload))
       (let ((message (first (mq-protocol:poll-messages backend q :timeout 5))))
         (unless message
           (error 'mq-protocol:mq-error :message "amqp canary: nothing came back"))
         (mq-protocol:ack backend message)
         (mq-protocol:mq-message-payload message))))
   :host host :port port))
