(in-package #:mq-parity)

;;; AMQP 0-9-1 handshake + publish/consume against a live broker.
;;; Framing comes from mq-backend-amqp; this file only drives the session.

(defun plain-sasl-response (user password)
  (concatenate '(simple-array (unsigned-byte 8) (*))
               #(0)
               (map '(vector (unsigned-byte 8)) #'char-code (or user ""))
               #(0)
               (map '(vector (unsigned-byte 8)) #'char-code (or password ""))))

(defun %expect-method (conn name)
  (multiple-value-bind (method frame)
      (mq-backend-amqp:read-amqp-method conn)
    (declare (ignore frame))
    (unless (eq name (mq-backend-amqp:amqp-method-name method))
      (error 'mq-protocol:mq-connection-error
             :message (format nil "expected ~s, got ~s"
                              name (mq-backend-amqp:amqp-method-name method))))
    method))

(defun amqp-handshake (conn &key (user "guest") (password "guest") (vhost "/"))
  (mq-backend-amqp:write-amqp-octets
   (mq-backend-amqp:amqp-connection-stream conn)
   mq-backend-amqp:+amqp-protocol-header+)
  (%expect-method conn :connection-start)
  (mq-backend-amqp:write-amqp-method
   conn :connection-start-ok
   :channel 0
   :client-properties '(("product" . "mq-parity"))
   :mechanism "PLAIN"
   :response (plain-sasl-response user password)
   :locale "en_US")
  (let* ((tune (%expect-method conn :connection-tune))
         (args (mq-backend-amqp:amqp-method-arguments tune)))
    (mq-backend-amqp:write-amqp-method
     conn :connection-tune-ok
     :channel 0
     :channel-max (or (getf args :channel-max) 0)
     :frame-max (or (getf args :frame-max) 131072)
     :heartbeat 0))
  (mq-backend-amqp:write-amqp-method
   conn :connection-open
   :channel 0
   :virtual-host vhost
   :reserved-1 ""
   :reserved-2 nil)
  (%expect-method conn :connection-open-ok)
  (mq-backend-amqp:write-amqp-method
   conn :channel-open
   :channel 1
   :reserved-1 "")
  (%expect-method conn :channel-open-ok)
  conn)

(defun call-with-amqp-connection (fn &key (host *amqp-host*)
                                       (port *amqp-port*)
                                       (user nil)
                                       (password nil)
                                       (vhost "/"))
  (let* ((user (or user (%env "MQ_PARITY_USER" "guest")))
         (password (or password (%env "MQ_PARITY_PASSWORD" "guest")))
         (sock (usocket:socket-connect host port
                                       :timeout 2
                                       :element-type '(unsigned-byte 8))))
    (unwind-protect
         (let ((conn (mq-backend-amqp:make-amqp-connection
                      :stream (usocket:socket-stream sock)
                      :channel 1)))
           (amqp-handshake conn :user user :password password :vhost vhost)
           (funcall fn conn))
      (ignore-errors (usocket:socket-close sock)))))

(defun amqp-publish-consume (&key (host *amqp-host*)
                               (port *amqp-port*)
                               (payload "mq-parity"))
  "Declare an exclusive queue, consume, publish via mq-protocol, return body octets."
  (call-with-amqp-connection
   (lambda (conn)
     (let* ((q (format nil "parity.~d" (get-universal-time)))
            (backend (mq-backend-amqp:make-amqp-backend
                      :connection conn :channel 1))
            (msg (mq-protocol:make-mq-message
                  :key "parity"
                  :payload payload
                  :topic q)))
       (mq-backend-amqp:write-amqp-method
        conn :queue-declare
        :queue q :passive nil :durable nil
        :exclusive t :auto-delete t :no-wait t :arguments nil)
       (mq-backend-amqp:write-amqp-method
        conn :basic-consume
        :queue q :consumer-tag "parity"
        :no-local nil :no-ack nil :exclusive t :no-wait t :arguments nil)
       (mq-protocol:publish backend q msg)
       (let ((deliver (%expect-method conn :basic-deliver)))
         (let* ((header (mq-backend-amqp:read-amqp-frame conn))
                (body (mq-backend-amqp:read-amqp-frame conn))
                (tag (getf (mq-backend-amqp:amqp-method-arguments deliver)
                           :delivery-tag)))
           (declare (ignore header))
           (mq-protocol:ack backend (or tag 1))
           (mq-backend-amqp:amqp-frame-payload body)))))
   :host host :port port))
