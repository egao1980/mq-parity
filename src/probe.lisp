(in-package #:mq-parity)

(defun env-off-p (name)
  (let ((v (string-downcase (or (uiop:getenv name) ""))))
    (member v '("0" "false" "no" "off") :test #'string=)))

(defun live-requested-p (&optional extra)
  "T unless PARITY or EXTRA is explicitly off (0/false/no/off)."
  (and (not (env-off-p "PARITY"))
       (or (null extra) (not (env-off-p extra)))))

(defun %env (name &optional default)
  (or (uiop:getenv name) default))

(defparameter *amqp-host*
  (%env "MQ_PARITY_HOST" "127.0.0.1"))

(defparameter *amqp-port*
  (parse-integer (%env "MQ_PARITY_PORT" "5672") :junk-allowed t))

(defun tcp-reachable-p (host port &key (timeout 0.4))
  (handler-case
      (let ((sock (usocket:socket-connect host port
                                          :timeout timeout
                                          :element-type '(unsigned-byte 8))))
        (usocket:socket-close sock)
        t)
    (error () nil)))

(defun amqp-reachable-p ()
  (and (live-requested-p "MQ_PARITY")
       (tcp-reachable-p *amqp-host* *amqp-port*)))

(defparameter *kafka-bootstrap-default* "127.0.0.1:19092")

(defun kafka-bootstrap ()
  "MQ_PARITY_KAFKA_BOOTSTRAP, or NIL when unset/empty."
  (let ((v (uiop:getenv "MQ_PARITY_KAFKA_BOOTSTRAP")))
    (when (and v (plusp (length v)))
      v)))

(defun %compose-file ()
  (let ((root (asdf:system-source-directory "mq-parity")))
    (when root
      (namestring (merge-pathnames "docker-compose.yml" root)))))

(defun %run-compose-ps ()
  "Output of `docker compose ps`, or NIL if compose/docker is unavailable."
  (let ((file (%compose-file)))
    (unless (and file (probe-file file))
      (return-from %run-compose-ps nil))
    (dolist (argv (list (list "docker" "compose" "-f" file "ps"
                              "--status" "running" "--services")
                        (list "docker-compose" "-f" file "ps"
                              "--services")))
      (handler-case
          (multiple-value-bind (out _err code)
              (uiop:run-program argv
                                :output :string
                                :error-output :string
                                :ignore-error-status t)
            (declare (ignore _err))
            (when (and (or (null code) (zerop code)) out)
              (return-from %run-compose-ps out)))
        (error ())))
    nil))

(defun kafka-compose-broker-p ()
  "T when `docker compose ps` lists a running redpanda/kafka service."
  (let ((out (%run-compose-ps)))
    (when (and out (plusp (length out)))
      (loop for line in (uiop:split-string out :separator '(#\Newline #\Return))
            for name = (string-trim '(#\Space #\Tab) line)
            thereis (and (plusp (length name))
                         (or (search "redpanda" name :test #'char-equal)
                             (string-equal name "kafka")
                             (search "kafka" name :test #'char-equal))
                         t)))))

(defun kafka-broker-endpoint ()
  "Bootstrap `host:port` from env, or the compose default when a broker is up."
  (or (kafka-bootstrap)
      (when (kafka-compose-broker-p)
        *kafka-bootstrap-default*)))
