(in-package #:mq-parity)

;;; Kafka canary against a live broker (Redpanda in docker-compose.yml).
;;; The librdkafka driver, its native overlay and consumer-group offsets
;;; come from mq-backend-kafka (0.2.0); this file only drives the canaries.

(defun librdkafka-available-p ()
  "T when mq-backend-kafka loaded librdkafka."
  (mq-backend-kafka:librdkafka-loaded-p))

(defun kafka-driver-available-p ()
  "T when a live mq-backend-kafka driver can be made."
  (librdkafka-available-p))

(defun kafka-live-ready-p ()
  "Values RUN-P, SKIP-REASON. Live only with a broker and librdkafka."
  (cond
    ((not (live-requested-p "MQ_PARITY"))
     (values nil "PARITY=0 or MQ_PARITY=0 — live Kafka canary skipped"))
    ((null (kafka-broker-endpoint))
     (values nil
             "MQ_PARITY_KAFKA_BOOTSTRAP unset and docker compose ps shows no broker"))
    ((not (librdkafka-available-p))
     (values nil "librdkafka is not loaded (mq-backend-kafka overlay missing) — Kafka canary skipped"))
    (t
     (values t nil))))

(defun make-live-kafka-backend (&key (bootstrap (kafka-broker-endpoint)) group-id)
  "An mq-backend-kafka backend on BOOTSTRAP (its own consumer group unless
GROUP-ID is given). Release it with DESTROY-LIVE-KAFKA-BACKEND or CL:CLOSE."
  (let ((backend (mq-backend-kafka:make-kafka-backend
                  :bootstrap-servers (or bootstrap *kafka-bootstrap-default*)
                  :group-id group-id)))
    (unless (mq-backend-kafka:kafka-driver-loaded-p backend)
      (error 'mq-protocol:mq-error :message "librdkafka not loaded"))
    backend))

(defun destroy-live-kafka-backend (backend)
  (close backend)
  backend)

(defun call-with-live-kafka (fn &key bootstrap group-id)
  (let ((backend (make-live-kafka-backend :bootstrap (or bootstrap (kafka-broker-endpoint))
                                          :group-id group-id)))
    (unwind-protect (funcall fn backend)
      (destroy-live-kafka-backend backend))))

(defun %octets-to-string (value)
  "A key or header VALUE as a string: the backend gives strings for UTF-8
and octets for binary values."
  (cond
   ((null value) "")
   ((stringp value) value)
   ((vectorp value) (map 'string #'code-char value))
   (t (princ-to-string value))))

(defun %unique-topic (label)
  (format nil "parity.~a.~d.~d" label (get-universal-time) (random 1000000)))

(defun kafka-produce-consume-canary (&key (count 10))
  "Produce COUNT keyed messages with headers, consume and ack them.
→ plist :topic :sent :got. Topics auto-create on first produce."
  (call-with-live-kafka
   (lambda (backend)
     (let ((topic (%unique-topic "order"))
           (sent '()))
       (dotimes (i count)
         (let* ((key (if (evenp i) "alpha" "beta"))
                (headers (list (cons "canary" "mq-parity")
                               (cons "seq" (princ-to-string i))
                               (cons "key" key)))
                (msg (mq-protocol:make-mq-message :key key
                                                  :payload (format nil "body-~d" i)
                                                  :headers headers)))
           (mq-protocol:publish backend topic msg)
           (push (list :key key :seq i :headers headers
                       :payload (mq-protocol:mq-message-payload msg))
                 sent)))
       (mq-backend-kafka:kafka-flush backend :timeout 10)
       (let ((got (mq-protocol:poll-messages backend topic :n count :timeout 20)))
         (dolist (m got)
           (mq-protocol:ack backend m))
         (list :topic topic :sent (nreverse sent) :got got))))))

(defun kafka-nack-redelivery-canary ()
  "Publish one message, nack it with requeue, consume the redelivery, ack.
→ plist :first :second."
  (call-with-live-kafka
   (lambda (backend)
     (let ((topic (%unique-topic "nack")))
       (mq-protocol:publish backend topic
                            (mq-protocol:make-mq-message
                             :key "nack-key" :payload "nack-body"
                             :headers (list (cons "canary" "mq-parity")
                                            (cons "kind" "nack"))))
       (mq-backend-kafka:kafka-flush backend :timeout 10)
       (let ((first (first (mq-protocol:poll-messages backend topic :n 1 :timeout 15))))
         (unless first
           (error 'mq-protocol:mq-error :message "kafka nack canary: no first delivery"))
         (mq-protocol:nack backend first :requeue t)
         (let ((second (first (mq-protocol:poll-messages backend topic :n 1 :timeout 15))))
           (unless second
             (error 'mq-protocol:mq-error :message "kafka nack canary: no redelivery"))
           (mq-protocol:ack backend second)
           (list :first first :second second)))))))

(defun kafka-group-resume-canary ()
  "Two members of one group in turn: the first acks one message of two, the
second resumes after it. → plist :first :second (payload strings)."
  (let ((topic (%unique-topic "group"))
        (group (format nil "parity-group-~d-~d" (get-universal-time) (random 1000000))))
    (call-with-live-kafka
     (lambda (producer)
       (dolist (p '("g1" "g2"))
         (mq-protocol:publish producer topic (mq-protocol:make-mq-message :payload p)))))
    (flet ((take (ack-p)
             (call-with-live-kafka
              (lambda (b)
                (let ((m (first (mq-protocol:poll-messages b topic :n 1 :timeout 20))))
                  (when (and m ack-p) (mq-protocol:ack b m))
                  (and m (map 'string #'code-char (mq-protocol:mq-message-payload m)))))
              :group-id group)))
      (list :first (take t) :second (take nil)))))
