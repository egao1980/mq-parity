(in-package #:mq-parity)

;;; Live Kafka canary via mq-backend-kafka + a librdkafka CFFI driver.
;;; The library is loaded lazily — missing .so must not break default CI.

(cffi:define-foreign-library librdkafka
  (:darwin (:or "librdkafka.dylib" "librdkafka.1.dylib"))
  (:unix (:or "librdkafka.so.1" "librdkafka.so"))
  (:windows (:or "rdkafka.dll" "librdkafka.dll"))
  (t (:default "rdkafka")))

(defvar *librdkafka-state* :unknown)
(defvar *librdkafka-error* nil)
(defvar *kafka-contexts* (make-hash-table :test 'eq))

(defconstant +rd-kafka-producer+ 0)
(defconstant +rd-kafka-consumer+ 1)
(defconstant +rd-kafka-offset-beginning+ -2)
(defconstant +rd-kafka-msg-f-copy+ 2)
(defconstant +rd-kafka-vtype-end+ 0)
(defconstant +rd-kafka-vtype-topic+ 1)
(defconstant +rd-kafka-vtype-value+ 4)
(defconstant +rd-kafka-vtype-key+ 5)
(defconstant +rd-kafka-vtype-msgflags+ 7)
(defconstant +rd-kafka-vtype-headers+ 10)
(defconstant +rd-kafka-admin-op-createtopics+ 1)

(cffi:defcstruct rd-kafka-message
  (err :int)
  (rkt :pointer)
  (partition :int32)
  (payload :pointer)
  (len :size)
  (key :pointer)
  (key-len :size)
  (offset :int64)
  (private :pointer))

(cffi:defcstruct rd-kafka-topic-partition
  (topic :pointer)
  (partition :int32)
  (offset :int64)
  (metadata :pointer)
  (metadata-size :size)
  (opaque :pointer)
  (err :int)
  (private :pointer))

(cffi:defcfun ("rd_kafka_conf_new" %rd-conf-new) :pointer)
(cffi:defcfun ("rd_kafka_conf_set" %rd-conf-set) :int
  (conf :pointer) (name :string) (value :string)
  (errstr :pointer) (errstr-size :size))
(cffi:defcfun ("rd_kafka_conf_destroy" %rd-conf-destroy) :void
  (conf :pointer))
(cffi:defcfun ("rd_kafka_new" %rd-new) :pointer
  (type :int) (conf :pointer) (errstr :pointer) (errstr-size :size))
(cffi:defcfun ("rd_kafka_destroy" %rd-destroy) :void
  (rk :pointer))
(cffi:defcfun ("rd_kafka_poll" %rd-poll) :int
  (rk :pointer) (timeout-ms :int))
(cffi:defcfun ("rd_kafka_flush" %rd-flush) :int
  (rk :pointer) (timeout-ms :int))
(cffi:defcfun ("rd_kafka_err2str" %rd-err2str) :string
  (err :int))
(cffi:defcfun ("rd_kafka_last_error" %rd-last-error) :int)
(cffi:defcfun ("rd_kafka_topic_name" %rd-topic-name) :string
  (rkt :pointer))
(cffi:defcfun ("rd_kafka_headers_new" %rd-headers-new) :pointer
  (initial-count :size))
(cffi:defcfun ("rd_kafka_header_add" %rd-header-add) :int
  (hdrs :pointer) (name :string) (name-size :long)
  (value :pointer) (value-size :long))
(cffi:defcfun ("rd_kafka_headers_destroy" %rd-headers-destroy) :void
  (hdrs :pointer))
(cffi:defcfun ("rd_kafka_message_headers" %rd-message-headers) :int
  (rkmessage :pointer) (hdrsp :pointer))
(cffi:defcfun ("rd_kafka_header_cnt" %rd-header-cnt) :size
  (hdrs :pointer))
(cffi:defcfun ("rd_kafka_header_get_all" %rd-header-get-all) :int
  (hdrs :pointer) (idx :size) (namep :pointer)
  (valuep :pointer) (sizep :pointer))
(cffi:defcfun ("rd_kafka_poll_set_consumer" %rd-poll-set-consumer) :int
  (rk :pointer))
(cffi:defcfun ("rd_kafka_topic_partition_list_new" %rd-tpl-new) :pointer
  (size :int))
(cffi:defcfun ("rd_kafka_topic_partition_list_add" %rd-tpl-add) :pointer
  (rktparlist :pointer) (topic :string) (partition :int32))
(cffi:defcfun ("rd_kafka_topic_partition_list_destroy" %rd-tpl-destroy) :void
  (rktparlist :pointer))
(cffi:defcfun ("rd_kafka_assign" %rd-assign) :int
  (rk :pointer) (partitions :pointer))
(cffi:defcfun ("rd_kafka_consumer_poll" %rd-consumer-poll) :pointer
  (rk :pointer) (timeout-ms :int))
(cffi:defcfun ("rd_kafka_message_destroy" %rd-message-destroy) :void
  (rkmessage :pointer))
(cffi:defcfun ("rd_kafka_consumer_close" %rd-consumer-close) :int
  (rk :pointer))
(cffi:defcfun ("rd_kafka_topic_new" %rd-topic-new) :pointer
  (rk :pointer) (topic :string) (topic-conf :pointer))
(cffi:defcfun ("rd_kafka_topic_destroy" %rd-topic-destroy) :void
  (rkt :pointer))
(cffi:defcfun ("rd_kafka_seek" %rd-seek) :int
  (rkt :pointer) (partition :int32) (offset :int64) (timeout-ms :int))
(cffi:defcfun ("rd_kafka_NewTopic_new" %rd-newtopic-new) :pointer
  (topic :string) (num-partitions :int) (replication-factor :int)
  (errstr :pointer) (errstr-size :size))
(cffi:defcfun ("rd_kafka_NewTopic_destroy" %rd-newtopic-destroy) :void
  (new-topic :pointer))
(cffi:defcfun ("rd_kafka_AdminOptions_new" %rd-adminoptions-new) :pointer
  (rk :pointer) (op :int))
(cffi:defcfun ("rd_kafka_AdminOptions_destroy" %rd-adminoptions-destroy) :void
  (options :pointer))
(cffi:defcfun ("rd_kafka_AdminOptions_set_request_timeout"
               %rd-adminoptions-set-timeout)
  :int
  (options :pointer) (tmout :int) (errstr :pointer) (errstr-size :size))
(cffi:defcfun ("rd_kafka_queue_new" %rd-queue-new) :pointer
  (rk :pointer))
(cffi:defcfun ("rd_kafka_queue_destroy" %rd-queue-destroy) :void
  (rkqu :pointer))
(cffi:defcfun ("rd_kafka_queue_poll" %rd-queue-poll) :pointer
  (rkqu :pointer) (timeout-ms :int))
(cffi:defcfun ("rd_kafka_CreateTopics" %rd-create-topics) :void
  (rk :pointer) (new-topics :pointer) (new-topic-cnt :size)
  (options :pointer) (rkqu :pointer))
(cffi:defcfun ("rd_kafka_event_destroy" %rd-event-destroy) :void
  (rkev :pointer))
(cffi:defcfun ("rd_kafka_produceva" %rd-produceva) :pointer
  (rk :pointer) (vus :pointer) (cnt :size))
(cffi:defcfun ("rd_kafka_error_string" %rd-error-string) :string
  (error :pointer))
(cffi:defcfun ("rd_kafka_error_destroy" %rd-error-destroy) :void
  (error :pointer))

;;; rd_kafka_vu_t is vtype (int) + 4 pad + 64-byte union (_pad[64]).
(defconstant +rd-kafka-vu-size+ 72)
(defconstant +rd-kafka-vu-union+ 8)

(defstruct (rdkafka-ctx (:constructor %make-rdkafka-ctx))
  bootstrap
  producer
  consumer
  group-id
  (inflight (make-hash-table :test 'eql))
  (next-tag 0)
  (assigned (make-hash-table :test 'equal))
  (redeliver (make-hash-table :test 'equal)))

(defun %librdkafka-search-dirs ()
  (dolist (dir '("/opt/homebrew/lib"
                 "/usr/local/lib"
                 "/usr/lib/x86_64-linux-gnu"
                 "/usr/lib/aarch64-linux-gnu"
                 "/usr/lib"))
    (when (uiop:directory-exists-p dir)
      (pushnew dir cffi:*foreign-library-directories* :test #'equal)))
  cffi:*foreign-library-directories*)

(defun %load-librdkafka ()
  (%librdkafka-search-dirs)
  (let ((explicit (uiop:getenv "MQ_PARITY_LIBRDKAFKA")))
    (when (and explicit (plusp (length explicit)))
      (cffi:load-foreign-library explicit)
      (return-from %load-librdkafka t)))
  (dolist (cand '("/opt/homebrew/lib/librdkafka.dylib"
                  "/usr/local/lib/librdkafka.dylib"
                  "/usr/lib/x86_64-linux-gnu/librdkafka.so.1"
                  "/usr/lib/aarch64-linux-gnu/librdkafka.so.1"
                  "/usr/lib/librdkafka.so.1"))
    (when (probe-file cand)
      (cffi:load-foreign-library (namestring (truename cand)))
      (return-from %load-librdkafka t)))
  (cffi:load-foreign-library 'librdkafka)
  t)

(defun ensure-librdkafka ()
  (ecase *librdkafka-state*
    (:loaded t)
    (:missing nil)
    (:unknown
     (handler-case
         (progn
           (%load-librdkafka)
           (setf *librdkafka-state* :loaded
                 *librdkafka-error* nil)
           t)
       (error (c)
         (setf *librdkafka-state* :missing
               *librdkafka-error* (princ-to-string c))
         nil)))))

(defun librdkafka-available-p ()
  (and (ensure-librdkafka) t))

(defun kafka-driver-available-p ()
  "T when librdkafka can be loaded for a live mq-backend-kafka driver."
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
     (values nil
             (format nil "librdkafka missing — Kafka canary skipped~@[: ~a~]"
                     *librdkafka-error*)))
    (t
     (values t nil))))

(defun %octets (value)
  (etypecase value
    (null (make-array 0 :element-type '(unsigned-byte 8)))
    ((vector (unsigned-byte 8))
     (coerce value '(simple-array (unsigned-byte 8) (*))))
    (string
     (map '(simple-array (unsigned-byte 8) (*)) #'char-code value))
    (symbol
     (map '(simple-array (unsigned-byte 8) (*))
          #'char-code (string-downcase (symbol-name value))))))

(defun %octets-to-string (value)
  (cond
    ((null value) "")
    ((stringp value) value)
    ((vectorp value) (map 'string #'code-char value))
    (t (princ-to-string value))))

(defun %copy-foreign (octets)
  (let* ((n (length octets))
         (p (cffi:foreign-alloc :uint8 :count (max n 1))))
    (loop for i from 0 below n
          do (setf (cffi:mem-aref p :uint8 i) (aref octets i)))
    (values p n)))

(defun %foreign-octets (ptr n)
  (let ((out (make-array n :element-type '(unsigned-byte 8))))
    (loop for i from 0 below n
          do (setf (aref out i) (cffi:mem-aref ptr :uint8 i)))
    out))

(defun %conf-set (conf name value errstr)
  (let ((rc (%rd-conf-set conf name value errstr 512)))
    (unless (zerop rc)
      (error 'mq-protocol:mq-error
             :message (format nil "rd_kafka_conf_set ~a: ~a"
                              name (cffi:foreign-string-to-lisp errstr))))))

(defun %make-handle (type bootstrap extra)
  (let ((conf (%rd-conf-new))
        (errstr (cffi:foreign-alloc :char :count 512)))
    (unwind-protect
         (progn
           (%conf-set conf "bootstrap.servers" bootstrap errstr)
           (%conf-set conf "client.id" "mq-parity" errstr)
           (dolist (pair extra)
             (%conf-set conf (car pair) (cdr pair) errstr))
           (let ((rk (%rd-new type conf errstr 512)))
             (when (cffi:null-pointer-p rk)
               (error 'mq-protocol:mq-error
                      :message (format nil "rd_kafka_new: ~a"
                                       (cffi:foreign-string-to-lisp errstr))))
             ;; conf is owned by rk on success
             (setf conf (cffi:null-pointer))
             rk))
      (cffi:foreign-free errstr)
      (unless (cffi:null-pointer-p conf)
        (%rd-conf-destroy conf)))))

(defun %headers-from-message (headers)
  (unless headers
    (return-from %headers-from-message (cffi:null-pointer)))
  (let* ((pairs (if (and (consp headers) (consp (first headers)))
                    headers
                    (loop for (k v) on headers by #'cddr
                          collect (cons (string k) v))))
         (hdrs (%rd-headers-new (max 1 (length pairs)))))
    (dolist (pair pairs)
      (let ((name (string (car pair)))
            (oct (%octets (cdr pair))))
        (multiple-value-bind (ptr n) (%copy-foreign oct)
          (unwind-protect
               (let ((rc (%rd-header-add hdrs name -1 ptr n)))
                 (unless (zerop rc)
                   (%rd-headers-destroy hdrs)
                   (error 'mq-protocol:mq-error
                          :message (format nil "rd_kafka_header_add: ~a"
                                           (%rd-err2str rc)))))
            (cffi:foreign-free ptr)))))
    hdrs))

(defun %read-headers (rkmessage)
  (cffi:with-foreign-object (hdrsp :pointer)
    (setf (cffi:mem-ref hdrsp :pointer) (cffi:null-pointer))
    (let ((rc (%rd-message-headers rkmessage hdrsp)))
      (unless (zerop rc)
        (return-from %read-headers nil))
      (let ((hdrs (cffi:mem-ref hdrsp :pointer)))
        (when (cffi:null-pointer-p hdrs)
          (return-from %read-headers nil))
        (loop with n = (%rd-header-cnt hdrs)
              for i from 0 below n
              collect
              (cffi:with-foreign-objects ((namep :pointer)
                                          (valuep :pointer)
                                          (sizep :size))
                (%rd-header-get-all hdrs i namep valuep sizep)
                (cons (cffi:foreign-string-to-lisp
                       (cffi:mem-ref namep :pointer))
                      (%octets-to-string
                       (%foreign-octets (cffi:mem-ref valuep :pointer)
                                        (cffi:mem-ref sizep :size))))))))))

(defun %vu-slot (base i)
  (cffi:inc-pointer base (* i +rd-kafka-vu-size+)))

(defun %vu-clear (base i)
  (let ((p (%vu-slot base i)))
    (dotimes (b +rd-kafka-vu-size+)
      (setf (cffi:mem-aref p :uint8 b) 0))))

(defun %vu-set-vtype (base i vtype)
  (%vu-clear base i)
  (setf (cffi:mem-ref (%vu-slot base i) :int) vtype))

(defun %vu-set-ptr (base i ptr)
  (setf (cffi:mem-ref (cffi:inc-pointer (%vu-slot base i) +rd-kafka-vu-union+)
                      :pointer)
        ptr))

(defun %vu-set-mem (base i ptr size)
  (let ((u (cffi:inc-pointer (%vu-slot base i) +rd-kafka-vu-union+)))
    (setf (cffi:mem-ref u :pointer) ptr)
    (setf (cffi:mem-ref (cffi:inc-pointer u 8) :size) size)))

(defun %vu-set-int (base i n)
  (setf (cffi:mem-ref (cffi:inc-pointer (%vu-slot base i) +rd-kafka-vu-union+)
                      :int)
        n))

(defun %produceva (rk topic-ptr key-ptr key-len val-ptr val-len headers)
  "produceva uses a counted array — do not append VTYPE_END (that is producev)."
  (let* ((with-headers (and headers (not (cffi:null-pointer-p headers))))
         (n (if with-headers 5 4))
         (arr (cffi:foreign-alloc :uint8 :count (* n +rd-kafka-vu-size+))))
    (unwind-protect
         (progn
           (%vu-set-vtype arr 0 +rd-kafka-vtype-topic+)
           (%vu-set-ptr arr 0 topic-ptr)
           (%vu-set-vtype arr 1 +rd-kafka-vtype-key+)
           (%vu-set-mem arr 1 key-ptr key-len)
           (%vu-set-vtype arr 2 +rd-kafka-vtype-value+)
           (%vu-set-mem arr 2 val-ptr val-len)
           (%vu-set-vtype arr 3 +rd-kafka-vtype-msgflags+)
           (%vu-set-int arr 3 +rd-kafka-msg-f-copy+)
           (when with-headers
             (%vu-set-vtype arr 4 +rd-kafka-vtype-headers+)
             (%vu-set-ptr arr 4 headers))
           (let ((err (%rd-produceva rk arr n)))
             (cond
               ((cffi:null-pointer-p err) 0)
               (t
                (let ((msg (%rd-error-string err)))
                  (%rd-error-destroy err)
                  (error 'mq-protocol:mq-error
                         :message (format nil "rd_kafka_produceva: ~a" msg)))))))
      (cffi:foreign-free arr))))

(defun %publish (ctx destination message)
  (let* ((topic (or (mq-protocol:mq-message-topic message)
                    destination))
         (key-oct (%octets (mq-protocol:mq-message-key message)))
         (val (mq-protocol:mq-message-payload message))
         (hdrs (%headers-from-message (mq-protocol:mq-message-headers message)))
         (owned (not (cffi:null-pointer-p hdrs))))
    (cffi:with-foreign-string (topic-ptr topic)
      (multiple-value-bind (key-ptr key-len) (%copy-foreign key-oct)
        (multiple-value-bind (val-ptr val-len) (%copy-foreign val)
          (unwind-protect
               (progn
                 (%produceva (rdkafka-ctx-producer ctx)
                             topic-ptr key-ptr key-len
                             val-ptr val-len hdrs)
                 (setf owned nil)
                 (%rd-poll (rdkafka-ctx-producer ctx) 0)
                 message)
            (cffi:foreign-free key-ptr)
            (cffi:foreign-free val-ptr)
            (when (and owned (not (cffi:null-pointer-p hdrs)))
              (%rd-headers-destroy hdrs))))))))

(defun %meta-key (topic partition offset)
  (list topic partition offset))

(defun %message-from-rk (ctx rkmessage)
  (let* ((rkt (cffi:foreign-slot-value rkmessage '(:struct rd-kafka-message)
                                       'rkt))
         (topic (if (cffi:null-pointer-p rkt)
                    nil
                    (%rd-topic-name rkt)))
         (partition (cffi:foreign-slot-value
                     rkmessage '(:struct rd-kafka-message) 'partition))
         (offset (cffi:foreign-slot-value
                  rkmessage '(:struct rd-kafka-message) 'offset))
         (payload-ptr (cffi:foreign-slot-value
                       rkmessage '(:struct rd-kafka-message) 'payload))
         (len (cffi:foreign-slot-value
               rkmessage '(:struct rd-kafka-message) 'len))
         (key-ptr (cffi:foreign-slot-value
                   rkmessage '(:struct rd-kafka-message) 'key))
         (key-len (cffi:foreign-slot-value
                   rkmessage '(:struct rd-kafka-message) 'key-len))
         (tag (incf (rdkafka-ctx-next-tag ctx)))
         (meta (%meta-key topic partition offset))
         (msg (mq-protocol:make-mq-message
               :key (unless (or (cffi:null-pointer-p key-ptr) (zerop key-len))
                      (%octets-to-string (%foreign-octets key-ptr key-len)))
               :payload (if (cffi:null-pointer-p payload-ptr)
                            #()
                            (%foreign-octets payload-ptr len))
               :headers (%read-headers rkmessage)
               :delivery-tag tag
               :topic topic
               :redelivered-p (and (gethash meta (rdkafka-ctx-redeliver ctx))
                                   t))))
    (setf (gethash tag (rdkafka-ctx-inflight ctx)) meta)
    msg))

(defun %ensure-assigned (ctx destination)
  (let ((topic (string destination)))
    (unless (gethash topic (rdkafka-ctx-assigned ctx))
      (let ((list (%rd-tpl-new 1)))
        (let ((tp (%rd-tpl-add list topic 0)))
          (setf (cffi:foreign-slot-value
                 tp '(:struct rd-kafka-topic-partition) 'offset)
                +rd-kafka-offset-beginning+))
        (let ((rc (%rd-assign (rdkafka-ctx-consumer ctx) list)))
          (%rd-tpl-destroy list)
          (unless (zerop rc)
            (error 'mq-protocol:mq-error
                   :message (format nil "rd_kafka_assign: ~a"
                                    (%rd-err2str rc)))))
        (setf (gethash topic (rdkafka-ctx-assigned ctx)) t)))))

(defun %poll-one (ctx timeout-ms)
  (let ((msg (%rd-consumer-poll (rdkafka-ctx-consumer ctx) timeout-ms)))
    (when (or (null msg) (cffi:null-pointer-p msg))
      (return-from %poll-one nil))
    (unwind-protect
         (let ((err (cffi:foreign-slot-value
                     msg '(:struct rd-kafka-message) 'err)))
           (when (zerop err)
             (%message-from-rk ctx msg)))
      (%rd-message-destroy msg))))

(defun %poll-messages (ctx destination &key (n 1) timeout)
  (%ensure-assigned ctx destination)
  (let* ((want (or n 1))
         (seconds (or timeout 15))
         (deadline (+ (get-internal-real-time)
                      (round (* seconds internal-time-units-per-second))))
         (acc '()))
    (loop while (and (< (length acc) want)
                     (< (get-internal-real-time) deadline))
          do (let ((m (%poll-one ctx 400)))
               (when m (push m acc))))
    (nreverse acc)))

(defun %ack (ctx message)
  (let ((tag (if (integerp message)
                 message
                 (mq-protocol:mq-message-delivery-tag message))))
    (remhash tag (rdkafka-ctx-inflight ctx))
    message))

(defun %nack (ctx message &key (requeue t))
  (let* ((tag (if (integerp message)
                  message
                  (mq-protocol:mq-message-delivery-tag message)))
         (meta (gethash tag (rdkafka-ctx-inflight ctx))))
    (when (and requeue meta)
      (destructuring-bind (topic partition offset) meta
        (let ((rkt (%rd-topic-new (rdkafka-ctx-consumer ctx)
                                  topic
                                  (cffi:null-pointer))))
          (unless (cffi:null-pointer-p rkt)
            (let ((rc (%rd-seek rkt partition offset 5000)))
              (%rd-topic-destroy rkt)
              (unless (zerop rc)
                (error 'mq-protocol:mq-error
                       :message (format nil "rd_kafka_seek: ~a"
                                        (%rd-err2str rc))))))
          (setf (gethash meta (rdkafka-ctx-redeliver ctx)) t))))
    (remhash tag (rdkafka-ctx-inflight ctx))
    message))

(defun %driver (ctx)
  (list :publish
        (lambda (backend dest message)
          (declare (ignore backend))
          (%publish ctx dest message))
        :poll
        (lambda (backend dest &key n timeout)
          (declare (ignore backend))
          (%poll-messages ctx dest :n n :timeout timeout))
        :ack
        (lambda (backend message)
          (declare (ignore backend))
          (%ack ctx message))
        :nack
        (lambda (backend message &key requeue)
          (declare (ignore backend))
          (%nack ctx message :requeue requeue))
        :subscribe
        (lambda (backend dest handler)
          (declare (ignore backend dest handler))
          nil)))

(defun %destroy-ctx (ctx)
  (when (rdkafka-ctx-consumer ctx)
    (ignore-errors (%rd-consumer-close (rdkafka-ctx-consumer ctx)))
    (ignore-errors (%rd-destroy (rdkafka-ctx-consumer ctx)))
    (setf (rdkafka-ctx-consumer ctx) nil))
  (when (rdkafka-ctx-producer ctx)
    (ignore-errors (%rd-flush (rdkafka-ctx-producer ctx) 2000))
    (ignore-errors (%rd-destroy (rdkafka-ctx-producer ctx)))
    (setf (rdkafka-ctx-producer ctx) nil)))

(defun make-live-kafka-backend (&key (bootstrap (kafka-broker-endpoint))
                                  group-id)
  (unless (ensure-librdkafka)
    (error 'mq-protocol:mq-error
           :message (or *librdkafka-error* "librdkafka not loaded")))
  (let* ((bootstrap (or bootstrap *kafka-bootstrap-default*))
         (group (or group-id
                    (format nil "mq-parity-~d-~d"
                            (get-universal-time)
                            (random 100000))))
         (producer (%make-handle +rd-kafka-producer+ bootstrap
                                 '(("acks" . "all")
                                   ("linger.ms" . "0")
                                   ("allow.auto.create.topics" . "true"))))
         (consumer (%make-handle +rd-kafka-consumer+ bootstrap
                                 (list (cons "group.id" group)
                                       (cons "enable.auto.commit" "false")
                                       (cons "auto.offset.reset" "earliest")
                                       (cons "enable.partition.eof" "false")
                                       (cons "allow.auto.create.topics" "true"))))
         (ctx (%make-rdkafka-ctx :bootstrap bootstrap
                                 :producer producer
                                 :consumer consumer
                                 :group-id group)))
    (%rd-poll-set-consumer consumer)
    (let ((backend (mq-backend-kafka:make-kafka-backend
                    :bootstrap-servers bootstrap
                    :cffi-library-path (uiop:getenv "MQ_PARITY_LIBRDKAFKA")
                    :driver (%driver ctx))))
      (setf (gethash backend *kafka-contexts*) ctx)
      backend)))

(defun destroy-live-kafka-backend (backend)
  (let ((ctx (gethash backend *kafka-contexts*)))
    (when ctx
      (%destroy-ctx ctx)
      (remhash backend *kafka-contexts*)))
  backend)

(defun call-with-live-kafka (fn &key bootstrap group-id)
  (let ((backend (make-live-kafka-backend :bootstrap bootstrap
                                          :group-id group-id)))
    (unwind-protect (funcall fn backend)
      (destroy-live-kafka-backend backend))))

(defun kafka-create-topic (backend topic &key (partitions 1) (replication 1))
  "Create TOPIC via librdkafka Admin API. Auto-create still covers failure."
  (let ((ctx (or (gethash backend *kafka-contexts*)
                 (error 'mq-protocol:mq-error
                        :message "not a live kafka backend"))))
    (let ((errstr (cffi:foreign-alloc :char :count 512)))
      (unwind-protect
           (let ((nt (%rd-newtopic-new topic partitions replication errstr 512)))
             (when (cffi:null-pointer-p nt)
               (return-from kafka-create-topic nil))
             (let* ((opts (%rd-adminoptions-new (rdkafka-ctx-producer ctx)
                                                +rd-kafka-admin-op-createtopics+))
                    (q (%rd-queue-new (rdkafka-ctx-producer ctx))))
               (%rd-adminoptions-set-timeout opts 10000 errstr 512)
               (cffi:with-foreign-object (arr :pointer 1)
                 (setf (cffi:mem-aref arr :pointer 0) nt)
                 (%rd-create-topics (rdkafka-ctx-producer ctx) arr 1 opts q))
               (let ((ev (%rd-queue-poll q 15000)))
                 (unless (cffi:null-pointer-p ev)
                   (%rd-event-destroy ev)))
               (%rd-queue-destroy q)
               (%rd-adminoptions-destroy opts)
               (%rd-newtopic-destroy nt)
               (%rd-flush (rdkafka-ctx-producer ctx) 2000)
               t))
        (cffi:foreign-free errstr)))))

(defun %unique-topic (label)
  (format nil "parity.~a.~d.~d"
          label (get-universal-time) (random 1000000)))

(defun kafka-produce-consume-canary (&key (count 10))
  "Produce COUNT keyed+headed messages, consume+ack, return :sent/:got."
  (call-with-live-kafka
   (lambda (backend)
     (let ((topic (%unique-topic "order")))
       (kafka-create-topic backend topic :partitions 1)
       (let ((sent '()))
         (dotimes (i count)
           (let* ((key (if (evenp i) "alpha" "beta"))
                  (headers (list (cons "canary" "mq-parity")
                                 (cons "seq" (princ-to-string i))
                                 (cons "key" key)))
                  (msg (mq-protocol:make-mq-message
                        :key key
                        :payload (format nil "body-~d" i)
                        :headers headers
                        :topic topic)))
             (mq-protocol:publish backend topic msg)
             (push (list :key key :seq i :headers headers :payload
                         (mq-protocol:mq-message-payload msg))
                   sent)))
         (let ((ctx (gethash backend *kafka-contexts*)))
           (%rd-flush (rdkafka-ctx-producer ctx) 10000))
         (let ((got (mq-protocol:poll-messages backend topic
                                               :n count :timeout 20)))
           (dolist (m got)
             (mq-protocol:ack backend m))
           (list :topic topic
                 :sent (nreverse sent)
                 :got got)))))))

(defun kafka-nack-redelivery-canary ()
  "Publish one message, nack+requeue, consume the redelivery, ack."
  (call-with-live-kafka
   (lambda (backend)
     (let ((topic (%unique-topic "nack")))
       (kafka-create-topic backend topic :partitions 1)
       (let ((msg (mq-protocol:make-mq-message
                   :key "nack-key"
                   :payload "nack-body"
                   :headers (list (cons "canary" "mq-parity")
                                  (cons "kind" "nack"))
                   :topic topic)))
         (mq-protocol:publish backend topic msg)
         (let ((ctx (gethash backend *kafka-contexts*)))
           (%rd-flush (rdkafka-ctx-producer ctx) 10000))
         (let ((first (first (mq-protocol:poll-messages backend topic
                                                        :n 1 :timeout 15))))
           (unless first
             (error 'mq-protocol:mq-error
                    :message "kafka nack canary: no first delivery"))
           (mq-protocol:nack backend first :requeue t)
           (let ((second (first (mq-protocol:poll-messages backend topic
                                                           :n 1 :timeout 15))))
             (unless second
               (error 'mq-protocol:mq-error
                      :message "kafka nack canary: no redelivery"))
             (mq-protocol:ack backend second)
             (list :first first :second second))))))))
