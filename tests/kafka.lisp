(in-package #:mq-parity/tests)

(defun %header (headers name)
  (cond
    ((null headers) nil)
    ((and (consp headers) (consp (first headers)))
     (let ((v (cdr (assoc name headers :test #'string-equal))))
       (when v (mq-parity::%octets-to-string v))))
    (t
     (loop for (k v) on headers by #'cddr
           when (string-equal (string k) name)
             return (mq-parity::%octets-to-string v)))))

(defun %key (message)
  (mq-parity::%octets-to-string (mq-protocol:mq-message-key message)))

(defun %payload-string (message)
  (mq-parity::%octets-to-string (mq-protocol:mq-message-payload message)))

(defun %seqs-for-key (messages key)
  (loop for m in messages
        when (string= (%key m) key)
          collect (parse-integer (%header (mq-protocol:mq-message-headers m) "seq")
                                 :junk-allowed t)))

(deftest kafka-compose-ps-probe
  (ok (member (kafka-compose-broker-p) '(t nil))))

(deftest kafka-bootstrap-probe
  (ok (or (null (kafka-bootstrap))
          (stringp (kafka-bootstrap)))))

(deftest kafka-produce-consume-order-and-headers
  (multiple-value-bind (run-p reason)
      (kafka-live-ready-p)
    (if (not run-p)
        (skip reason)
        (let* ((result (kafka-produce-consume-canary :count 10))
               (got (getf result :got)))
          (ok (= 10 (length got)))
          (dolist (m got)
            (ok (string= "mq-parity"
                         (%header (mq-protocol:mq-message-headers m) "canary")))
            (ok (string= (%key m)
                         (%header (mq-protocol:mq-message-headers m) "key"))))
          (let ((alpha (%seqs-for-key got "alpha"))
                (beta (%seqs-for-key got "beta")))
            (ok (equal alpha (sort (copy-list alpha) #'<)))
            (ok (equal beta (sort (copy-list beta) #'<)))
            (ok (plusp (length alpha)))
            (ok (plusp (length beta))))))))

(deftest kafka-nack-redelivery
  (multiple-value-bind (run-p reason)
      (kafka-live-ready-p)
    (if (not run-p)
        (skip reason)
        (let* ((result (kafka-nack-redelivery-canary))
               (first (getf result :first))
               (second (getf result :second)))
          (ok (mq-protocol:mq-message-p first))
          (ok (mq-protocol:mq-message-p second))
          (ok (string= "nack-body" (%payload-string first)))
          (ok (string= "nack-body" (%payload-string second)))
          (ok (string= "mq-parity"
                       (%header (mq-protocol:mq-message-headers second) "canary")))
          (ok (mq-protocol:mq-message-redelivered-p second))))))
