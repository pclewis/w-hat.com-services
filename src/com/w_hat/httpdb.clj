(ns com.w-hat.httpdb
  (:refer-clojure :exclude [get set])
  (:require (com.w-hat [db :as db] [sl :as sl] [re :as re])
            [taoensso.carmine :as car])
  (:import org.apache.commons.codec.digest.Crypt))

(defn encrypt-password
  ([^String pass] (Crypt/crypt pass))
  ([^String pass ^String salt] (Crypt/crypt pass salt)))

(def ^:private KEY_ROOT  "hdb")
(def ^:private KEY_USERS (str KEY_ROOT ":" "users"))
(def RECORD_META [:created-at :updated-at :created-by :updated-by :read-password :write-password])
(def USER_META   [:admin-password :default-write-password :default-read-password :disable-record-metadata])

(defn- is-uuid? [u] (boolean (re-matches sl/RE_UUID u)))

(defn- get-uuid [u] {:post [(is-uuid? %)]} (if (map? u) (:uuid u) u))
(defn- data-key [u] (str KEY_ROOT ":" (get-uuid u) ":" "data"))
(defn- meta-key [u] (str KEY_ROOT ":" (get-uuid u) ":" "meta"))

(defn- ensure-user-exists
  [uuid]
  {:pre [(is-uuid? uuid)]}
   (db/wcar (car/hsetnx KEY_USERS (str uuid :space-available) 250000)
            (car/hsetnx KEY_USERS (str uuid :space-used)      0)))

(defn is-user?
  "True if u has the minimum set of required keys to be considered a user."
  [u]
  (every? (partial contains? u) [:uuid :space-available :space-used :space-free]))

(defn user
  [uuid]
  {:pre  [(re-matches sl/RE_UUID uuid)]
   :post [(is-user? %)]}
  (ensure-user-exists uuid)
  (let [user (db/wcar (db/hgetmap KEY_USERS uuid {:space-available         car/as-long
                                                  :space-used              car/as-long
                                                  :disable-record-metadata car/as-bool
                                                  :admin-password          identity
                                                  :default-read-password   identity
                                                  :default-write-password  identity}))]
    (into user {:space-free (- (:space-available user) (:space-used user))
                :uuid uuid})))

(defn check-password
  "Check if password matches any of crypted-pws"
  [password & crypted-pws]
  (if password
    (some #(= % (encrypt-password password %)) (remove nil? crypted-pws))))

(defn- serialized-length
  "Get the length of the serialized value of a key."
  [key]
  (if (db/wcar (db/exists? key))
    (Integer/parseInt (last (first (re-seq #"serializedlength:([0-9]+)" (db/wcar (car/debug-object key))))))
    0))

(defn- update-space-used
  [user]
  {:pre [(is-user? user)]}
  (let [uuid (:uuid user)
        dk   (data-key uuid)
        mk   (meta-key uuid)
        res  (db/wcar (car/atomically
                       [dk mk]
                       (let [new-space-used (+ (serialized-length dk) (serialized-length mk))]
                         (car/hset KEY_USERS (str uuid :space-used) new-space-used)
                         (car/echo new-space-used))))]
    (if-not (empty? res) (Integer/parseInt (last res)))))

(defn- encrypt-passwords
  [m]
  (reduce #(update-in %1 [%2] encrypt-password) m
          (filter #(.endsWith (str %) "-password")
                  (keys (filter val m)))))

(defn- get-meta
  [uuid path]
  (db/wcar (db/hgetmap (meta-key uuid) path RECORD_META)))

(defn- set-meta-generic
  [hkey prefix meta]
  (->> meta
       (encrypt-passwords)
       (db/hsetmap hkey prefix)
       (car/atomically [])
       (db/wcar)))

(defn- now
  []
  (let [df (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'")
        tz (java.util.TimeZone/getTimeZone "UTC")]
    (.setTimeZone df tz)
    (.format df (java.util.Date.))))

(comment
  (serialized-now))

(defn- append-to-keyword
  [k s]
  (-> (str k s) (subs 1) (keyword)))

(defn- set-update-meta
  [type requestor owner path]
  (let [at (append-to-keyword type "-at")
        by (append-to-keyword type "-by")]
    (set-meta-generic (meta-key owner) path {at (now) by (if-not (= requestor owner) (:uuid requestor))})))

(defn get
  "Look up record owned by owner and return it if user has read access to it with the supplied password."
  [user owner path password]
  {:pre [(is-user? user)]}
  (if-let [record (db/wcar (car/hget (data-key owner) path))]
    (if-let [meta (get-meta owner path)]
      (if (:read-password meta)
        (if (check-password password (:read-password meta) (:admin-password owner))
          record
          :denied)
        record)
      record)))

(defn set
  "Set a record if requestor is allowed to with the supplied password, else return :denied"
  [requestor owner path password mode data attrs]
  {:pre [(is-user? requestor) (is-user? owner)]}
  (let [record (db/wcar (car/hget (data-key owner) path))
        meta   (get-meta owner path)
        newmeta (select-keys attrs RECORD_META)
        wd     (case mode
                 :append  (str record data)
                 :prepend (str data record)
                 data)]
    (if (and (:write-password meta) (not (check-password password :write-password (:admin-password owner))))
        :denied
        (do (db/wcar (car/hset (data-key owner) path wd))
            (if (and (not-empty newmeta) ;; can set meta if new record and we're owner, or we have admin pass
                     (or (and (= owner requestor) (not record))
                         (check-password password (:admin-password owner))))
              (set-meta-generic (meta-key owner) path newmeta))
            (if-not (:disable-record-metadata owner)
              (do
                (if-not record (set-update-meta :created requestor owner path))
                (set-update-meta :updated requestor owner path)))
            (update-space-used owner)))))

(defn delete
  [requestor owner path password]
  {:pre [(is-user? requestor) (is-user? owner)]}
  (let [meta (db/wcar (car/hget (meta-key owner) path))]
    (if (and (or (not= owner requestor) (:write-password meta))
             (not (check-password password (:write-password meta) (:admin-password owner))))
        :denied
        (do (db/wcar (car/atomically []
                      (car/hdel (data-key owner) path)
                      (db/hsetmap (meta-key owner) path (apply hash-map (mapcat list RECORD_META (repeat nil))))))
            (update-space-used owner)))))

(defn set-meta
  [requestor owner path password attrs]
  {:pre [(is-user? requestor) (is-user? owner)]}
  (if-not (check-password password (:admin-password owner))
    :denied
    (do (set-meta-generic (meta-key owner) path (select-keys attrs RECORD_META))
        (update-space-used owner))))

(defn set-user-meta
  [requestor password attrs]
  {:pre [(is-user? requestor)]}
  (if-not (check-password password (:admin-password requestor))
    :denied
    (set-meta-generic KEY_USERS (:uuid requestor) (select-keys attrs USER_META))))
