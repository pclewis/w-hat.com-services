(ns com.w-hat.httpdb
  (:require (com.w-hat [db :as db] [sl :as sl] [re :as re])
            [taoensso.carmine :as car])
  (:import org.apache.commons.codec.digest.Crypt))

(defn encrypt-password
  ([pass] (Crypt/crypt pass))
  ([pass salt] (Crypt/crypt pass salt)))

(defn- httpdb-key [uuid] (str "hdb:" uuid))
(defn- meta-key   [uuid] (str "hdb:" uuid ":meta"))

(defn- ensure-user-exists
  [uuid]
   (db/wcar (car/hsetnx "httpdb-users" (str uuid :space-available) 250000)
            (car/hsetnx "httpdb-users" (str uuid :space-used)      0)))
  
(defn user
  [uuid]
  (ensure-user-exists uuid)
  (let [user (db/wcar (db/hgetmap "httpdb-users" uuid {:space-available        car/as-long
                                                       :space-used             car/as-long
                                                       :admin-password         identity
                                                       :default-read-password  identity 
                                                       :default-write-password identity}))]
    (into user {:space-free (- (:space-available user) (:space-used user))
                :uuid uuid})))

(defn check-password
  "Check if password matches any of crypted-pws"
  [password & crypted-pws]
  (if password
    (some #(= % (encrypt-password password %)) (filter identity crypted-pws))))

(defn- serialized-length
  "Get the length of the serialized value of a key."
  [key]
  (if (db/wcar (db/exists? key))
    (. Integer parseInt (last (first (re-seq #"serializedlength:([0-9]+)" (db/wcar (car/debug-object key))))))
    0))

(defn- update-space-used
  [user]
  (let [uuid (:uuid user)
        hk   (httpdb-key uuid)
        mk   (meta-key uuid)
        res  (db/wcar (car/atomically
                       [hk mk]
                       (let [new-space-used (+ (serialized-length hk) (serialized-length mk))]
                         (car/hset "httpdb-users" (str uuid :space-used) new-space-used)
                         (car/echo new-space-used))))]
    (if-not (empty? res) (Integer/parseInt (last res)))))

(def RECORD_META [:date-created :date-updated :created-by :updated-by :read-password :write-password])
(def USER_META [:admin-password :default-write-password :default-read-password])

(defn- get-meta
  [uuid path]
  (db/wcar (db/hgetmap (meta-key uuid) path RECORD_META)))

(defn get-accessible-record
  "Look up record owned by owner and return it if user has read access to it with the supplied password."
  [user owner path password]
  (if-let [record (db/wcar (car/hget (httpdb-key (:uuid owner)) path))]
    (if-let [meta (db/wcar (car/hget (meta-key   (:uuid owner)) path))]
      (if (:read-password meta)
        (if (check-password password (:read-password meta) (:admin-password owner))
          record
          :denied)
        record)
      record)))

;; TODO: can set meta on new records
(defn- set*
  "Set a record if requestor is allowed to with the supplied password, else return :denied"
  [requestor owner path password mode data]
  (let [record (db/wcar (car/hget (httpdb-key (:uuid owner)) path))
        meta   (db/wcar (car/hget (meta-key   (:uuid owner)) path))
        wd     (case mode
                 :append (str record data)
                 :prepend (str data record)
                 data)]
      (if (and (:write-password meta) (not (check-password password (:read-password meta) (:admin-password owner))))
        :denied
        (do (db/wcar (car/hset (httpdb-key (:uuid owner)) path wd))
          (update-space-used owner)))))


(defn- encrypt-passwords [m]
  (reduce #(update-in %1 [%2] encrypt-password) m
          (filter #(.endsWith (str %) "-password")
                  (keys (filter val m)))))

(defn- set-meta-generic [hkey prefix meta] (db/wcar (db/hsetmap hkey prefix (encrypt-passwords meta))))

(defn- set-meta*
  [requestor owner path password attrs]
  (if-not (check-password password (:admin-password owner))
    :denied
    (do (set-meta-generic (meta-key (:uuid owner)) path (select-keys attrs RECORD_META))
        (update-space-used owner))))

(defn- set-user-meta
  [requestor password attrs]
  (if-not (check-password password (:admin-password requestor))
    :denied
    (set-meta-generic "httpdb-users" (:uuid requestor) (select-keys attrs USER_META))))
 
(def ^:private RE_SHARED_PATH (re-pattern (str "__/" , "(?<uuid>" sl/RE_UUID ")" , "/" , "(?<path>.*)" )))
(defn get
  [requestor path password]
  (cond (= path "__sys/space/free")      (:space-free requestor)
        (= path "__sys/space/used")      (:space-used requestor)
        (= path "__sys/space/available") (:space-available requestor)
        (.startsWith path "__/")         (if-let [match (re/match-exact RE_SHARED_PATH path)]
                                           (get-accessible-record requestor (user (:uuid match)) path password))
        :else                            (get-accessible-record requestor requestor path password)))

(defn set
  [requestor path password mode data]
  (cond (.startsWith path "__/") (if-let [match (re/match-exact RE_SHARED_PATH path)]
                                   (set* requestor (user (:uuid match)) path password mode data))
        (.startsWith path "__")  :denied
        :else                    (set* requestor requestor path password mode data)))

(defn set-meta
  [requestor path password attrs]
  (cond (= path "__sys/defaults") (set-user-meta requestor password attrs)
        (.startsWith path "__/")  (if-let [match (re/match-exact RE_SHARED_PATH path)]
                                    (set-meta* requestor (user (:uuid match)) path password attrs))
        (.startsWith path "__")   :denied
        :else                     (set-meta* requestor requestor path password attrs)))
