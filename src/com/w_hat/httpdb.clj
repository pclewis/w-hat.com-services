(ns com.w-hat.httpdb
  (:require (com.w-hat [db :as db] [sl :as sl] [re :as re])
            [taoensso.carmine :as car]
            [pallet.md5crypt :as md5]))

(defn create-user
  [uuid]
  (let [user {:space-available 250000
              :space-used      0}]
    (db/wcar (car/hset "httpdb-users" uuid user))
    user))
  
(defn user
  [uuid]
  (if-let [user (or (db/wcar (car/hget "httpdb-users" uuid)) (create-user uuid))]
    (into user {:uuid uuid})))

(defn- httpdb-key [uuid] (str "hdb:" uuid))
(defn- meta-key   [uuid] (str "hdb:" uuid ":meta"))

;; TODO: hash new passwords with something better
(defn- check-password
  "Check if password matches any of crypted-pws"
  [password & crypted-pws]
  (if password
    (some #(= % (md5/crypt password %)) (filter identity crypted-pws))))

(defn- serialized-length
  "Get the length of the serialized value of a key."
  [key]
  (if (zero? (db/wcar (car/exists key)))
    0
    (. Integer parseInt (last (first (re-seq #"serializedlength:([0-9]+)" (db/wcar (car/debug-object key))))))))

(defn update-space
  [user]
  (let [hk (httpdb-key (:uuid user))
        mk (meta-key (:uuid user))]
    (db/wcar (car/watch hk)
             (car/watch mk)
             (car/multi)
             (car/hset "httpdb-users" (:uuid user) (into user {:space-used (+ (serialized-length hk) (serialized-length mk))}))
             (car/exec))))

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

(defn space-free [u] (- (:space-available u) (:space-used u)))

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
          (update-space owner)
          (space-free (user (:uuid owner)))))))

;; TODO: encrypt passwords, filter attrs?
(defn- set-meta*
  [requestor owner path password attrs]
  (if-not (check-password password (:admin-password owner))
    :denied
    (let [meta (db/wcar (car/hget (meta-key (:uuid owner)) path))
          newmeta (into (or meta {}) attrs)]
      (do (db/wcar (car/hset (meta-key (:uuid owner)) path newmeta))
        (update-space owner)
        (space-free (user (:uuid owner)))))))

(defn- set-user-meta*
  [requestor password attrs]
  (if-not (check-password password (:admin-password requestor))
    :denied
    (let [newmeta (into {} (filter val {:default-read-password (:read-password attrs)
                                        :default-write-password (:write-password attrs)}))]
      (db/wcar (car/hset "httpdb-users" (:uuid requestor) (into (-> requestor :uuid user) newmeta))))))
 
(def ^:private RE_SHARED_PATH (re-pattern (str "__/" , "(?<uuid>" sl/RE_UUID ")" , "/" , "(?<path>.*)" )))
(defn get
  [requestor path password]
  (cond (= path "__sys/space/free")      (- (:space-available user) (:space-used user))
        (= path "__sys/space/used")      (:space-used user)
        (= path "__sys/space/available") (:space-available user)
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