(ns com.w-hat.httpdb
  (:refer-clojure :exclude [get set list])
  (:require (com.w-hat [db :as db] [sl :as sl] [re :as re] [util :refer :all])
            [taoensso.carmine :as car])
  (:import org.apache.commons.codec.digest.Crypt))

(def ^:private KEY_ROOT  "hdb")
(def ^:private KEY_USERS (str KEY_ROOT ":" "users"))

(defn- keyname [type id] (str KEY_ROOT ":" id type))

(defn encrypt-password
  ([^String pass]              (Crypt/crypt pass))
  ([^String pass ^String salt] (Crypt/crypt pass salt)))

(defn- parse-bool [v] (case v ("true" "t" "1" "yes" "y") 1 nil))

(declare calculate-space-used now)

;; fns are passed [requestor owner path], except calculate is passed [id self]
(def user-fields
  {:id                     {:calculate (fn [id _] id)}
   :space-available        {:default 250000 :get car/as-long}
   :space-used             {:default 0      :get car/as-long :on-record-update (fn [_ o _] (calculate-space-used (:id o)))}
   :space-free             {:calculate (fn [_ {:keys [space-used space-available]}] (- space-available space-used))}
   :track-create           {:set parse-bool :get car/as-bool}
   :track-update           {:set parse-bool :get car/as-bool}
   :default-read-password  {:set encrypt-password :get identity}
   :default-write-password {:set encrypt-password :get identity}
   :admin-password         {:set encrypt-password :get identity}})

(def record-fields
  {:created-by             {:default (fn [_ o _] (if (:track-create o) (:id o)))
                            :on-record-create (fn [r o _] (if (:track-create o) (:id r)))}
   :created-at             {:on-record-create (fn [_ o _] (if (:track-create o) (now)))}
   :updated-by             {:default (fn [_ o _] (if (:track-update o) (:id o)))
                            :on-record-update (fn [r o _] (if (:track-update o) (:id r)))}
   :updated-at             {:on-record-update (fn [_ o _] (if (:track-update o) (now)))}
   :read-password          {:default (fn [_ o _] (:default-read-password o))
                            :set encrypt-password}
   :write-password         {:default (fn [_ o _] (:default-create-password o))
                            :set encrypt-password}})

(defn set-fields
  "Return a map containing the result of calling (:set) for each entry in fs on the corresponding entry in m."
  [fs m]
  (let [ks (clojure.set/intersection (clojure.core/set (keys m)) (clojure.core/set (keys (filter-subkeys #{:set} fs))))]
    (merge-with #((:set %1) %2) (select-keys fs ks) (select-keys m ks))))

(defn calculate-fields
  "Return a map containing the results of calling k with args for each entry in fs that contains k."
  [fs k & args]
  (mmap #(apply (k %) args) (filter-subkeys #{k} fs)))

(defn- get-default [f & args]
  (let [d (:default f)]
    (if (isa? (class d) Runnable)
      (apply d args)
      d)))

(defn get-new-meta
  [fs m ks & args]
  (let [nm (apply merge (set-fields fs m) (map #(apply calculate-fields fs % args) (if (coll? ks) ks [ks])))]
    (merge-with #(if-not (= %1 %2) %2) ;; nil-ify values that match default
                (mmap #(apply get-default % args) (select-keys fs (keys nm)))
                nm)))

;; dont think this is actually needed
(defn- ensure-user-exists
  "Set default user fields if they do not already exist."
  [id]
  (db/wcar (doseq [[k v] (filter val (mmap :default user-fields))]
             (car/hsetnx KEY_USERS (str id k) v))))

(defn user?
  "True if user has the minimum set of required keys to be considered a user."
  [user]
  (every? #(contains? user %) (keys (filter-subkeys #{:default :calculate} user-fields))))

(defn load-user
  [id]
  {:post [(user? %)]}
  (let [user (db/wcar (db/hgetmap KEY_USERS id (mmap :get user-fields)))]
    (merge (mmap get-default user-fields)
           (filter val user)
           (mmap #((:calculate %) id user) (filter-subkeys #{:calculate} user-fields)))))

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

(defn- calculate-space-used
  [id]
  (apply + (map #(-> % (keyname id) serialized-length) [:data :meta])))

(defn- get-record-meta
  [user path & ks]
  (let [fields (if (empty? ks) record-fields (select-keys record-fields ks))
        meta (merge (mmap #(get-default % nil user path) fields)
                    (filter val (db/wcar (db/hgetmap (keyname :meta (:id user)) path
                                                     (mmap #(clojure.core/get % :get identity) fields)))))]
    (if (= 1 (count ks)) (get-in meta ks) meta)))

(defn- get-record [owner path]
  (db/wcar (car/hget (keyname :data (:id owner)) path)))

(defn- update-user [user metafn]
  (db/atomically
   [(keyname :meta (:id user)) (keyname :data (:id user))]
   (db/hsetmap KEY_USERS (:id user) (metafn))))

(defn- set-record [owner path mode data meta]
  (let [dk (keyname :data (:id owner))]
    (db/atomically
     (if (#{:append :prepend} mode) [dk] []) ;; watch dk if we're basing new value off old value
     (let [nd (case mode :append  (str (get-record owner path) data)
                         :prepend (str data (get-record owner path))
                         data)]
       (car/hset dk path nd))
     (db/hsetmap (keyname :meta (:id owner)) path meta))))

(comment
  (set-record masau masau "test" nil "testing" {:write-password "hi"}) )

(defn- record-exists? [user path]
  (db/hexists? (keyname :data (:id user)) path))

(defn- now
  []
  (let [df (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'")
        tz (java.util.TimeZone/getTimeZone "UTC")]
    (.setTimeZone df tz)
    (.format df (java.util.Date.))))

(defn get
  "Look up record owned by owner and return it if user has read access to it with the supplied password."
  [requestor owner path password]
  (let [record        (get-record owner path)
        read-password (get-record-meta owner path :read-password)]
    (if (or (nil? read-password)
            (check-password password read-password (:admin-password owner)))
      record
      :denied)))

(defn set
  [requestor owner path password mode data meta]
  (let [exists?        (record-exists? owner path)
        write-password (get-record-meta owner path :write-password)
        events         (cons :on-record-update (if-not exists? [:on-record-create]))]
    (if (or (and exists? (nil? write-password))                               ;; exists but no password
            (and (not exists?) (= requestor owner))                           ;; doesn't exist and we're owner
            (check-password password write-password (:admin-password owner))) ;; we have a valid password
      (do (set-record owner path mode data (get-new-meta record-fields meta events requestor owner path))
          (update-user owner #(get-new-meta user-fields {} events requestor owner path)))
      :denied)))

(defn delete
  [requestor owner path password]
  (when (record-exists? owner path)
    (let [write-password (get-record-meta owner path :write-password)]
      (if [(or (nil? write-password)
                (check-password password write-password (:admin-password owner)))]
        (do (db/wcar (car/hdel (keyname :data (:id owner)) path))
            (update-user owner #(get-new-meta user-fields {} [:on-record-delete :on-record-update] requestor owner path)))
        :denied))))

(defn set-meta
  [requestor owner path password meta]
  (when (record-exists? owner path)
    (if-not (check-password password (:admin-password owner))
      :denied
      (do (db/atomically
           [] (db/hsetmap (keyname :meta (:id owner)) path
                          (get-new-meta record-fields meta :on-record-update requestor owner path)))
          (update-user owner #(get-new-meta user-fields {} :on-record-update requestor owner path))))))

(defn set-user-meta
  [owner password meta]
  (if-not (check-password password (:admin-password owner))
    :denied
    (update-user owner #(get-new-meta user-fields meta []))))

(defn list
  [owner path]
  (filter #(.startsWith % path) (db/wcar (car/hkeys (keyname :data (:id owner))))))

(comment
  (get-new-meta user-fields {} [:on-record-delete :on-record-update] masau masau "wat")
  (set masau masau "_test_" nil nil "test" {:read-password "hi"})
  (get masau masau "_test_" "hi")
  (delete masau masau "_test_" nil)
  (set-meta masau masau "_test_" "tester" {:read-password "hi"})
  (set-user-meta masau "tester" {:track-create "1"})
  (get-record-meta masau "_test")
  (list masau ""))
