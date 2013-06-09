(ns com.w-hat.ndb
  "Configurable database and database mapping."
  (:refer-clojure :exclude [get])
  (:require (com.w-hat [config :as config])
            [taoensso.carmine :as car]
            [plumbing.core :refer [map-keys]]))

;; Redis has some cool commands, but to get acceptable memory usage
;; everything needs to be crammed into hashes so it can take advantage
;; of ziplists. This limits us to the most basic of operations and makes
;; the database structure awkward.
;;
;; Additionally, the fact that system memory imposes a ceiling on the
;; size of the data store makes me question whether Redis is an
;; appropriate choice for a service like httpdb where it's relatively
;; easy for an anonymous and basically unstoppable person to cause a
;; near-infinite increase in the database size.
;;
;; This namespace lets us present the database as a nested map store to
;; the app and confine data structure optimization wackiness to the
;; config file in a simple and flexible way. It also provides an easy
;; path to exploring alternative backends for any part of the app
;; without impacting application code.

(defprotocol DB
  (get ^String [db path])
  (put [db path value])
  (delete [db path])
  (exists? [db path])
  (size [db path])
  (list [db path])
  (put-map [db path map]
     "Update multiple keys based on entries in map. Each key will be appended to path. Entries
     with a nil value will be deleted.
     Ex: (put-map db [:a] {:b 1 :c nil}) is equivalent to:
         (put db [:a :b] 1) (delete db [:a :c])")
  (update-map [db path f]
    "Update a stored map based on a map of updates as returned by calling f, ensuring that the
     update is atomic and the value of the map is not changed while f is executing. Note f may
     be called multiple times."))
;; (atomically [f])


;; FIXME: crappy but exposes the api I want
(def get- get)
(defn get
  ([db path] (get- db path))
  ([db path not-found] (if-let [result (get- db path)] result not-found)))


(defmacro ^:private with-redis
  [config & body]
  `(car/with-conn ~(:pool config) ~(:spec config) ~@body))

(defn- call-me-maybe
  [f & args]
  (if (ifn? f) (apply f args) f))

(defn- empty-to-nil
  [s]
  (when-not (or (nil? s) (empty? s)) s))

(defmacro ^:private with-redis*
  [config path & body]
  `(let [~'key (call-me-maybe (:key ~config) ~path)
         ~'subkey (empty-to-nil (call-me-maybe (:subkey ~config) ~path))]
     (with-redis ~config ~@body)))

(defn- serialized-length
  [config key]
  (->> (car/debug-object key)
       (with-redis config)
       (re-seq #"serializedlength:([0-9]+)")
       first
       last
       Integer/parseInt))

(defn- redis-group-by-key
  [config prefix map]
  (group-by #(call-me-maybe (:key config) (conj prefix (first %)))
            (map-keys #(call-me-maybe (:subkey config) (conj prefix %)) map)))

(defn- redis-put-map
  [config path fgetmap watch]
  (with-redis config
    (car/atomically
     (if (nil? watch) [] [(call-me-maybe (:key config) watch)])
     (car/echo "hi") ; echo to distinguish failure from noop
     (let [gs (group-by (comp nil? val) (fgetmap))]
       (doseq [[k vs] (redis-group-by-key config path (gs false))]
         (apply car/hmset k (flatten vs)))
       (doseq [[k vs] (redis-group-by-key config path (gs true))]
         (apply car/hdel k (keys vs)))))))

(deftype Redis [config]
  DB
  (get [_ path]
    (with-redis* config path
      (if subkey
        (car/hget key subkey)
        (car/get key))))

  (put [_ path value]
    (with-redis* config path
      (if subkey
        (car/hset key subkey value)
        (car/set key value))))

  (delete [_ path]
    (with-redis* config path
      (if subkey
        (car/hdel key subkey)
        (car/del key))))

  (exists? [_ path]
    (with-redis* config path
      (car/parse-bool
       (if subkey
         (car/hexists key subkey)
         (car/exists key)))))

  (size [db path]
    (if-not (exists? db path)
      0
      (if (nil? (empty-to-nil ((:subkey config) path)))
        (serialized-length config ((:key config) path))
        (.length (get db path)))))

  (list [db path]
    (with-redis* config path
      (if subkey
        (car/with-parser (fn [r] (filter #(.startsWith % subkey) r)) (car/hkeys key))
        (car/keys (str (.replaceAll key "([*?\\[\\]\\\\])" "\\\\$1") "*")))))

  (put-map [db path map]
    (redis-put-map config path #(identity map) nil)
    nil)

  (update-map [db path f]
    (loop []
      (when-not (seq (redis-put-map config path f path))
        (recur)))
    nil))

(def ^:private redis-conn-pool (car/make-conn-pool))

(defmulti make-db :type)
(defmethod make-db :redis [config]
  (Redis. (into config {:pool redis-conn-pool
                        :spec (apply car/make-conn-spec (select-keys config #{:host :port :timeout :db}))})))

(defn handle
  [db]
  (make-db (-> (config/config) :databases db)))

(comment
  (def n2k (make-db (-> (config/config) :databases :name2key)))
  (def hd (make-db (-> (config/config) :databases :httpdb-data)))

  (def hdc (-> (config/config) :databases :httpdb-data))
  (redis-group-by-key hdc [:masa] {:a 1 :b 2})
  (redis-put-map hdc [:masa] #(identity {:a 1}) nil)
  (call-me-maybe (:key hdc) [:masa])

  ((-> (config/config) :databases :httpdb-data :subkey) [:a])

  (update-map hd [:test] #(identity {:a 1 :b 2 :c nil :d 4 :e nil}))

  (with-redis hdc
    (car/atomically ["hi"]
                    (car/get "hi")
                    (with-redis hdc (car/set "hi" "bye")) ))

  (get hd ["hi" "there"])
  (put hd ["hi" "there"] "yo wasup")
  (size hd ["hi" "there"])


  (get n2k ["haaai" "haaaai" "haaaai"])
  (put n2k ["hi hi"] 1)
  (get n2k ["hi hi"])
  (size n2k ["hi hi"])

  (get (Redis. {}) "hi")
  (config/reload-config)
  (remove-ns 'com.w-hat.ndb))
