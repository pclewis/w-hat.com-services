(ns com.w-hat.ndb
  "Configurable database and database mapping."
  (:refer-clojure :exclude [get])
  (:require (com.w-hat [config :as config] [util :refer :all])
            [taoensso.carmine :as car]
            [taoensso.carmine.message-queue :as mq]
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
  (put-map [db path update-map]
     "Update multiple keys based on entries in map. Each key will be appended to path. Entries
     with a nil value will be deleted. Entries map also be a vector to peform operations on the
     existing value without fetching it.
     Ex:
         (put-map db [:a] {:b 1 :c nil :d [:append \"hello\"]})
     is equivalent to:
         (put db [:a :b] 1)
         (delete db [:a :c])
         (put db [:a :d] (str (get db [:a :d]) \"hello\"))
     except the put-map version is atomic and does not load d from database."))

;; Hacked together to be able to use carmine's Redis message queue.
(defprotocol MQ
  (enqueue [mq qname value])
  (make-dequeue-worker [mq qname handlerfn]))

(defmacro ^:private with-redis
  [config & body]
  `(car/with-conn (:pool ~config) (:spec ~config) ~@body))

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

  (put-map [db path update-map]
    (with-redis config
      (doseq [[key submap] (redis-group-by-key config path update-map)]
        (let [updates (map-vals #(cond (nil? %)    [:del]
                                       (vector? %) %
                                       :else       [:set %]) (into {} submap))
              update-groups (group-by (comp first val) updates) ; {:set [[:a [:set 1]]}
              args (flatten (map (comp (juxt count #(map-vals second %))
                                       #(or (% update-groups) []))
                                 [:set :del :append :prepend]))]
          (apply car/eval*
                 "local start = 1; local e = start+tonumber(ARGV[start])*2
                  for i = start+1,e,2 do  redis.call(\"HSET\", KEYS[1], ARGV[i], ARGV[i+1])  end
                  start = e+1; e = start+tonumber(ARGV[start])
                  for i = start+1,e do  redis.call(\"HDEL\", KEYS[1], ARGV[i])  end
                  start = e+1; e = start+tonumber(ARGV[start])*2
                  for i = start+1,e,2 do
                    redis.call(\"HSET\", KEYS[1], ARGV[i], (redis.call(\"HGET\", KEYS[1], ARGV[i]) or \"\") .. ARGV[i+1])
                  end
                  start = e+1; e = start+tonumber(ARGV[start])*2
                  for i = start+1,e,2 do
                    redis.call(\"HSET\", KEYS[1], ARGV[i], ARGV[i+1] .. (redis.call(\"HGET\", KEYS[1], ARGV[i]) or \"\"))
                  end"
                 1 key (keep identity args)))))
    nil)

  MQ
  (enqueue [mq qname value]
    (with-redis config
      (mq/enqueue qname value)))

  (make-dequeue-worker [mq qname handlerfn]
    (mq/make-dequeue-worker (:pool config) (:spec config) qname :handler-fn handlerfn)))

(def ^:private redis-conn-pool (car/make-conn-pool))

(defmulti make-db :type)
(defmethod make-db :redis [config]
  (Redis. (into config {:pool redis-conn-pool
                        :spec (apply car/make-conn-spec (-> (select-keys config #{:host :port :timeout :db}) seq flatten))})))

(defn handle
  [db]
  (make-db (-> (config/config) :databases db)))

(comment
  (def n2k (make-db (-> (config/config) :databases :name2key)))
  (enqueue n2k "test" "test")
  (def hd (make-db (-> (config/config) :databases :httpdb-data)))

  (def hdc (-> (config/config) :databases :httpdb-data))
  hdc
  (doc car/make-conn-spec)
  (redis-group-by-key hdc [:masa] {:a 1 :b 2})
  (put-map hd [:masa] {:a 1 :b [:append "wat"] :c nil})
  (call-me-maybe (:key hdc) [:masa])

  (apply car/make-conn-spec (-> (select-keys hdc #{:host :port :timeout :db}) seq flatten))

  (redis-group-by-key hdc ["test"] {:a 1})

  (map-keys #(identity [:set %]) [ [:a 1]])

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
