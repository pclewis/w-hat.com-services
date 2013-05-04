(ns com.w-hat.db
  (:require [taoensso.carmine :as car]
            [com.w-hat.config :refer [config]]))

(def pool (car/make-conn-pool))
(def spec (car/make-conn-spec :host (config :redis-host)))
(defmacro wcar [& body] `(car/with-conn pool spec ~@body))


(defn exists? [k] (car/parse-bool (car/exists k)))

(defmacro wmap [m]
  `(apply hash-map (car/with-conn pool spec ~@(mapcat #(list (list 'car/return (key %)) (val %)) m))))

(comment ;; goofy version that lets you use KEY to reference key in body
  `(apply hash-map (car/with-conn pool spec ~@(mapcat #(list (list 'car/return (key %)) `(let [~'KEY ~(key %)] ~(val %))) m)))
)

(defn hgetmap
  "Look up mutliple keys with a given prefix in a hash and return them as a map.
   If m is a map then value should be function to apply to convert value to desired type.

   Ex: Given a hash 'hash' with values pre:a=100, pre:b=200
       (hgetmap \"hash\" \"pre\" {:a car/as-long :b identity})
       => {:a 100 :b \"200\"}"
  [hkey prefix m]
  (let [ks (if (map? m) (keys m) m)
        fs (if (map? m) (vals m) (repeat identity))]
    (car/with-parser (fn [v] (apply hash-map (mapcat #(list %1 (when-not (nil? %3) (%2 %3))) ks fs v)))
                     (apply (partial car/hmget hkey) (map #(str prefix %) ks)))))

(defn hsetmap
  "Set multiple keys with a given prefix in a hash.
   Values mapped to nil will be deleted from the hash.
   Should generally be wrapped in car/atomically."
  [hkey prefix m]
  (let [ks (remove (comp nil? val) m)
        ds (filter (comp nil? val) m)]
    (if-not (empty? ks) (apply (partial car/hmset hkey) (mapcat #(list (str prefix (key %)) (val %)) ks)))
    (if-not (empty? ds) (apply (partial car/hdel hkey)  (map #(str prefix %) (keys ds))))))



(comment
  (wcar (hsetmap "testing123" "wee" {:a nil :b 4 :c 16}))
  (prn (wcar (car/hgetall "testing123"))))

(comment (wcar (hgetmap "httpdb-users" com.w-hat.httpdb/masa {:space-used car/as-long :admin-password identity})))
