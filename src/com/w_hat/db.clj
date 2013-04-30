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
   Values in source map should be conversion function to apply.
  
   Ex: Given a hash 'hash' with values pre:a=100, pre:b=200
       (hgetmap \"hash\" \"pre\" {:a car/as-long :b identity})
       => {:a 100 :b \"200\"}"
  [hkey prefix m]
  (let [ks (keys m)
        fs (vals m)]
    (car/with-parser (fn [v] (apply hash-map (mapcat #(list %1 (if (nil? %3) nil (%2 %3))) ks fs v)))
                     (apply (partial car/hmget hkey) (map #(str prefix %) ks)))))

(comment (wcar (hgetmap "httpdb-users" com.w-hat.httpdb/masa {:space-used car/as-long :admin-password identity})))


