(ns com.w-hat.util
  (:require [plumbing.fnk.pfnk   :as pfnk]
            [plumbing.fnk.schema :as schema]
            [plumbing.core :refer [fnk]])
  (:import org.apache.commons.codec.digest.Crypt))

(defn filter-subkeys
  "Return only entries in m where the value contains a key for which (pred key) returns true"
  [pred m]
  (into (empty m) (filter #(some pred (-> % val keys)) m)))

;; TODO: compare to plumbing.core/map-vals, consider plumbing.core/for-map
(defn map-vals
  "Apply function to each value of map. {k (f v)}"
  [f m]
  (into (empty m) (for [[k v] m] [k (f v)])))

;; would be nice if map-vals could just detect arity
(defn map-map
  "Apply function to each entry in map. {k (f k v)}"
  [f m]
  (into (empty m) (for [[k v] m] [k (f k v)])))

(defn map-apply
  "Apply nested key to args. {k (apply (fk v) args)}"
  [fk m & args]
  (map-vals #(apply (fk %) args) (filter-subkeys #{fk} m)))


(defn hash-password
  "Hash password like crypt(3)"
  ([^String pass]              (Crypt/crypt pass))
  ([^String pass ^String salt] (Crypt/crypt pass salt)))

(defn check-password
  "Check if password matches any of hashed-pws"
  [password & hashed-pws]
  (if password
    (some #(= % (hash-password password %)) (remove nil? hashed-pws))))

(def ^java.text.SimpleDateFormat ^:private iso8601 (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'"))
(.setTimeZone iso8601 (java.util.TimeZone/getTimeZone "UTC"))

(defn iso8601-format
  "Format a date using ISO8601 format. Always UTC."
  [date]
  (.format iso8601 date))

(defn iso8601-parse
  "Parse a string in ISO8601 format."
  [s]
  (.parse iso8601 s))


(defn merge-fnk
  "Create a fnk with the unioned input requirements of multiple fnks.
   The return value of all but the last fnk will be discarded."
  ([f g]
     (let [input-schema (schema/union-input-schemata (pfnk/input-schema f) (pfnk/input-schema g))
           h (fn [m] (f m) (g m))]
       (vary-meta h assoc :plumbing.fnk.pfnk/io-schemata [input-schema true])))
  ([f g & more]
     (reduce merge-fnk (merge-fnk f g) more)))
