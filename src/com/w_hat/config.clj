(ns com.w-hat.config
  (:require clojure.edn))

(defn- exists? [^String f] (-> f java.io.File. .exists))
(defn- read-config [f] (if (exists? f) (clojure.edn/read-string (slurp f))))

(def ^:private current-config (atom nil))

(defn load-config
  []
  (into {} (map read-config ["config.clj" "/etc/w-hat.clj"])))

(defn reload-config
  []
  (let [c @current-config]
    (compare-and-set! current-config c (load-config))))

(defn config
  ([] (if (nil? @current-config) (reload-config)) @current-config)
  ([k] (k (config))))

