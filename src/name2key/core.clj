(ns name2key.core
  (:use compojure.core)
  (:use org.httpkit.server)
  (:require [compojure.route        :as route]
            [compojure.handler      :as handler]
            [taoensso.carmine       :as car]
            [ring.middleware.reload :as reload]
            [org.httpkit.client     :as http]))

(def pool (car/make-conn-pool))
(def spec (car/make-conn-spec :host "younha"))
(defmacro wcar [& body] `(car/with-conn pool spec ~@body))

(def RE_UUID          #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
(def RE_USER_NAME     #"\w+(?:\.\w+)?")
(def RE_RESIDENT_NAME #"\w+ \w+")
(def RE_TITLE         (re-pattern (str "(?:" "[^<>]+ \\((?<name0>" RE_USER_NAME ")\\)" ; Display Name (username)
                                       "|"   "(?<name1>" RE_RESIDENT_NAME ")"          ; Resident Name
                                       "|"   "(?<name2>" RE_USER_NAME ")"              ; Username
                                       ")"   )))

(def NULL_KEY "00000000-0000-0000-0000-000000000000")



(defn call-method
  "Calls a private or protected method.

   params is a vector of classes which correspond to the arguments to
   the method e

   obj is nil for static methods, the instance object otherwise.

   The method-name is given a symbol or a keyword (something Named)."
  [klass method-name params obj & args]
  (-> klass (.getDeclaredMethod (name method-name)
                                (into-array Class params))
      (doto (.setAccessible true))
      (.invoke obj (into-array Object args))))

(defn get-named-groups
  [re]
  (call-method java.util.regex.Pattern "namedGroups" [] re))

(defn match-map
  "Return named groups from matcher as a map."
  [ng matcher]
  (zipmap (map keyword(keys ng))
          (map (fn [v] (some identity (map #(.group matcher %) v)))
               (vals ng))))

(defn matches
  "Match s against re and return named groups as map. Names ending with numbers will be merged."
  [re s]
  (let [ng (group-by #(re-find #"[a-zA-Z]+" %) (map key (get-named-groups re)))
        matcher (re-matcher re s)]
    (if (.matches matcher) (match-map ng matcher))))

(defn all-matches
  "Match s against re and return named groups as map. Names ending with numbers will be merged."
  [re s]
  (let [ng (group-by #(re-find #"[a-zA-Z]+" %) (map key (get-named-groups re)))
        matcher (re-matcher re s)]
    (take-while identity (repeatedly #(if (.find matcher) (match-map ng matcher))))))


(defn canonical-name
  "Return the canonical version of a user/resident name."
  [name]
  (-> name
      (.toLowerCase)
      (.replace "+" ".")
      (.replace " " ".")
      (.replace ".resident" "")))

(defn valid-name?
  "Return true if name is a valid (not necessarily canonical) resident/user name."
  [name]
  (.matches (re-matcher #"\w+ \w+|\w+(\.\w+)?" name)))

(defn name2key-sl-search
  "Look up a key on SL search"
  [name]
  (let [{:keys [status headers body error] :as resp}
          @(http/get "http://search.secondlife.com/client_search.php"
            {:query-params {:session NULL_KEY
                            :s "People"
                            :mat 7 :m "y"
                            :q name}
             :timeout 2000})
        re (re-pattern (str "<a href=\"http://world.secondlife.com/resident/(?<uuid>" RE_UUID ")\"\\s*>" RE_TITLE "</a>"))
        cn (canonical-name name)]
    (if-not error
      (some #(if (= cn (canonical-name(:name %))) (:uuid %)) ; TODO: queue up all the other keys we found
            (all-matches re body)))))

(defn name2key-redis
  "Return the key of a resident."
  [name]
  (wcar (car/get (canonical-name name))))

(defn store-key
  [name key]
  (wcar (car/set (canonical-name name) key)))

(defn name2key-any
  "Return the key of a resident by any means."
  [name]
  (let [k (name2key-redis name)]
    (if k k
      (let [nk (name2key-sl-search name)]
        (if nk (do (store-key name nk) nk))))))

(defn handler-name2key
  [name terse]
  (if (valid-name? name)
    (let [key (name2key-any name)]
      (if key
        (if terse key (str name " " key))
        {:status 404 :body (if terse NULL_KEY "None found")}))
    {:status 400 :body "Invalid name"}))

(defn add-keys [keys]
  (str "Adding keys " keys))

(defroutes n2k
  (GET "/" [name terse keys]
       (cond keys (add-keys keys)
             name (handler-name2key name terse)
             true {:status 400 :body "You did it wrong."}))
  (GET "/:name" [name]
       (handler-name2key name true))
  (route/not-found "Not Found"))

(def app (reload/wrap-reload (handler/site n2k)))

(defn -main [& args]
  (run-server app {:port 8080}))

