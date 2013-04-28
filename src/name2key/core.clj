(ns name2key.core
  (:require [org.httpkit.server       :as httpkit]
            [compojure.route          :as route]
            [compojure.handler        :as handler]
            [ring.middleware.reload   :as reload]
            [org.httpkit.client       :as http]
            [compojure.core           :refer [defroutes GET]]
            [clj-logging-config.log4j :refer [set-logger!]]
            (com.w-hat [sl :as sl] [name2key :as n2k]))
  (:gen-class))

(set-logger!)

(defn handler-name2key
  [name terse]
  (let [k (if (sl/valid-name? name) (n2k/name2key name) :invalid)]
    (case k
      :invalid {:status 400 :body "Invalid name"}
      nil      {:status 404 :body (if terse sl/NULL_KEY "None found")}
      (if terse k (str name " " k)))))

(defn handler-add-keys
  [body]
  (str (n2k/add-keys (re-seq sl/RE_UUID body)) " new keys."))

(defroutes n2k
  (GET "/favicon.ico" [] {:status 404 :body sl/NULL_KEY}) ; avoid lookups for this
  (GET "/" [name terse keys]
       (cond keys (handler-add-keys keys)
             name (handler-name2key name terse)
             :else {:status 400 :body "You did it wrong."}))
  (GET "/:name" [name]
       (handler-name2key name true))
  (route/not-found "Not Found"))

(def app (reload/wrap-reload (handler/site n2k)))

(defn -main [& args]
  (n2k/make-key-resolve-worker)
  (httpkit/run-server app {:port 8080}))

