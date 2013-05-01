(ns com.w-hat.core
  (:require [org.httpkit.server       :as httpkit]
            [compojure.route          :as route]
            [compojure.handler        :as handler]
            [ring.middleware.reload   :as reload]
            [org.httpkit.client       :as http]
            [clojure.tools.logging    :as log]
            [compojure.core           :refer [defroutes GET ANY context]]
            [clj-logging-config.log4j :refer [set-logger!]]
            (com.w-hat [sl :as sl] [name2key :as n2k] [httpdb :as httpdb]))
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

(defroutes name2key-routes
  (GET "/favicon.ico" [] {:status 404 :body sl/NULL_KEY}) ; avoid lookups for this
  (GET "/" [name terse keys]
       (cond keys (handler-add-keys keys)
             name (handler-name2key name terse)
             :else {:status 400 :body "You did it wrong."}))
  (GET "/:name" [name]
       (handler-name2key name true))
  (route/not-found "Not Found"))

(defmacro and-let [bindings expr]
  (if (seq bindings)
    `(if-let [~(first bindings) ~(second bindings)]
       (and-let ~(drop 2 bindings) ~expr))
     expr))

(defn httpdb-try-authenticate
  [login pass]
  (log/info "Trying to authenticate " login)
  (and-let [uuid (if (re-matches sl/RE_UUID login)
                   login
                   (n2k/name2key login))
            user (httpdb/user uuid)
            r    (httpdb/check-password pass (:admin-password user))]
           uuid))

(defn is-linden? [ip] true)

(defn handler-httpdb
  [path req]
  (let [params  (:params req)
        headers (:headers req)
        uuid    (cond (:login params)
                      ,, (httpdb-try-authenticate (:login params) (:password params))

                      (and (:x-secondlife-owner-key headers)
                           (is-linden? (:remote-addr req)))
                      ,, (:x-secondlife-owner-key headers)

                      :else
                      ,, nil)
        requestor (when uuid (httpdb/user uuid))]
    (log/info requestor path)
    (let [resp (if (nil? requestor)
                 :denied
                 (case (:request-method req)
                   :get (httpdb/get requestor path (:password params))
                   :set (httpdb/set requestor path (:password params) (:mode params) (:data req))
                   :invalid))]
      (case resp
        :denied {:status 403}
        :invalid {:status 400}
        nil {:status 404}
        (str resp)))))

(defroutes httpdb-routes
  (ANY ["/:path" :path #".*"] [path :as r] (handler-httpdb path r)))


(defn handler-key2name [uuid]
  (if-let [name (n2k/key2name uuid)]
    name
    {:status 404 :body ""}))

(defroutes key2name-routes
  (GET ["/:uuid" :uuid sl/RE_UUID] [uuid] (handler-key2name uuid)))

(defroutes routes
  (context "/name2key" [] name2key-routes)
  (context "/key2name" [] key2name-routes)
  (context "/httpdb" [] httpdb-routes)
  (route/not-found "Not Found"))

(def app (reload/wrap-reload (handler/api routes)))

(defn -main [& args]
  (n2k/make-key-resolve-worker)
  (httpkit/run-server app {:port 8080}))