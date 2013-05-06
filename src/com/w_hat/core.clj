(ns com.w-hat.core
  (:require [org.httpkit.server       :as httpkit]
            [compojure.response       :as response]
            [compojure.route          :as route]
            [compojure.handler        :as handler]
            [ring.middleware.reload   :as reload]
            [org.httpkit.client       :as http]
            [clojure.tools.logging    :as log]
            [ring.util.response       :refer [redirect]]
            [compojure.core           :refer [defroutes context GET PUT POST DELETE ANY]]
            [clj-logging-config.log4j :refer [set-logger!]]
            (ring.middleware [params :refer [wrap-params]] [keyword-params :refer [wrap-keyword-params]])
            (com.w-hat [sl :as sl] [name2key :as n2k] [httpdb :as httpdb] [re :as re]))
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
            user (httpdb/load-user uuid)
            r    (httpdb/check-password pass (:admin-password user))]
           uuid))

(defn is-linden?* [ip]
  (let [rev-addr (java.net.InetAddress/getByName ip)
        hostname (.getCanonicalHostName rev-addr)
        fwd-addr (java.net.InetAddress/getByName hostname)]
    (and (= rev-addr fwd-addr) (.endsWith hostname ".agni.lindenlab.com"))))

(def is-linden? (clojure.core.memoize/memo-ttl is-linden?* (* 60 60 24 7 1000)))

(def ^:private RE_SHARED_PATH (re-pattern (str "__/" , "(?<uuid>" sl/RE_UUID ")" , "/" , "(?<path>.*)" )))

(defn wrap-httpdb-auth
  [handler]
  (fn [request]
    (let [params           (:params request)
          headers          (:headers request)
          script-owner-key (:x-secondlife-owner-key headers)
          requestor-uuid   (cond (:login params) (httpdb-try-authenticate (:login params) (:password params))
                                 (and script-owner-key (re-matches sl/RE_UUID script-owner-key) (is-linden? (:remote-addr request))) script-owner-key)
          requestor        (if requestor-uuid (httpdb/load-user requestor-uuid))
          ;; translate /__/me/ to /__/<requestor-uuid>/
          path             (:path-info request)
          path             (if (.startsWith path "/__/me/") (.replace path "/__/me/" (str "/__/" requestor-uuid "/")) path)
          owner-uuid       (if-let [m (re/match-exact RE_SHARED_PATH path)] (:uuid m) requestor-uuid)
          owner            (if (= owner-uuid requestor-uuid) requestor (httpdb/load-user owner-uuid))
          path             (if (.startsWith path "/__/") (.replaceFirst path (str "/" owner-uuid "/") "/") path)]
      (if requestor-uuid
        (handler (into request {:requestor requestor
                                :owner     owner
                                :path-info path}))
        {:status 403}))))

(extend-protocol response/Renderable
  Long    (render [v _] {:status 200, :headers {"Content-Type" "text/plain"}, :body (str v)})
  Integer (render [v _] {:status 200, :headers {"Content-Type" "text/plain"}, :body (str v)}))

(defroutes httpdb-routes
  (GET  "/__sys/space/free"      {:keys [requestor]} (:space-free      requestor))
  (GET  "/__sys/space/used"      {:keys [requestor]} (:space-used      requestor))
  (GET  "/__sys/space/available" {:keys [requestor]} (:space-available requestor))
  (GET  "/__sys/admin_password"  [] 0) ;; TODO
  (POST "/__sys/defaults"        {:keys [params requestor]} ;; legacy
        (httpdb/set-user-meta requestor (:password params) {:default-read-password (:read-password params)                                                                                                    :default-write-password (:write-password params)}))
  (PUT  "/__sys/defaults"        {:keys [params requestor]} ;; legacy
        (httpdb/set-user-meta requestor (:password params) {:default-read-password (:read-password params)                                                                                                    :default-write-password (:write-password params)}))
  (POST "/__sys/options"         {:keys [params requestor]}     (httpdb/set-user-meta requestor (:password params) params))

  (GET     "/*" {:keys [params path-info requestor owner]}      (httpdb/get requestor owner path-info (:password params)))
  (PUT     "/*" {:keys [params path-info requestor owner body]} (httpdb/set requestor owner path-info (:password params)
                                                                            (:mode params) (slurp body) params))
  (POST    "/*" {:keys [params path-info requestor owner]}      (httpdb/set-meta requestor owner path-info (:password params) params))
  (DELETE  "/*" {:keys [params path-info requestor owner]}      (httpdb/delete   requestor owner path-info (:password params))))


(defn handler-key2name [uuid]
  (if-let [name (n2k/key2name uuid)]
    name
    {:status 404 :body ""}))

(defroutes key2name-routes
  (GET ["/:uuid" :uuid sl/RE_UUID] [uuid] (handler-key2name uuid)))

(defroutes routes
  (context "/name2key" [] name2key-routes)
  (context "/key2name" [] key2name-routes)
  (context "/httpdb"   [] (wrap-httpdb-auth httpdb-routes))
  (GET "/hi" [:as r] (str r))
  (route/not-found "Not Found"))

(defn wrap-log [handler]
  (fn [request]
    (log/info request)
    (handler request)))

(def app (-> routes
             wrap-log
             reload/wrap-reload
             wrap-keyword-params
             wrap-params))

(defn -main [& args]
  (n2k/make-key-resolve-worker)
  (httpkit/run-server app {:port 8080}))

(comment
  (n2k/name2key "masakazu kojima"))
