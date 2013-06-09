(ns com.w-hat.httpdb
  (:refer-clojure :exclude [get set list read])
  (:require (com.w-hat [ndb :as db] [sl :as sl] [re :as re] [util :refer :all])
            [taoensso.carmine :as car]
            [plumbing.core :refer [fnk]]
            [plumbing.graph :as graph]
            [slingshot.slingshot :refer [throw+]]))

(def users (db/handle :httpdb-users))
(def data  (db/handle :httpdb-data))

(def user-fields
  {:space-allocated        {:type :int :default 250000}
   :space-used             {:calculate (fnk [id] (db/size data [id]))}
   :space-free             {:calculate (fnk [space-allocated space-used] (- space-allocated space-used))}
   :track-create           {:type :bool}
   :track-update           {:type :bool}
   :default-read-password  {:type :password}
   :default-write-password {:type :password}
   :admin-password         {:type :password}})

(def record-fields
  {:data                   {}

   :owner                  {:calculate (fnk [request] (:owner request))}

   :path                   {:calculate (fnk [request] (:path request))}

   :dbpath                 {:calculate (fnk [owner path] [(:id owner) path])}

   :created-by             {:default (fnk [owner] (if (:track-create owner) (:id owner)))

                            :on-record-create (fnk [owner [:request requestor]]
                                                   (if (:track-create owner) (:id requestor)))}

   :created-at             {:type :date

                            :on-record-create (fnk [owner] (if (:track-create owner) (java.util.Date.)))}

   :updated-by             {:default (fnk [owner] (if (:track-update owner) (:id owner)))

                            :on-record-update (fnk [owner [:request requestor]]
                                                   (if (:track-update owner) (:id requestor)))}

   :updated-at             {:type :date

                            :on-record-update (fnk [owner] (if (:track-update owner) (java.util.Date.)))}

   :read-password          {:type :password
                            :default (fnk [owner] (:default-read-password owner))}

   :write-password         {:type :password
                            :default (fnk [owner] (:default-create-password owner))}

   :exists?                {:calculate (fnk [owner dbpath] (db/exists? data (conj dbpath :data)))}

   :readable?              {:calculate (fnk [read-password owner [:request requestor password]]
                                            (or (nil? read-password)
                                                (check-password password read-password (:admin-password owner))))}

   :writable?              {:calculate (fnk [exists? write-password owner [:request requestor password]]
                                            (or (and exists? (nil? write-password))
                                                (and (not exists?) (= (:id owner) (:id requestor)))
                                                (check-password password write-password (:admin-password owner))))}})

(defmulti read (fn [type _] type))
(defmethod read :default [_ v] v)
(defmethod read :int  [_ v] (Integer/parseInt v))
(defmethod read :bool [_ v] (= v "1"))
(defmethod read :date [_ v] (iso8601-parse v))

(defmulti write (fn [type _] type))
(defmethod write :default [_ v] v)
(defmethod write :bool [_ v] (case v (true "true" "t" "1" "yes" "y") 1 nil))
(defmethod write :password [_ v] (hash-password v))
(defmethod write :date [_ v] (iso8601-format v))

(defn- get-field
  [db path f default]
  (if-let [result (db/get db path)]
    (f result)
    default))

;; FIXME: icky
(defn- make-graph [fields make-calculate-fn]
  (graph/lazy-compile
   (map-map (fn [k {:keys [type default calculate]}]
              (if calculate
                calculate
                (make-calculate-fn k type default)))
            fields)))

(def ^:private user-graph
  (make-graph user-fields
              (fn [k t d] (fnk [id]
                            (get-field users [id k] #(read t %) d)))))

(def ^:private record-graph
  (make-graph record-fields
              (fn [k t d]
                (let [f (fnk [request owner path dbpath :as rec]
                             (get-field data
                                        (conj dbpath k)
                                        #(read t %)
                                        (when d (d rec))))]
                  (if (= k :data)
                    (merge-fnk (fnk [readable?] (when (not readable?) (throw+ {:type ::denied}))) f)
                    f)))))

;; see also: https://github.com/Prismatic/plumbing/issues/4
(defn- merge-inputs [g m] (into (g m) m))

(defn load-user
  [id]
  (merge-inputs user-graph {:id id}))

(defn load-record
  ([requestor owner path password]
     (merge-inputs record-graph {:request {:requestor requestor :owner owner :path path :password password}}))
  ([record]
     (merge-inputs record-graph (select-keys record [:request]))))

(comment
  (:readable? (record-graph {:request {:path "test" :owner (load-user "masa") :requestor "you" :password "hi"}}))
  (map->Request {:owner (load-user "masa") :requestor (load-user "masa") :path "hi" :password "Hi"})

  (def masa (load-user "masa"))

  (record-defaults (load-record masa (load-user "butt") "hi/there" "wat"))

  (:data (load-record masa masa "hi/there" "hii"))

  (update-record masa masa "hi/there" "yoo" #(identity {:read-password "hi" :data "wat" :blah %}))

  (update-record (load-record masa masa "hi/there" "yoo") {:data "hello world"})

  (update-user (load-user "masa") {:track-update true})

  (write (:type (:track-update user-fields)) false)

  (conj [1] nil)
  (filter [1 nil])

  )

(defn update-record
  [record update-fn]
  (db/update-map
   data (:dbpath record)
   (fn []
     (let [record  (load-record record) ; reload record from db
           old-len (db/size data (conj (:dbpath record) :data))
           defaults (map-apply :default record-fields record)
           events  (keep identity [:on-record-update (if-not (:exists? record) :on-record-create)])
           updates (update-fn record)
           updates (apply dissoc updates (keys (filter-subkeys #{:calculate} record-fields))) ; remove unsettable fields
           updates (apply dissoc updates (filter #(= (% updates) (% record)) (keys updates))) ; remove fields that match current record
           updates (apply merge updates (map #(map-apply % record-fields record) events))     ; add calculated fields
           updates (map-map #(if (= (%1 defaults) %2) nil %2) updates)                        ; nil-ify fields that match defaults
           updates (map-map #(if %2 (write (:type (%1 record-fields)) %2)) updates)           ; apply types
           new-data (:data updates)
           new-len (if (contains? updates :data)
                     (if (nil? new-data)
                       0
                       (if (map? new-data)
                         (+ old-len (-> new-data vals first count))
                         (count new-data)))
                     old-len)]
       (when (and (> new-len old-len)
                  (> 0 (- (:space-free (:owner record))
                          (- new-len old-len))))
         (throw+ {:type ::quota-exceeded}))
       (when-not (:writable? record)
         (throw+ {:type ::denied}))
       updates))))

(defn update-user
  [user updates]
  (let [updates (apply dissoc updates (keys (filter-subkeys #{:calculate} record-fields)))
        updates (map-map #(if %2 (write (:type (%1 user-fields)) %2)) updates)]
    (db/put-map users [(:id user)] updates)))


(defn delete-record
  [record]
  (when-not (:writable? record)
    (throw+ {:type ::denied}))
  (db/put-map data (:dbpath record)
              (map-vals (fn [m] nil) (filter-subkeys (complement #{:calculate}) record-fields))))

;; goofy interface..
(defn list
  [record]
  (distinct (map #(.replaceFirst % ":[^:]+$" "") (db/list data (:dbpath record)))))



(comment

  (filter #(.startsWith % "/hiaa") (db/list data ["masa" "/"]))

  (update-record (load-user "masa") (load-user "masa") "test" "test" (fn [record] {:data "hii"}))
  (delete-record (load-user "masa") (load-user "masa") "test" "hii")
  (empty? "asdf")
  (update-user (load-user "masa") {:space-allocated 5})
  (def gov (load-user (com.w-hat.name2key/name2key "governor linden")))
  (update-user (load-user (com.w-hat.name2key/name2key "governor linden")) {:admin-password "test"})

  (update-record (load-record gov gov "/bbbi" "") (fn [record] {:data "hello world" :read-password "readme"}))

  (:id  (into (user "masa") {:id "masa"}))
  (record (user "masa") "hello/world")

 (defn get-new-meta
   [fs m ks & args]
   (let [nm (apply merge (set-fields fs m) (map #(apply calculate-fields fs % args) (if (coll? ks) ks [ks])))]
     (merge-with #(if-not (= %1 %2) %2) ;; nil-ify values that match default
                 (mmap #(apply get-default % args) (select-keys fs (keys nm)))
                 nm)))

 (defn load-user
   [id]
   {:post [(user? %)]}
   (let [user (db/wcar (db/hgetmap KEY_USERS id (mmap :get (filter-subkeys #{:get} user-fields))))]
     (merge (mmap get-default user-fields)
            (filter val user)
            (mmap #((:calculate %) id user) (filter-subkeys #{:calculate} user-fields)))))




 (defn- get-record [owner path]
   (db/wcar (car/hget (keyname :data (:id owner)) path)))

 (defn- update-user [user metafn]
   (db/atomically
    [(keyname :meta (:id user)) (keyname :data (:id user))]
    (db/hsetmap KEY_USERS (:id user) (metafn))))

 (defn- set-record [owner path mode data meta]
   (let [dk (keyname :data (:id owner))]
     (db/atomically
      (if (#{:append :prepend} mode) [dk] []) ;; watch dk if we're basing new value off old value
      (let [nd (case mode :append  (str (get-record owner path) data)
                     :prepend (str data (get-record owner path))
                     data)]
        (car/hset dk path nd))
      (db/hsetmap (keyname :meta (:id owner)) path meta))))


 (defn get
   "Look up record owned by owner and return it if user has read access to it with the supplied password."
   [requestor owner path password]
   (let [record        (get-record owner path)
         read-password (get-record-meta owner path :read-password)]
     (if (or (nil? read-password)
             (check-password password read-password (:admin-password owner)))
       record
       :denied)))

 (defn set
   [requestor owner path password mode data meta]
   (let [exists?        (record-exists? owner path)
         write-password (get-record-meta owner path :write-password)
         events         (cons :on-record-update (if-not exists? [:on-record-create]))]
     (if (or (and exists? (nil? write-password))                               ;; exists but no password
             (and (not exists?) (= requestor owner))                           ;; doesn't exist and we're owner
             (check-password password write-password (:admin-password owner))) ;; we have a valid password
       (set-record owner path mode data (get-new-meta record-fields meta events requestor owner path))
       :denied)))

 (defn delete
   [requestor owner path password]
   (when (record-exists? owner path)
     (let [write-password (get-record-meta owner path :write-password)]
       (if [(or (nil? write-password)
                (check-password password write-password (:admin-password owner)))]
         (db/wcar (car/hdel (keyname :data (:id owner)) path))
         :denied))))

 (defn set-meta
   [requestor owner path password meta]
   (when (record-exists? owner path)
     (if-not (check-password password (:admin-password owner))
       :denied
       (do (db/atomically
            [] (db/hsetmap (keyname :meta (:id owner)) path
                           (get-new-meta record-fields meta :on-record-update requestor owner path)))))))

 (defn set-user-meta
   [owner password meta]
   (if-not (check-password password (:admin-password owner))
     :denied
     (update-user owner #(get-new-meta user-fields meta []))))

 (defn list
   [owner path]
   (filter #(.startsWith % path) (db/wcar (car/hkeys (keyname :data (:id owner))))))

 (comment
   (get-new-meta user-fields {} [:on-record-delete :on-record-update] masau masau "wat")
   (set masau masau "_test_" nil nil "test" {:read-password "hi"})
   (get masau masau "_test_" "hi")
   (delete masau masau "_test_" nil)
   (set-meta masau masau "_test_" "tester" {:read-password "hi"})
   (set-user-meta masau "tester" {:track-create "1"})
   (get-record-meta masau "_test")
   (list masau "")))
