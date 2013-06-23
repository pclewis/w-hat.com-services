(ns com.w-hat.httpdb
  (:refer-clojure :exclude [get set list read])
  (:require (com.w-hat [ndb :as db] [sl :as sl] [re :as re] [util :refer :all])
            [taoensso.carmine :as car]
            [plumbing.core :refer [fnk]]
            [plumbing.graph :as graph]
            [slingshot.slingshot :refer [throw+]]
            [clojure-csv.core :as csv]))

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
                    (merge-fnk (fnk [readable?] (when-not readable? (throw+ {:type ::denied}))) f)
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

(defn update-record
  [record updates]
  (db/put-map
   data
   (:dbpath record)
   (let [old-len (db/size data (conj (:dbpath record) :data))
         defaults (map-apply :default record-fields record)
         events  (keep identity [:on-record-update (if-not (:exists? record) :on-record-create)])
         updates (apply dissoc updates (keys (filter-subkeys #{:calculate} record-fields))) ; remove unsettable fields
         ; can't detect fields that match existing without extra reads from db. also might not have permission
         ; updates (apply dissoc updates (filter #(= (% updates) (% record)) (keys updates))) ; remove fields that match current record
         updates (apply merge updates (map #(map-apply % record-fields record) events))     ; add calculated fields
         updates (map-map #(when-not (= (%1 defaults) %2) %2) updates)                      ; nil-ify fields that match defaults
         updates (map-map #(if %2 (write (:type (%1 record-fields)) %2)) updates)           ; apply types
         new-data (:data updates)
         new-len (if (contains? updates :data)
                   (if (nil? new-data)
                     0
                     (if (vector? new-data)
                       (+ old-len (-> new-data second count))
                       (count new-data)))
                   old-len)]
     (when (and (> new-len old-len)
                (> 0 (- (:space-free (:owner record))
                        (- new-len old-len))))
       (throw+ {:type ::quota-exceeded}))
     (when-not (:writable? record)
       (throw+ {:type ::denied}))
     updates)))

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
              (apply dissoc (map-vals (fn [r] nil) record-fields)
                     (keys (filter-subkeys #{:calculate} record-fields)))))

;; goofy interface..
(defn list
  [record]
  (distinct (map #(.replaceFirst % ":[^:]+$" "") (db/list data (:dbpath record)))))

(defn parse-mysql-null [^String s] (when-not (= s "\\N") s))

(defn import-csv
  [filename]
  (let [csv (csv/parse-csv (clojure.java.io/reader filename))
        parse-mysql-date (fn [^String s] (iso8601-parse (str (.replace s " " "T") "Z")))]
    (doseq [row csv]
      (try
        (db/put-map data [(nth row 0) (.replaceFirst (nth row 1) (str "/" (nth row 0) "/") "/")]
                    {:created-at (write :date (parse-mysql-date (nth row 2)))
                     :created-by (nth row 3)
                     :updated-at (write :date (parse-mysql-date (nth row 4)))
                     :updated-by (nth row 5)
                     :read-password (parse-mysql-null (nth row 6))
                     :write-password (parse-mysql-null (nth row 7))
                     :data (nth row 8)})
        (catch Exception e
          (prn "Exception " e " on row " row)
          (throw e))))))

(defn import-user-csv
  [filename]
  (let [csv (csv/parse-csv (slurp filename))]
    (doseq [row csv]
      (let [row (map parse-mysql-null row)]
        (when-not (every? nil? (rest row))
          (db/put-map users [(nth row 0)]
                      {:admin-password (nth row 1)
                       :default-read-password (nth row 2)
                       :default-write-password (nth row 3)}))))))
