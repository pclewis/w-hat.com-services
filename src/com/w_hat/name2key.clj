(ns com.w-hat.name2key
  (:require (com.w-hat [ndb :as db] [re :as re] [sl :as sl])
            [clojure.tools.logging          :as log]
            [clojure.core.memoize           :refer [memo-ttl]]
            [org.httpkit.client             :as http]))

(def n2k (db/handle :name2key))
(def k2n (db/handle :key2name))

(def ^:private RE_SL_RESIDENT_RESULT (re-pattern (str "<title>" sl/RE_TITLE "</title>")))
(defn sl-search-key
  "Look up a key on SL Search and return a name."
  [key]
  (log/info "Looking up key" key "on SL search")
  (let [{:keys [status headers body error] :as resp} @(http/get (str "http://world.secondlife.com/resident/" key) {:timeout 2000})]
    (if error
      (log/error "Error looking up" key ":" error)
      (if-let [rm (re/match-anywhere RE_SL_RESIDENT_RESULT body)]
        (sl/canonical-name (:name rm))))))

(def ^:private RE_SL_SEARCH_RESULT (re-pattern (str "<a href=\"http://world.secondlife.com/resident/(?<uuid>" sl/RE_UUID ")\"\\s*>" sl/RE_TITLE "</a>")))
(defn sl-search-name
  "Look up a name on SL Search and return a map of all name->key pairs returned. May not include requested name."
  [name]
  (log/info "Looking up name" name "on SL search")
  (let [{:keys [status headers body error] :as resp}
          @(http/get "http://search.secondlife.com/client_search.php"
            {:query-params {:session sl/NULL_KEY ; must pass a session even if it's invalid
                            :s "People"          ; only include people
                            :mat 7 :m "y"        ; include mature results
                            :q name}
             :timeout 2000})]
    (if error
      (log/error "Error looking up" name ":" error)
      (into {} (map #(hash-map (-> % :name sl/canonical-name) (:uuid %))
                    (re/match-all RE_SL_SEARCH_RESULT body))))))

(def in-world-key2name-url (atom nil))
(defn- refresh-in-world-key2name-url
  []
  (log/info "Getting new in-world key2name url")
  (let [oldval @in-world-key2name-url
        {:keys [status headers body error] :as resp} @(http/get "http://w-hat.com/uuid-url.txt")]
    (if error
      (log/error "Error fetching new key2name url:" error)
      (let [newval (clojure.string/trim body)]
        (if (not= oldval newval)
          (compare-and-set! in-world-key2name-url oldval newval))))))

(defn in-world-key2name
  [key]
  (when (nil? @in-world-key2name-url) (refresh-in-world-key2name-url))
  (let [{:keys [status headers body error] :as resp} @(http/post @in-world-key2name-url {:form-params {:value key}})
        update-and-retry (fn [] (if (refresh-in-world-key2name-url) (in-world-key2name key)))]
    (cond error          (do (println "Error calling in-world key2name:" error) (update-and-retry))
          (= status 404) (update-and-retry)
          (= status 200) (sl/canonical-name (nth (clojure.string/split body #"@") 1)))))

(defn- name-hash [name] (subs name 0 (min 4 (count name))))
(defn- key-hash  [key]  (subs key  0 3))

(defn- save
  [name key]
  (db/put n2k [name] key)
  (db/put k2n [key] name))

(defn name2key-redis
  [name]
  (let [cn (sl/canonical-name name)]
    (db/get n2k [cn])))

(defn name2key-sls*
  [name]
  (let [results (sl-search-name name)
        cname (sl/canonical-name name)]
    (when results
      (doseq [[name key] results] (save name key)) ; save found keys
      (results cname))))

(def name2key-sls (memo-ttl name2key-sls* 60000))

(defn key2name-redis
  [key]
  (db/get k2n [key]))

(defn key2name-remote*
  [key]
  (let [name (or (sl-search-key key) (in-world-key2name key))]
    (when name
      (save name key)
      name)))

(def key2name-remote (memo-ttl key2name-remote* 60000))

(defn name2key
  [name]
  (or (name2key-redis name) (name2key-sls name)))

(defn key2name
  [key]
  (or (key2name-redis key) (key2name-remote key)))

(defn add-keys
  [ks]
  (->> (remove #(db/exists? k2n [%]) ks)
       (map #(db/enqueue n2k "key-resolve" %))
       doall
       count))

(defn make-key-resolve-worker []
  (db/make-dequeue-worker n2k "key-resolve" key2name))

(defn csv
  []
  (clojure.string/join "\n" (map (fn [[k v]] (str k "," (sl/resident-name v)))
                                 (db/list k2n []))))

(comment
  (csv)
  (db/enqueue n2k "asdf" "asdf")
  (add-keys ["0050d800-f30d-441e-983f-d564c4f99b30"])
  (def q (db/make-dequeue-worker n2k "key-resolve" key2name))
  (taoensso.carmine.message-queue/stop q))
