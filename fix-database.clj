(require '[taoensso.carmine :as car]
         '[com.w-hat.sl :as sl]
         '[com.w-hat.re :as re])

(def pool (car/make-conn-pool))
(def spec0 (car/make-conn-spec :db 0))
(def spec1 (car/make-conn-spec :db 1))

(defmacro wcar0 [& body] `(car/with-conn pool spec0 ~@body))
(defmacro wcar1 [& body] `(car/with-conn pool spec1 ~@body))

(prn "Fixing single letter hashes..")

(doseq [dbkey (wcar0 (car/keys "?"))]
  (let [m (wcar0 (car/hgetall* dbkey))
        by-key (group-by #(subs (key %) 0 (min 4 (count (key %)))) m)]
    (prn dbkey "..." (count by-key) " groups")
    (doseq [[dk vs] by-key]
      (assert (< 1 (apply min (map count (map key vs)))))
      (wcar0 (apply car/hmset dk (flatten vs)))))
  (wcar0 (car/del dbkey)))

(prn "Fixing UUIDs in wrong db")

(doseq [dbkey (wcar0 (car/keys "???"))]
  (prn dbkey "...")
  (let [entries (wcar0 (car/hgetall* dbkey))
        to-move (filter #(->> % key (re/match-exact sl/RE_UUID) boolean) entries)]
    (when-not (empty? to-move)
      (wcar1 (apply car/hmset dbkey (flatten to-move)))
      (wcar0 (apply car/hdel dbkey (map key to-move))))))
