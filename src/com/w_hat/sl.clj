(ns com.w-hat.sl
  "Second Life stuff.")


(def NULL_KEY "00000000-0000-0000-0000-000000000000")

(def RE_UUID          #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
(def RE_USER_NAME     #"\w+(?:\.\w+)?")
(def RE_RESIDENT_NAME #"\w+ \w+")
(def RE_VALID_NAME    (re-pattern (str "(?:" RE_RESIDENT_NAME "|" RE_USER_NAME ")")))
(def RE_TITLE         (re-pattern (str "(?:" "[^<>]+ \\((?<name0>" RE_USER_NAME ")\\)" ; Display Name (username)
                                       "|"   "(?<name1>" RE_RESIDENT_NAME ")"          ; Resident Name
                                       "|"   "(?<name2>" RE_USER_NAME ")"              ; Username
                                       ")"   )))

(defn canonical-name
  "Return the canonical version of a user/resident name."
  [^String name]
  (-> name
      (.toLowerCase)
      (.replace "+" ".")
      (.replace " " ".")
      (.replace ".resident" "")))

(defn resident-name
  "Convert canonical name to resident name. (ex: governor.linden -> Governor Linden)"
  [^String name]
  (clojure.string/join  " " (take 2 (concat (map clojure.string/capitalize (clojure.string/split name #"\.")) ["Resident"]))))

(defn valid-name?
  "Return true if name is a valid (not necessarily canonical) resident/user name."
  [name]
  (.matches (re-matcher RE_VALID_NAME name)))
