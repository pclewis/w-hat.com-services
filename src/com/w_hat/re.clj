(ns com.w-hat.re
  "Utilities for composable regular expressions.")

; from clojure.contrib/reflect
(defn- call-method
  "Calls a private or protected method.

   params is a vector of classes which correspond to the arguments to
   the method e

   obj is nil for static methods, the instance object otherwise.

   The method-name is given a symbol or a keyword (something Named)."
  [^Class klass method-name params obj & args]
  (-> klass (.getDeclaredMethod (name method-name)
                                (into-array Class params))
      (doto (.setAccessible true))
      (.invoke obj (into-array Object args))))

(defn- named-groups
  "Return map of named groups to indexes from a Pattern"
  [^java.util.regex.Pattern pattern]
  (call-method java.util.regex.Pattern "namedGroups" [] pattern))

(defn- named-group-map
  "Return a map of {:group-name match-value} from a Matcher.
  
  To compensate for Java not supporting alternatives having the same group name,
  trailing digits are stripped from group names and the first such group with a
  match is used as the result.
  
  Ex: (?<name1>a)|(?<name2>b) -> {:name b}
  
  A match operation (.find etc) must already have been performed."
  [^java.util.regex.Matcher matcher]
  (let [gl (->> matcher (.pattern) (named-groups) (map key))
        ng (group-by #(re-find #"[a-zA-Z]+" %) gl)]
    (zipmap (map keyword (keys ng))
            (map (fn [v] (some identity (map #(.group matcher %) v)))
                 (vals ng)))))

(defn- match
  [f re s]
  (let [matcher (re-matcher re s)]
    (if (f matcher) (named-group-map matcher))))

(defn match-exact
  [re s]
  (match #(.matches %) re s))

(defn match-anywhere
  [re s]
  (match #(.find %) re s))

(defn match-all
  [re s]
  (let [matcher (re-matcher re s)]
    (take-while identity
                (repeatedly #(if (.find matcher) (named-group-map matcher))))))

(match-exact #"(?<a>hi)" "hi")