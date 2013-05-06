(ns com.w-hat.util)

(defn mmap
  "Apply function to each value of map."
  [f m]
  (into (empty m) (for [[k v] m] [k (f v)])))

(defn filter-subkeys
  "Return only entries in m where the value contains a key for which (pred key) returns true"
  [pred m]
  (into (empty m) (filter #(some pred (-> % val keys)) m)))
