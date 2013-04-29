(ns com.w-hat.db
  (:require [taoensso.carmine :as car]
            [com.w-hat.config :refer [config]]))

(def pool (car/make-conn-pool))
(def spec (car/make-conn-spec :host (config :redis-host)))
(defmacro wcar [& body] `(car/with-conn pool spec ~@body))