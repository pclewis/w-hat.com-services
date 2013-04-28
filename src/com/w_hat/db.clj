(ns com.w-hat.db
  (:require [taoensso.carmine :as car]))

(def pool (car/make-conn-pool))
(def spec (car/make-conn-spec :host "localhost"))
(defmacro wcar [& body] `(car/with-conn pool spec ~@body))
