(defproject com.w-hat/services "1.0.0-SNAPSHOT"
  :description "Services for w-hat.com"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [compojure "1.1.5"]
                 [http-kit "2.0.1"]
                 [com.taoensso/carmine "1.7.0"]
                 [ring/ring-core "1.1.8"]
                 [ring/ring-devel "1.1.8"]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-logging-config "1.9.10"]
                 [org.clojure/core.memoize "0.5.3"]
                 [commons-codec/commons-codec "1.8"]]
  :plugins       [[lein-kibit "0.0.8"]]
  :warn-on-reflection true
  :main com.w-hat.core)
