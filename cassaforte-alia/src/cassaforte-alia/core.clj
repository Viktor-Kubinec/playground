(ns cassaforte-alia.core
  (:require [qbits.alia :as alia]
            [clojurewerkz.cassaforte.client :as cc]
            [clojure.core.async :as a :refer [>! <! <!! >!! go chan close! go-loop]]))

(def current (atom 0))

(defn is-before [x] (> x (java.lang.System/currentTimeMillis)))

(defn test-d [f]
  (let [c (chan 100)
        end-time (+ 10000 (java.lang.System/currentTimeMillis))]
    (go-loop [x 1] 
             (when (and (>! c (<! (f))) 
                        (< x 50000)
                        (is-before end-time)) 
                   (recur (inc x))))
    (go-loop [y 1]
             (if-let [v (<! c)]
               (if (is-before end-time)
                   (do (swap! current inc)
                       (recur (inc y)))
                   (do (println "Time's up. Processed so far : " @current)
                       (reset! current 0)))))))

(defn cassaforte-execute-chan
  [session query]
  (let [c (chan)]
    (go (>! c (deref (cc/execute-async session query))))
    c))

(def session-cassaforte (cc/connect ["localhost"] {:keyspace "test"}))

(def cluster (alia/cluster {:contact-points ["localhost"]}))
(def session-alia (alia/connect cluster))
(alia/execute session-alia "USE test;")

(def slow-query "select * from csessions;")
(def fast-query "select * from csessions where id =767da9c5-7c8e-4bba-ab8d-06c8d5a9d6b0;")
(defn alia-test-slow-query [] (alia/execute-chan session-alia slow-query))
(defn cassa-test-slow-query [] (cassaforte-execute-chan session-alia slow-query))
(defn alia-test-fast-query [] (alia/execute-chan session-alia fast-query))
(defn cassa-test-fast-query [] (cassaforte-execute-chan session-alia fast-query))
