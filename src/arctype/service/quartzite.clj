(ns ^{:doc "Quartzite scheduler wrapper"}
  arctype.service.quartzite
  (:import [java.util TimeZone])
  (:require
    [clojure.string :as string]
    [clojure.tools.logging :as log]
    [clojurewerkz.quartzite.scheduler :as qs]
    [clojurewerkz.quartzite.schedule.cron :as cron]
    [schema.core :as S]
    [sundbry.resource :as resource :refer [with-resources]]
    [arctype.service.protocol :refer :all]))

(def Config
  {})

(def default-config
  {})

(defn- parse-24h
 [time-str]
 (mapv #(Integer/parseInt %) (string/split time-str #":")))

(defn make-daily-schedules
  [times-24h time-zone]
  (for [time-str times-24h
        :let [[h m] (parse-24h time-str)]]
    (cron/schedule 
      (cron/daily-at-hour-and-minute h m)
      (cron/in-time-zone (TimeZone/getTimeZone time-zone)))))

(defrecord Quartzite [config scheduler]
  PLifecycle
  (start [this]
    (log/debug {:message "Starting Quartzite service"})
    (-> this
        (update :scheduler qs/start)))

  (stop [this]
    (log/debug {:message "Stopping Quartzite service"})
    (qs/shutdown scheduler true)
    this)
  
  PClientDecorator
  (client [this]
    scheduler))

(S/defn create
  [resource-name
   config :- Config]
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->Quartzite
        {:config config
         :scheduler (qs/initialize)})
      resource-name)))
