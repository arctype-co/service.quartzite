(ns ^{:doc "Quartzite scheduler wrapper"}
  arctype.service.quartzite
  (:require
    [clojure.tools.logging :as log]
    [clojurewerkz.quartzite.scheduler :as qs]
    [schema.core :as S]
    [sundbry.resource :as resource :refer [with-resources]]
    [arctype.service.protocol :refer :all]))

(def Config
  {})

(def default-config
  {})

(defrecord Quartzite [config scheduler]
  PLifecycle
  (start [this]
    (log/debug {:message "Starting Quartzite service"})
    (-> this
        (update :scheduler qs/start)))

  (stop [this]
    (log/debug {:message "Stopping Quartzite service"})
    (doto scheduler qs/standby qs/shutdown)
    this))

(S/defn create
  [resource-name
   config :- Config]
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->Quartzite
        {:config config
         :scheduler (qs/initialize)})
      resource-name)))
