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
    (update this :scheduler qs/start))

  (stop [this]
    (-> scheduler qs/standby qs/shutdown)))

(S/defn create
  [resource-name
   config :- Config]
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->Quartzite
        {:config config
         :scheduler (qs/initialize)})
      resource-name)))
