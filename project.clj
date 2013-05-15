(defproject teratorn "0.1.0-SNAPSHOT"
  :description "Big data processing on Hadoop for Map of Life."
  :repositories {"conjars" "http://conjars.org/repo/"
                 "gbif" "http://repository.gbif.org/content/groups/gbif/"
                 "maven2" "http://repo2.maven.org/maven2"}
  :source-paths ["src/clj"]
  :resources-path "resources"
  :dev-resources-path "dev"
  :jvm-opts ["-XX:MaxPermSize=256M"
             "-XX:+UseConcMarkSweepGC"
             "-Xms1024M" "-Xmx1048M" "-server"]
  :plugins [[lein-swank "1.4.4"]
            [lein-emr "0.1.0-SNAPSHOT"]
            [lein-midje "3.0-beta1"]]
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                                  [cascalog/midje-cascalog "1.10.1-SNAPSHOT"]]}
             :plugins [[lein-midje "3.0-beta1"]]}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascalog "1.10.0"]                                                         ;
                 [cascalog-more-taps "0.3.1-SNAPSHOT"]
                 [org.clojure/data.json "0.2.1"]
                 [org.clojure/data.csv "0.1.2"]]
  :min-lein-version "2.0.0"
  :aot [teratorn.common teratorn.gbif teratorn.ebird])
