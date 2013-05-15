(ns teratorn.ebird
  "This namespace provides support for eBird data."
  (:use [teratorn.common]
        [cascalog.api]
        [cascalog.more-taps :as taps :only (hfs-delimited)]
        [clojure.data.json :only (read-json)])
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [cascalog.ops :as c]
            [clojure.data.csv :as csv]))

;; Ordered column names from ebird dump.
(def ebird-fields ["?global-unique-identifier" "?taxonomic-order" "?category" "?common-name" "?scientific-name" "?subspecies-common-name" "?subspecies-scientific-name" "?observation-count" "?breeding-bird-atlas-code" "?age-sex" "?country" "?country-code" "?state" "?state-code" "?county" "?county-code" "?iba-code" "?locality" "?locality-id" "?locality-type" "?latitude" "?longitude" "?observation-date" "?time-observations-started" "?trip-comments" "?species-comments" "?observer-id" "?first-name" "?last-name" "?sampling-event-identifier" "?protocol-type" "?project-code" "?duration-minutes" "?effort-distance-km" "?effort-area-ha" "?number-observers" "?all-species-reported" "?group-identifier" "?approved" "?reviewed" "?reason"])

;; Ordered column names for MOL master dataset schema.
(def mol-fields ["?uuid" "?global-unique-identifier" "?taxonomic-order" "?category" "?common-name" "?scientific-name" "?subspecies-common-name" "?subspecies-scientific-name" "?observation-count" "?breeding-bird-atlas-code" "?age-sex" "?country" "?country-code" "?state" "?state-code" "?county" "?county-code" "?iba-code" "?locality" "?locality-id" "?locality-type" "?lat" "?lon" "?observation-date" "?time-observations-started" "?trip-comments" "?species-comments" "?observer-id" "?first-name" "?last-name" "?sampling-event-identifier" "?protocol-type" "?project-code" "?duration-minutes" "?effort-distance-km" "?effort-area-ha" "?number-observers" "?all-species-reported" "?group-identifier" "?approved" "?reviewed" "?reason" "?season", "?theyear", "?themonth", "?theday"])

;; Ordered column names for MOL occ table schema.
(def occ-fields ["?taxloc-uuid" "?uuid" "?global-unique-identifier" "?taxonomic-order" "?category" "?common-name" "?scientific-name" "?subspecies-common-name" "?subspecies-scientific-name" "?observation-count" "?breeding-bird-atlas-code" "?age-sex" "?country" "?country-code" "?state" "?state-code" "?county" "?county-code" "?iba-code" "?locality" "?locality-id" "?locality-type" "?lat" "?lon" "?observation-date" "?time-observations-started" "?trip-comments" "?species-comments" "?observer-id" "?first-name" "?last-name" "?sampling-event-identifier" "?protocol-type" "?project-code" "?duration-minutes" "?effort-distance-km" "?effort-area-ha" "?number-observers" "?all-species-reported" "?group-identifier" "?approved" "?reviewed" "?reason" "?season", "?theyear", "?themonth", "?theday"])


 (defn ebird-split-line
  "Returns vector of line values by splitting on tab."
  [line]
  (let [vals (vec (.split line "\t"))
        n (count vals)]
    ;; e-bird lines have funky trailing tabs, so this hack deals with that:
    (cond (< n 41) (conj vals "")
          (> n 41) (subvec vals 0 41)
          :else vals)))

(defn parse-date
  "Return vector of year, month, day parsed out from supplied date string of the
  form YYYY-MM-DD."
  [date]
  (try
    (let [[year month day] (s/split date #"-")]
      (map str [year month day]))
    (catch Exception e
        ["" "" ""])))

(defn read-occurrences
  "Return Cascalog generator of ebird tuples with valid Scientific name and
   coordinates."
  [path]
  (let [src (hfs-textline path)
        sigfigs 7]
    (<- mol-fields
        (src ?line)
        (gen-uuid :> ?uuid)
        (ebird-split-line ?line :>> ebird-fields)
        (cleanup-data sigfigs ?latitude ?longitude ?year ?month ?day :> ?lat ?lon ?theyear ?themonth ?theday)
        (valid-latlon? ?latitude ?longitude)
        (valid-name? ?scientific-name)
        (parse-date ?observation-date :> ?year ?month ?day)
        (get-season ?lat ?themonth :> ?season))))

(defn occ-query
  "Return generator of unique occurrences with a taxloc-id."
  [tax-source loc-source taxloc-source occ-source]
  (let [uniques (<- [?tax-uuid ?loc-uuid ?global-unique-identifier
                     ?scientific-name ?common-name ?subspecies-common-name
                     ?subspecies-scientific-name ?taxonomic-order ?lat ?lon]
                    (tax-source ?tax-uuid ?scientific-name ?common-name
                                ?subspecies-common-name
                                ?subspecies-scientific-name ?taxonomic-order)
                    (loc-source ?loc-uuid ?lat ?lon)
                    (occ-source :>> mol-fields)
                    (:distinct true))]
    (<- occ-fields
        (uniques ?tax-uuid ?loc-uuid ?global-unique-identifier
                 ?scientific-name ?common-name ?subspecies-common-name
                 ?subspecies-scientific-name ?taxonomic-order ?latitude
                 ?longitude)
        (taxloc-source ?taxloc-uuid ?tax-uuid ?loc-uuid)
        (occ-source :>> mol-fields))))

(defn taxloc-query
  "Return generator of unique taxonomy locations from supplied source of unique
  taxonomies (via tax-query), unique locations (via loc-query), and occurrence
  source of mol-fields."
  [tax-source loc-source occ-source & {:keys [with-uuid] :or {with-uuid true}}]
  (let [occ (<- [?lat ?lon ?scientific-name ?common-name ?subspecies-common-name
                 ?subspecies-scientific-name ?taxonomic-order]
                (occ-source :>> mol-fields))
        uniques (<- [?tax-uuid ?loc-uuid]
                    (tax-source ?tax-uuid ?s ?c ?sc ?ss ?t)
                    (loc-source ?loc-uuid ?lat ?lon)
                    (occ ?lat ?lon ?s ?c ?sc ?ss ?t)
                    (:distinct true))]
    (if with-uuid
      (<- [?uuid ?tax-uuid ?loc-uuid]
          (uniques ?tax-uuid ?loc-uuid)
          (gen-uuid :> ?uuid))
      uniques)))

(defn tax-query
  "Return generator of unique taxonomy tuples from supplied source of mol-fields.
   Assumes sounce contains valid ?scientificname."
  [source & {:keys [with-uuid] :or {with-uuid true}}]  
  (let [uniques (<- [?scientific-name ?common-name ?subspecies-common-name
                     ?subspecies-scientific-name ?taxonomic-order]
                    (source :>> mol-fields)
                    (:distinct true))]
    (if with-uuid
      (<- [?uuid ?s ?c ?sc ?ss ?t]
          (uniques ?s ?c ?sc ?ss ?t)
          (gen-uuid :> ?uuid))
      uniques)))

(defn loc-query
  "Return generator of unique coordinate tuples from supplied source of
   mol-fields. Assumes source contains valid coordinates."
  [source & {:keys [with-uuid] :or {with-uuid true}}]
  (let [uniques (<- [?lat ?lon]
                    (source :>> mol-fields)
                    (:distinct true))]
    (if with-uuid
      (<- [?uuid ?lat ?lon]
          (uniques ?lat ?lon)
          (gen-uuid :> ?uuid))
      uniques)))

(defn build-master-dataset
  "Convert raw ebird data into seqfiles in MoL schema with invalid records filtered
   out."
  [& {:keys [source-path sink-path]
      :or {source-path (.getPath (io/resource "ebird.txt"))
           sink-path "/tmp/teratorn/ebird/master"}}]
  (let [query (read-occurrences source-path)]
    (?- (hfs-seqfile sink-path :sinkmode :replace) query)))

(defn build-cartodb-schema
  [& {:keys [source-path sink-path with-uuid]
      :or {source-path "/tmp/teratorn/ebird/master"
           sink-path "/tmp/teratorn/ebird/cartodb"
           with-uuid true}}]
  (let [source (hfs-seqfile source-path)
        loc-path (format "%s/loc" sink-path)
        loc-sink (hfs-seqfile loc-path :sinkmode :replace)
        tax-path (format "%s/tax" sink-path)
        tax-sink (hfs-seqfile tax-path :sinkmode :replace)
        taxloc-path (format "%s/taxloc" sink-path)
        taxloc-sink (hfs-seqfile taxloc-path :sinkmode :replace)
        occ-path (format "%s/occ" sink-path)
        occ-sink (hfs-seqfile occ-path :sinkmode :replace)]
    (do
      (?- loc-sink (loc-query source))
      (?- tax-sink (tax-query source))
      (?- taxloc-sink (taxloc-query tax-sink loc-sink source))
      (?- occ-sink (occ-query tax-sink loc-sink taxloc-sink source)))))

(defn build-cartodb-views
  [& {:keys [source-path sink-path]
      :or {source-path "/tmp/teratorn/ebird/cartodb"
           sink-path "/tmp/teratorn/ebird/views"}}]
  (let [loc-sink (hfs-textline (format "%s/loc" sink-path) :sinkmode :replace)
        loc-source (hfs-seqfile (format "%s/loc" source-path))
        tax-sink (hfs-textline (format "%s/tax" sink-path) :sinkmode :replace)
        tax-source (hfs-seqfile (format "%s/tax" source-path))
        taxloc-sink (hfs-textline (format "%s/taxloc" sink-path) :sinkmode :replace)
        taxloc-source (hfs-seqfile (format "%s/taxloc" source-path))
        occ-sink (hfs-textline (format "%s/occ" sink-path) :sinkmode :replace)
        occ-source (hfs-seqfile (format "%s/occ" source-path))
        occ-query (<- [?a ?b ?c ?d ?e ?f ?g ?h ?i ?j ?k ?l ?m ?n ?o ?p ?q ?r ?s ?t ?u ?v ?w ?x ?y ?z ?aa ?ab ?ac ?ad ?ae ?af ?ag ?ah ?ai ?aj ?ak ?al ?am ?an ?ao ?ap ?aq ?ar ?as ?at ?au]
                      (occ-source :>> occ-fields)
                      (quotemaster ?taxloc-uuid ?uuid ?global-unique-identifier ?taxonomic-order ?category ?common-name ?scientific-name ?subspecies-common-name ?subspecies-scientific-name ?observation-count ?breeding-bird-atlas-code ?age-sex ?country ?country-code ?state ?state-code ?county ?county-code ?iba-code ?locality ?locality-id ?locality-type ?lat ?lon ?observation-date ?time-observations-started ?trip-comments ?species-comments ?observer-id ?first-name ?last-name ?sampling-event-identifier ?protocol-type ?project-code ?duration-minutes ?effort-distance-km ?effort-area-ha ?number-observers ?all-species-reported ?group-identifier ?approved ?reviewed ?reason ?season, ?theyear, ?themonth, ?theday :> ?a ?b ?c ?d ?e ?f ?g ?h ?i ?j ?k ?l ?m ?n ?o ?p ?q ?r ?s ?t ?u ?v ?w ?x ?y ?z ?aa ?ab ?ac ?ad ?ae ?af ?ag ?ah ?ai ?aj ?ak ?al ?am ?an ?ao ?ap ?aq ?ar ?as ?at ?au))]
    (?- loc-sink loc-source)
    (?- tax-sink tax-source)
    (?- taxloc-sink taxloc-source)
    (?- occ-sink occ-query)))

(defmain BuildMasterDataset
  [source-path sink-path]
  (build-master-dataset :source-path source-path :sink-path sink-path))

(defmain BuildCartoDBSchema
  [source-path sink-path]
  (build-cartodb-schema :source-path source-path :sink-path sink-path))

(defmain BuildCartoDBViews
  [source-path sink-path]
  (build-cartodb-views :source-path source-path :sink-path sink-path))

(defn ebird-s3
  "Sink local eBird textline to S3."
  [source-path s3path]
  (let [s3creds (read-json (slurp (io/resource "s3.json")))
        key (:access-key s3creds)
        secret (:secret-key s3creds)
        sink (str "s3n://" key  ":" secret "@" s3path)]
    (?- (hfs-textline sink :sinkmode :replace)
        (hfs-textline source-path))))

(defmain EbirdToS3
  "Sink local eBird textline to S3."
  [source-path s3path]
  (let [s3creds (read-json (slurp (io/resource "s3.json")))
        key (:access-key s3creds)
        secret (:secret-key s3creds)
        sink (str "s3n://" key  ":" secret "@" s3path)]
    (?- (hfs-textline sink :sinkmode :replace)
        (hfs-textline source-path))))

(comment
  (let [source-path (.getPath (io/resource "occ-test.tsv"))
        sink-path "/tmp/gbifer/master"]
    (BuildMasterDataset source-path sink-path))
  (let [source-path "/tmp/gbifer/master"
        sink-path "/tmp/gbifer/schema"]
    (BuildCartoDBSchema source-path sink-path))
  (let [source-path "/tmp/gbifer/schema"
        sink-path "/tmp/gbifer/views"]
    (BuildCartoDBViews source-path sink-path))

  (defn quoter
    [x]
    (when (and (.startsWith x "\"") (.endsWith x "\""))
      x
      (format "\"%s\"" x)))
  
  (defn quotey
    [& vals]
    (vec (map #(format "\"%s\"" %) vals)))

  (let [source (hfs-seqfile "/tmp/gbifer/schema/occ")
        q (<- [?a ?b ?c ?d ?e ?f ?g ?h ?i ?j ?k ?l ?m ?n ?o ?p ?q ?r]
              (source :>> occ-fields)
              (quotey ?taxloc-uuid ?uuid ?occurrenceid ?taxonid ?dataresourceid ?datecollected ?theyear ?themonth
                      ?basisofrecord ?countryisointerpreted ?locality ?county ?continentorocean ?stateprovince ?precision
                      ?geospatialissue ?lastindexed ?season :> ?a ?b ?c ?d ?e ?f ?g ?h ?i ?j ?k ?l ?m ?n ?o ?p ?q ?r))]
    (?- (hfs-delimited "/tmp/gbifer/views/occ" :quote "\"") q))
  
  (let [source (hfs-seqfile "/tmp/gbifer/schema/occ")
        q (<- [?a ?b ?c ?d ?e ?f ?g ?h ?i ?j ?k ?l ?m ?n ?o ?p ?q ?r]
              (source :>> occ-fields)
              (quotey ?taxloc-uuid ?uuid ?occurrenceid ?taxonid ?dataresourceid ?datecollected ?theyear ?themonth
                      ?basisofrecord ?countryisointerpreted ?locality ?county ?continentorocean ?stateprovince ?precision
                      ?geospatialissue ?lastindexed ?season :> ?a ?b ?c ?d ?e ?f ?g ?h ?i ?j ?k ?l ?m ?n ?o ?p ?q ?r))]
    (??- q))

  (let [dq (read-occurrences (.getPath (io/resource "occ-test.tsv")))
        lq (loc-query dq)
        tq (tax-query dq)]
    (?- (hfs-seqfile "/tmp/loc" :sinkmode :replace) lq)
    (?- (hfs-seqfile "/tmp/tax" :sinkmode :replace) tq)
    (?- (hfs-seqfile "/tmp/data" :sinkmode :replace) dq)
    (let [lq-source (hfs-seqfile "/tmp/loc")
          tq-source (hfs-seqfile "/tmp/tax")
          d-source (hfs-seqfile "/tmp/data")
          tlq (taxloc-query tq-source lq-source d-source)]
      (?- (hfs-seqfile "/tmp/taxloc" :sinkmode :replace) tlq)
      (let [tlq-source (hfs-seqfile "/tmp/taxloc")
            occ-q (occ-query tq-source lq-source tlq-source d-source)]
        (?- (hfs-seqfile "/tmp/occ" :sinkmode :replace) occ-q)))))

