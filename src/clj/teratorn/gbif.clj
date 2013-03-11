(ns teratorn.gbif
  "This namespace provides support for GBIF data."
  (:use [cascalog.api]
        [cascalog.more-taps :as taps :only (hfs-delimited)])
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [cascalog.ops :as c]))

;; Ordered column names from the occurrence_20120802.txt.gz GBIF dump.
(def gbif-fields ["?occurrenceid" "?taxonid" "?dataresourceid" "?kingdom"
                 "?phylum" "?class" "?orderrank" "?family" "?genus"
                 "?scientificname" "?kingdomoriginal" "?phylumoriginal"
                 "?classoriginal" "?orderrankoriginal" "?familyoriginal"
                 "?genusoriginal" "?scientificnameoriginal" "?authororiginal"
                 "?datecollected" "?year" "?month" "?basisofrecord"
                 "?countryoriginal" "?countryisointerpreted" "?locality"
                 "?county" "?continentorocean" "?stateprovince" "?latitude"
                 "?latitudeinterpreted" "?longitude" "?longitudeinterpreted"
                 "?coordinateprecision" "?geospatialissue" "?lastindexed"])

;; Ordered column names for MOL master dataset schema.
(def mol-fields ["?uuid" "?occurrenceid" "?taxonid" "?dataresourceid" "?kingdom"
                 "?phylum" "?class" "?orderrank" "?family" "?genus"
                 "?scientificname" "?datecollected" "?theyear" "?themonth"
                 "?basisofrecord" "?countryisointerpreted" "?locality"
                 "?county" "?continentorocean" "?stateprovince"
                 "?lat" "?lon" "?precision" "?geospatialissue" "?lastindexed"
                 "?season"])

;; Ordered column names for MOL occ table schema.
(def occ-fields ["?taxloc-uuid" "?uuid" "?occurrenceid" "?taxonid"
                 "?dataresourceid" "?datecollected" "?theyear" "?themonth"
                 "?basisofrecord" "?countryisointerpreted" "?locality" "?county"
                 "?continentorocean" "?stateprovince" "?precision"
                 "?geospatialissue" "?lastindexed" "?season"])

;; Ordered column names for MOL occ table schema that include
;; tax-uuid, loc-uuid.
(def occ-fields-extras ["?taxloc-uuid" "?tax-uuid" "?loc-uuid" "?uuid"
                        "?occurrenceid" "?taxonid" "?dataresourceid"
                        "?datecollected" "?theyear" "?themonth" "?basisofrecord"
                        "?countryisointerpreted" "?locality" "?county"
                        "?continentorocean" "?stateprovince" "?precision"
                        "?geospatialissue" "?lastindexed" "?season"])

(defn str->num-or-empty-str
  "Convert a string to a number with read-string and return it. If not a
   number, return an empty string.

   Try/catch form will catch exception from using read-string with
   non-decimal degree or entirely wrong lats and lons (a la 5°52.5'N, 6d
   10m s S, or 0/0/0 - all have been seen in the data).

   Note that this will also handle major errors in the lat/lon fields
   that may be due to mal-formed or non-standard input text lines that
   would otherwise cause parsing errors."
  [s]
  (try
    (let [parsed-str (read-string s)]
      (if (number? parsed-str)
        parsed-str
        ""))
    (catch Exception e "")))

(defn handle-zeros
  "Handle trailing decimal points and trailing zeros. A trailing decimal
   point is removed entirely, while trailing zeros are only dropped if
   they immediately follow the decimal point.

   Usage:
     (handle-zeros \"3.\")
     ;=> \"3\"

     (handle-zeros \"3.0\")
     ;=> \"3\"

     (handle-zeros \"3.00\")
     ;=> \"3\"

     (handle-zeros \"3.001\")
     ;=> \"3.001\"

     (handle-zeros \"3.00100\")
     ;=>\"3.00100\""
  [s]
  (let [[head tail] (s/split s #"\.")]
    (if (or (zero? (count tail)) ;; nothing after decimal place
            (zero? (Integer/parseInt tail))) ;; all zeros after decimal place
      (str (Integer/parseInt head))
      s)))

(defn round-to
  "Round a value to a given number of decimal places and return a
   string. Note that this will drop all trailing zeros, and values like
   3.0 will be returned as \"3\""
  [digits n]
  (let [formatter (str "%." (str digits) "f")]
    (if (= "" n)
      n
      (->> (format formatter (double n))
           reverse
           (drop-while #{\0})
           reverse
           (apply str)
           (handle-zeros)))))

;; eBird data resource id:
(def ^:const EBIRD-ID "43")

(defn not-ebird
  "Return true if supplied id represents an eBird record, otherwise false."
  [id]
  (not= id EBIRD-ID))

(def season-map
  "Encodes seasons as indices: 0-3 for northern hemisphere, 4-7 for the south"
  {"N winter" 0
   "N spring" 1
   "N summer" 2
   "N fall" 3
   "S winter" 4
   "S spring" 5
   "S summer" 6
   "S fall" 7})

(defn parse-hemisphere
  "Returns a quarter->season map based on the hemisphere."
  [h]
  (let [n_seasons {0 "winter" 1 "spring" 2 "summer" 3 "fall"}
        s_seasons {0 "summer" 1 "fall" 2 "winter" 3 "spring"}]
    (if (= h "N") n_seasons s_seasons)))

(defn get-season-idx
  "Returns season index (roughly quarter) given a month."
  [month]
  {:pre [(>= 12 month)]}
  (let [season-idxs {11 0 12 0 1 0
                     2 1 3 1 4 1
                     5 2 6 2 7 2
                     8 3 9 3 10 3}]
    (get season-idxs month)))

(defn get-season
  "Based on the latitude and the month, return a season index
   as given in season-map.

   Usage:
     (get-season 40.0 1)
     ;=> \"0\""
  [lat month]
  (if (= "" month)
    ""
    (let [lat (if (string? lat) (read-string lat) lat)
          month (if (string? month) (read-string month) month)
          hemisphere (if (pos? lat) "N" "S")
          season (get (parse-hemisphere hemisphere)
                      (get-season-idx month))]
      (str (get season-map (format "%s %s" hemisphere season))))))

(defn makeline
  "Returns a string line by joining a sequence of values on tab."
  [& vals]
  (s/join \tab vals))

(defn split-line
  "Returns vector of line values by splitting on tab."
  [line]
  (vec (.split line "\t")))

(defn gen-uuid
  "Return a randomly generated UUID string."
  [& x]
  (str (java.util.UUID/randomUUID)))

(defn valid-latlon?
  "Return true if lat and lon are valid decimal degrees,
   otherwise return false. Assumes that lat and lon are both either numeric
   or string."
  [lat lon]
  (if (or (= "" lat)
          (= "" lon))
    false   
    (try
      (let [[lat lon] (if (number? lat)
                        [lat lon]
                        (map read-string [lat lon]))
            latlon-range {:lat-min -90 :lat-max 90 :lon-min -180 :lon-max 180}
            {:keys [lat-min lat-max lon-min lon-max]} latlon-range]
        (and (<= lat lat-max)
             (>= lat lat-min)
             (<= lon lon-max)
             (>= lon lon-min)))
      (catch Exception e false))))

(defn valid-name?
  "Return true if name is valid, otherwise return false."
  [name]
  (and (not= name nil) (not= name "") (not (.contains name "\""))))

(defn cleanup-data
  "Cleanup data by handling rounding, missing data, etc."
  [digits lat lon prec year month]
  (let [[lat lon clean-prec clean-year clean-month] (map str->num-or-empty-str [lat lon prec year month])]
    (concat (map (partial round-to digits) [lat lon clean-prec])
            (map str [clean-year clean-month]))))

(defn quoter
  "Return x surrounded in double quotes with any double quotes escaped with
  double quotes, if needed."
  [x]
  (if (or (and (.startsWith x "\"") (.endsWith x "\""))
          (= "" x)
          (not (.contains x "\""))) 
    x
    (format "\"%s\"" (.replace x "\"" "\"\"") "\"" "\"\"")))
  
(defn quotemaster
  "Maps quoter function over supplied vals."
  [& vals]
  (vec (map quoter vals)))

(defn read-occurrences
  "Return Cascalog generator of GBIF tuples with valid Scientific name and
   coordinates."
  [path]
  (let [src (hfs-textline path)
        sigfigs 7]
    (<- mol-fields
        (src ?line)
        (gen-uuid :> ?uuid)
        (s/replace ?line "\\N" "" :> ?clean-line)
        (split-line ?clean-line :>> gbif-fields)
        (not-ebird ?dataresourceid)
        (cleanup-data sigfigs ?latitudeinterpreted ?longitudeinterpreted ?coordinateprecision ?year ?month :>
                      ?lat ?lon ?precision ?theyear ?themonth)
        (valid-latlon? ?latitudeinterpreted ?longitudeinterpreted)
        (valid-name? ?scientificname)
        (get-season ?lat ?themonth :> ?season))))

(defn occ-query
  "Return generator of unique occurrences with a taxloc-id."
  [tax-source loc-source taxloc-source occ-source]
  (let [uniques (<- [?tax-uuid ?loc-uuid ?occurrenceid ?scientificname ?kingdom
                     ?phylum ?class ?orderrank ?family ?genus ?lat ?lon]
                    (tax-source ?tax-uuid ?scientificname ?kingdom ?phylum ?class ?orderrank ?family ?genus)
                    (loc-source ?loc-uuid ?lat ?lon)
                    (occ-source :>> mol-fields)
                    (:distinct true))]
    (<- occ-fields
        (uniques ?tax-uuid ?loc-uuid ?occurrenceid ?scientificname ?kingdom
                 ?phylum ?class ?orderrank ?family ?genus ?latitudeinterpreted
                 ?longitudeinterpreted)
        (taxloc-source ?taxloc-uuid ?tax-uuid ?loc-uuid)
        (occ-source :>> mol-fields))))

(defn taxloc-query
  "Return generator of unique taxonomy locations from supplied source of unique
  taxonomies (via tax-query), unique locations (via loc-query), and occurrence
  source of mol-fields."
  [tax-source loc-source occ-source & {:keys [with-uuid] :or {with-uuid true}}]
  (let [occ (<- [?lat ?lon ?scientificname
                 ?kingdom ?phylum ?class ?orderrank ?family ?genus]
                (occ-source :>> mol-fields))
        uniques (<- [?tax-uuid ?loc-uuid]
                    (tax-source ?tax-uuid ?s ?k ?p ?c ?o ?f ?g)
                    (loc-source ?loc-uuid ?lat ?lon)
                    (occ ?lat ?lon ?s ?k ?p ?c ?o ?f ?g)
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
  (let [uniques (<- [?scientificname ?kingdom ?phylum ?class ?orderrank ?family ?genus]
                    (source :>> mol-fields)
                    (:distinct true))]
    (if with-uuid
      (<- [?uuid ?s ?k ?p ?c ?o ?f ?g]
          (uniques ?s ?k ?p ?c ?o ?f ?g)
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
  "Convert raw GBIF data into seqfiles in MoL schema with invalid records filtered
   out."
  [& {:keys [source-path sink-path]
      :or {source-path (.getPath (io/resource "occ.txt"))
           sink-path "/tmp/mds"}}]
  (let [query (read-occurrences source-path)]
    (?- (hfs-seqfile sink-path :sinkmode :replace) query)))

(defn build-cartodb-schema
  [& {:keys [source-path sink-path with-uuid]
      :or {source-path "/tmp/mds"
           sink-path "/tmp"
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
    (?- loc-sink (loc-query source))
    (?- tax-sink (tax-query source))
    (?- taxloc-sink (taxloc-query tax-sink loc-sink source))
    (?- occ-sink (occ-query tax-sink loc-sink taxloc-sink source))))

(defn build-cartodb-views
  [& {:keys [source-path sink-path]
      :or {source-path "/tmp"
           sink-path "/tmp/cdb"}}]
  (let [loc-sink (hfs-textline (format "%s/loc" sink-path) :sinkmode :replace)
        loc-source (hfs-seqfile (format "%s/loc" source-path))
        tax-sink (hfs-textline (format "%s/tax" sink-path) :sinkmode :replace)
        tax-source (hfs-seqfile (format "%s/tax" source-path))
        taxloc-sink (hfs-textline (format "%s/taxloc" sink-path) :sinkmode :replace)
        taxloc-source (hfs-seqfile (format "%s/taxloc" source-path))
        occ-sink (hfs-textline (format "%s/occ" sink-path) :sinkmode :replace)
        occ-source (hfs-seqfile (format "%s/occ" source-path))
        occ-query (<- [?a ?b ?c ?d ?e ?f ?g ?h ?i ?j ?k ?l ?m ?n ?o ?p ?q ?r]
                      (occ-source :>> occ-fields)
                      (quotemaster ?taxloc-uuid ?uuid ?occurrenceid ?taxonid
                              ?dataresourceid ?datecollected ?theyear ?themonth
                              ?basisofrecord ?countryisointerpreted ?locality
                              ?county ?continentorocean ?stateprovince
                              ?precision ?geospatialissue ?lastindexed ?season :>
                              ?a ?b ?c ?d ?e ?f ?g ?h ?i ?j ?k ?l ?m ?n ?o ?p ?q
                              ?r))]
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

