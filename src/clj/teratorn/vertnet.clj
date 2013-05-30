(ns teratorn.vertnet
  "This namespace provides support for VertNet data."
  (:use [cascalog.api]
        [cascalog.more-taps :as taps :only (hfs-delimited)])
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [cascalog.ops :as c]
            [cascalog.io :as cio]
            [gulo.util :as u]
            [gulo.fields :as f]))

(defn prep-harvested
  [harvest-src & {:keys [with-uuid] :or {with-uuid true}}]
  (let [uniques (<- f/harvest-fields
                    (harvest-src ?line)
                    (u/split-line ?line :>> f/harvest-fields)
                    (:distinct true))]
    (if with-uuid
      (<- f/occ-fields
          (uniques :>> f/harvest-fields)
          (u/gen-uuid :> ?occ-uuid))
      uniques)))

(defn tax-query
  "Return generator of unique taxonomy tuples from supplied source of harvest-fields.
   Assumes sounce contains valid ?scientificname."
  [source & {:keys [with-uuid] :or {with-uuid true}}]  
  (let [uniques (<- [?scientificname ?kingdom ?phylum ?classs ?order ?family ?genus]
                    (source :>> f/occ-fields)
                    (:distinct true))]
    (if with-uuid
      (<- [?uuid ?s ?k ?p ?c ?o ?f ?g]
          (uniques ?s ?k ?p ?c ?o ?f ?g)
          (u/gen-uuid :> ?uuid))
      uniques)))

(defn loc-query
  "Return generator of unique coordinate tuples from supplied source of
   vertnet-fields. Assumes source contains valid coordinates."
  [source & {:keys [with-uuid] :or {with-uuid true}}]
  (let [sig-figs 7
        uniques (<- [?lat ?lon]
                    (source :>> f/occ-fields)
                    (u/cleanup-data sig-figs ?decimallatitude ?decimallongitude
                                  ?coordinateprecision ?year ?month :>
                                  ?lat ?lon _ _ _)
                    (u/valid-latlon? ?lat ?lon)
                    (:distinct true))]
    (if with-uuid
      (<- [?uuid ?lat ?lon]
          (uniques ?lat ?lon)
          (u/gen-uuid :> ?uuid))
      uniques)))

(defn taxloc-query
  "Return generator of unique taxonomy locations from supplied source of unique
  taxonomies (via tax-query), unique locations (via loc-query), and occurrence
  source of mol-fields."
  [tax-source loc-source occ-source & {:keys [with-uuid] :or {with-uuid true}}]
  (let [sig-figs 7
        occ (<- [?lat ?lon ?scientificname
                 ?kingdom ?phylum ?classs ?order ?family ?genus]
                (occ-source :>> f/occ-fields)
                (u/cleanup-data sig-figs ?decimallatitude ?decimallongitude ?coordinateprecision
                              ?year ?month :> ?lat ?lon _ _ _)
                (u/valid-latlon? ?lat ?lon))
        uniques (<- [?tax-uuid ?loc-uuid]
                    (tax-source ?tax-uuid ?s ?k ?p ?c ?o ?f ?g)
                    (loc-source ?loc-uuid ?lat ?lon)
                    (occ ?lat ?lon ?s ?k ?p ?c ?o ?f ?g)
                    (:distinct true))]
    (if with-uuid
      (<- [?uuid ?tax-uuid ?loc-uuid]
          (uniques ?tax-uuid ?loc-uuid)
          (u/gen-uuid :> ?uuid))
      uniques)))

(defn harvest-tax-loc-query
  [tax-src loc-src occ-src & [sig-figs]]
  (let [sig-figs (or sig-figs 7)]
    (<- f/harvest-tax-loc-fields ;;[?scientificname]
        (tax-src ?tax-uuid ?scientificname ?kingdom ?phylum ?classs ?order ?family ?genus)
        (loc-src ?loc-uuid ?lat ?lon)        
        (occ-src :>> f/occ-fields)
        (u/cleanup-data sig-figs ?decimallatitude ?decimallongitude ?coordinateprecision
                      ?year ?month :> ?lat ?lon _ _ _)
        (u/valid-latlon? ?lat ?lon))))

(defn occ-query
  "Return generator of unique occurrences with a taxloc-id."
  [tax-src loc-src taxloc-src occ-src]
  (let [src (harvest-tax-loc-query tax-src loc-src occ-src)] 
    (<- f/vertnet-fields
        (src :>> f/harvest-tax-loc-fields)
        (taxloc-src ?taxloc-uuid ?tax-uuid ?loc-uuid)
        (occ-src :>> f/occ-fields))))

(defmain Shred
  [harvest-path seq-path tables-path]
  (let [seq-sink #(hfs-seqfile (.getPath (cio/temp-dir %)) :sinkmode :replace)
        seq-source #(hfs-seqfile (.getPath (cio/temp-dir %))) 
        harvest-src (hfs-textline harvest-path)
        _ (?- (seq-sink "shred") (prep-harvested harvest-src))
        src (seq-source "shred")
        [sink-loc sink-tax sink-tax-loc sink-occ] (map seq-sink
                                                       ["loc" "tax" "tax-loc" "occ"])]
    (?- sink-loc (loc-query (seq-source "shred")))
    (?- sink-tax (tax-query (seq-source "shred")))
    (?- sink-tax-loc (taxloc-query (seq-source "tax") (seq-source "loc") src))
    (?- sink-occ (occ-query (seq-source "tax")
                            (seq-source "loc")
                            (seq-source "tax-loc") src))))
