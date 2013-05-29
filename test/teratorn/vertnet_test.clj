(ns teratorn.vertnet-test
  "Unit test the teratorn.vertnet namespace."
  (:use teratorn.vertnet
        [cascalog.api]
        [midje sweet cascalog])
  (:require [clojure.java.io :as io]
            [cascalog.ops :as c]
            [cascalog.io :as cio]
            [gulo.util :as u]
            [gulo.fields :as f]))

(def harvest-src
  (let [harvest-path (.getPath (io/resource "vertnet-test/harvested.txt"))
        harvest-src (hfs-textline harvest-path)
        out-path (.getPath (cio/temp-dir "harvest-src"))
        sink (hfs-seqfile out-path :sinkmode :replace)]
    (do
      (?- sink (prep-harvested harvest-src))
      (hfs-seqfile out-path))))

(fact "Integration test for `prep-harvested`."
  (let [harvest-path (.getPath (io/resource "vertnet-test/harvested.txt"))
        harvest-src (hfs-textline harvest-path)
        src (prep-harvested harvest-src)]
    (<- [?url]
        (src :>> f/occ-fields)))
  => (produces-some
      [["http://ipt.vertnet.org:8080/ipt/resource.do?r=ttrs_birds"]]))

(fact "Check `tax-query`."
  (let [new-src (tax-query harvest-src :with-uuid true)]
    (<- [?scientificname ?kingdom ?phylum ?classs ?order ?family ?genus]
        (new-src _ ?scientificname ?kingdom ?phylum ?classs ?order ?family ?genus)))
  => (produces-some [["Accipiter cooperii" "Animalia" "Chordata" "Aves" "" "" ""]]))

(fact "Check `loc-query`."
  (let [new-src (loc-query harvest-src :with-uuid true)]
    (<- [?lat ?lon]
        (new-src _ ?lat ?lon)))
  => (produces-some [[29.6519572 -84.8886108]]))

(fact "Check `taxloc-query`. Doesn't check values of uuids, just
       ensures they're strings."
  (let [tax-src (tax-query harvest-src :with-uuid true)
        loc-src (loc-query harvest-src :with-uuid true)
        taxloc-src (taxloc-query tax-src loc-src harvest-src :with-uuid true)]
    (<- [?str1 ?str2 ?str3]
        (taxloc-src ?taxloc-uuid ?tax-uuid ?loc-uuid)
        ((c/each #'string?) ?taxloc-uuid ?tax-uuid ?loc-uuid :> ?str1 ?str2 ?str3)
        (:distinct true))) => (produces [[true true true]]))

(fact "Check `harvest-tax-loc-query`."
  (let [tax-src (tax-query harvest-src :with-uuid true)
        loc-src (loc-query harvest-src :with-uuid true)
        taxloc-src (taxloc-query tax-src loc-src harvest-src :with-uuid true)
        src (harvest-tax-loc-query tax-src loc-src harvest-src)]
    (<- [?scientificname ?kingdom ?phylum ?classs ?order ?family ?genus ?lat ?lon]
        (src :>> f/harvest-tax-loc-fields)))
  => (produces-some [["Actitis macularius" "Animalia" "Chordata" "Aves"
                      "" "" "" 29.6519572 -84.8886108]]))

(fact "Check `occ-query`."
  (let [loc-src (loc-query harvest-src :with-uuid true)
        tax-src (tax-query harvest-src :with-uuid true)
        taxloc-src (taxloc-query tax-src loc-src harvest-src :with-uuid true)
        src (occ-query tax-src loc-src taxloc-src harvest-src)]
    (<- [?scientificname ?kingdom ?phylum ?classs ?order ?family ?genus]
        (src :>> f/vertnet-fields)))
  => (produces-some
      [["Eudocimus albus" "Animalia" "Chordata" "Aves" "" "" ""]]))
