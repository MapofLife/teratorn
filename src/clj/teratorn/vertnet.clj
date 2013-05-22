(ns teratorn.vertnet
  "This namespace provides support for VertNet data."
  (:use [cascalog.api]
        [cascalog.more-taps :as taps :only (hfs-delimited)]
        [teratorn.common])
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [cascalog.ops :as c]
            [cascalog.io :as cio]))

(def resource-fields
  "Ordered vector of fields used in resource map."
  [:pubdate :url :eml :dwca :title :icode :description :contact :orgname :email
   :emlrights :count])

(def base-fields
  "Ordered vector of VertNet field names. Used as base for various
   vectors of column and Cascalog field names."
  ["harvestid" "id" "associatedmedia" "associatedoccurrences" "associatedreferences"
   "associatedsequences" "associatedtaxa" "basisofrecord" "bed" "behavior"
   "catalognumber" "collectioncode" "collectionid" "continent"
   "coordinateprecision" "coordinateuncertaintyinmeters" "country" "countrycode"
   "county" "datageneralizations" "dateidentified" "day" "decimallatitude"
   "decimallongitude" "disposition" "earliestageorloweststage"
   "earliesteonorlowesteonothem" "earliestepochorlowestseries"
   "earliesteraorlowesterathem" "earliestperiodorlowestsystem" "enddayofyear"
   "establishmentmeans" "eventattributes" "eventdate" "eventid" "eventremarks"
   "eventtime" "fieldnotes" "fieldnumber" "footprintspatialfit" "footprintwkt"
   "formation" "geodeticdatum" "geologicalcontextid" "georeferenceprotocol"
   "georeferenceremarks" "georeferencesources" "georeferenceverificationstatus"
   "georeferencedby" "group" "habitat" "highergeography" "highergeographyid"
   "highestbiostratigraphiczone" "identificationattributes" "identificationid"
   "identificationqualifier" "identificationreferences" "identificationremarks"
   "identifiedby" "individualcount" "individualid" "informationwithheld"
   "institutioncode" "island" "islandgroup" "latestageorhigheststage"
   "latesteonorhighesteonothem" "latestepochorhighestseries"
   "latesteraorhighesterathem" "latestperiodorhighestsystem" "lifestage"
   "lithostratigraphicterms" "locality" "locationattributes" "locationid"
   "locationremarks" "lowestbiostratigraphiczone" "maximumdepthinmeters"
   "maximumdistanceabovesurfaceinmeters" "maximumelevationinmeters"
   "measurementaccuracy" "measurementdeterminedby" "measurementdetermineddate"
   "measurementid" "measurementmethod" "measurementremarks" "measurementtype"
   "measurementunit" "measurementvalue" "member" "minimumdepthinmeters"
   "minimumdistanceabovesurfaceinmeters" "minimumelevationinmeters" "month"
   "occurrenceattributes" "occurrencedetails" "occurrenceid" "occurrenceremarks"
   "othercatalognumbers" "pointradiusspatialfit" "preparations"
   "previousidentifications" "recordnumber" "recordedby" "relatedresourceid"
   "relationshipaccordingto" "relationshipestablisheddate"
   "relationshipofresource" "relationshipremarks" "reproductivecondition"
   "resourceid" "resourcerelationshipid" "samplingprotocol" "sex"
   "startdayofyear" "stateprovince" "taxonattributes" "typestatus"
   "verbatimcoordinatesystem" "verbatimcoordinates" "verbatimdepth"
   "verbatimelevation" "verbatimeventdate" "verbatimlatitude"
   "verbatimlocality" "verbatimlongitude" "waterbody" "year" "footprintsrs"
   "georeferenceddate" "identificationverificationstatus" "institutionid"
   "locationaccordingto" "municipality" "occurrencestatus"
   "ownerinstitutioncode" "samplingeffort" "verbatimsrs" "locationaccordingto7"
   "taxonid" "taxonconceptid" "datasetid" "datasetname" "source" "modified"
   "accessrights" "rights" "rightsholder" "language" "higherclassification"
   "kingdom" "phylum" "classs" "order" "family" "genus" "subgenus"
   "specificepithet" "infraspecificepithet" "scientificname" "scientificnameid"
   "vernacularname" "taxonrank" "verbatimtaxonrank" "infraspecificmarker"
   "scientificnameauthorship" "nomenclaturalcode" "namepublishedin"
   "namepublishedinid" "taxonomicstatus" "nomenclaturalstatus" "nameaccordingto"
   "nameaccordingtoid" "parentnameusageid" "parentnameusage"
   "originalnameusageid" "originalnameusage" "acceptednameusageid"
   "acceptednameusage" "taxonremarks" "dynamicproperties" "namepublishedinyear"])

;; Ordered vector of harvesting output column names for use in wide Cascalog sources:
(def harvest-fields
  (concat (vec (map kw->field-str resource-fields))
          (map str->cascalog-field base-fields)
          ["?dummy"]))

(def occ-fields
  (into ["?occ-uuid"] harvest-fields))

;; Ordered vector of occ table column names for use in wide Cascalog sources:
(def rec-fields
  (concat ["?occ-id"] (map kw->field-str base-fields) ["?iname" "?icode"]))

;; Ordered vector of column names for the occ table.
(def vertnet-columns
  (concat ["taxloc_uuid" "occ_uuid"] (handle-sql-reserved base-fields) ["iname" "icode"]))

(def vertnet-fields
  (concat ["?taxloc-uuid" "?tax-uuid" "?loc-uuid" "?occ-uuid"] harvest-fields))

(defn prep-harvested
  [harvest-src & {:keys [with-uuid] :or {with-uuid true}}]
  (let [uniques (<- harvest-fields
                    (harvest-src ?line)
                    (split-line ?line :>> harvest-fields)
                    (:distinct true))]
    (if with-uuid
      (<- occ-fields
          (uniques :>> harvest-fields)
          (gen-uuid :> ?occ-uuid))
      uniques)))

(defn tax-query
  "Return generator of unique taxonomy tuples from supplied source of harvest-fields.
   Assumes sounce contains valid ?scientificname."
  [source & {:keys [with-uuid] :or {with-uuid true}}]  
  (let [uniques (<- [?scientificname ?kingdom ?phylum ?classs ?order ?family ?genus]
                    (source :>> occ-fields)
                    (:distinct true))]
    (if with-uuid
      (<- [?uuid ?s ?k ?p ?c ?o ?f ?g]
          (uniques ?s ?k ?p ?c ?o ?f ?g)
          (gen-uuid :> ?uuid))
      uniques)))

(defn loc-query
  "Return generator of unique coordinate tuples from supplied source of
   vertnet-fields. Assumes source contains valid coordinates."
  [source & {:keys [with-uuid] :or {with-uuid true}}]
  (let [sig-figs 7
        uniques (<- [?lat ?lon]
                    (source :>> occ-fields)
                    (cleanup-data sig-figs ?decimallatitude ?decimallongitude
                                  ?coordinateprecision ?year ?month :>
                                  ?lat ?lon _ _ _)
                    (valid-latlon? ?lat ?lon)
                    (:distinct true))]
    (if with-uuid
      (<- [?uuid ?lat ?lon]
          (uniques ?lat ?lon)
          (gen-uuid :> ?uuid))
      uniques)))

(defn taxloc-query
  "Return generator of unique taxonomy locations from supplied source of unique
  taxonomies (via tax-query), unique locations (via loc-query), and occurrence
  source of mol-fields."
  [tax-source loc-source occ-source & {:keys [with-uuid] :or {with-uuid true}}]
  (let [sig-figs 7
        occ (<- [?lat ?lon ?scientificname
                 ?kingdom ?phylum ?classs ?order ?family ?genus]
                (occ-source :>> occ-fields)
                (cleanup-data sig-figs ?decimallatitude ?decimallongitude ?coordinateprecision
                              ?year ?month :> ?lat ?lon _ _ _)
                (valid-latlon? ?lat ?lon))
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

(def harvest-tax-loc-fields
  ["?tax-uuid" "?loc-uuid" "?occ-uuid" "?scientificname" "?kingdom"
   "?phylum" "?classs" "?order" "?family" "?genus" "?lat" "?lon"])

(defn harvest-tax-loc-query
  [tax-src loc-src occ-src & [sig-figs]]
  (let [sig-figs (or sig-figs 7)]
    (<- harvest-tax-loc-fields ;;[?scientificname]
        (tax-src ?tax-uuid ?scientificname ?kingdom ?phylum ?classs ?order ?family ?genus)
        (loc-src ?loc-uuid ?lat ?lon)        
        (occ-src :>> occ-fields)
        (cleanup-data sig-figs ?decimallatitude ?decimallongitude ?coordinateprecision
                      ?year ?month :> ?lat ?lon _ _ _)
        (valid-latlon? ?lat ?lon))))

(defn occ-query
  "Return generator of unique occurrences with a taxloc-id."
  [tax-src loc-src taxloc-src occ-src]
  (let [src (harvest-tax-loc-query tax-src loc-src occ-src)] 
    (<- vertnet-fields
        (src :>> harvest-tax-loc-fields)
        (taxloc-src ?taxloc-uuid ?tax-uuid ?loc-uuid)
        (occ-src :>> occ-fields))))

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
