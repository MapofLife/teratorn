(ns teratorn.common
  "This namespace provides common functions."
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [cascalog.ops :as c]))

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
