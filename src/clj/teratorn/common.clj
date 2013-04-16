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
    (let [parsed-str (parse-double s)]
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
  (let [line (if (.endsWith line "\t") (str line " ") line)]
    (map #(.trim %) (vec (.split line "\t")))))

(defn gen-uuid
  "Return a randomly generated UUID string."
  [& x]
  (str (java.util.UUID/randomUUID)))

(defn parse-double
  "Wrapper for `java.lang.Double/parseDouble`, suitable for use with `map`.

   Usage:
     (parse-double \"-.1\")
     ;=> -0.1
     (map parse-double [\"100\" \"-.1\"])
     ;=> (100 -0.1)"
  [s]
  (java.lang.Double/parseDouble s))

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
                        (map parse-double [lat lon]))
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
    (s/replace x "'" "''")
    (let [val (format "\"%s\"" (.replace x "\"" "\"\"") "\"" "\"\"")
          val (s/replace val "'" "''")]
      val)))
  
(defn quotemaster
  "Maps quoter function over supplied vals."
  [& vals]
  (vec (map quoter vals)))

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
