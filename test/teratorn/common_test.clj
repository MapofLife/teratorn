(ns teratorn.common-test
  "Unit test the teratorn.common namespace."
  (:use teratorn.common
        [midje sweet]))

(fact "Check `kw->field-str`."
  (kw->field-str :pubdate) => "?pubdate"
  (kw->field-str "pubdate") => "?pubdate")

(tabular
 (fact "Check valid-latlon? function."   
   (valid-latlon? ?lat ?lon) => ?result)
 ?lat ?lon ?result
 "41.850033" "-87.65005229999997" true
 "90" "180" true
 "-90" "-180" true
 "90" "-180" true
 "-90" "180" true
 "0" "0" true
 "90.0" "180.0" true
 "-90.0" "-180.0" true
 "90.0" "-180.0" true
 "-90.0" "180.0" true
 "0" "0" true
 "0.0" "0.0" true 
 41.850033 -87.65005229999997 true
 "-91" "0" false
 "91" "0" false
 "-181" "0" false
 "181" "0" false
 "asdf" "asdf" false)
