(ns teratorn.common-test
  "Unit test the teratorn.gbif namespace."
  (:use teratorn.common
        [midje sweet])
  (:require [clojure.string :as s]))

(fact
  (split-line "joe\tblow") => ["joe" "blow"])

(facts
  "Test valid-name?"
  (valid-name? "ants") => true
  (valid-name? "") => false
  (valid-name? nil) => false)

(facts
  "Test valid-latlon?"
  (valid-latlon? 1.2 2.3) => true
  (valid-latlon? "1.2" "2.3") => true
  (valid-latlon? 100 100.) => false
  (valid-latlon? "100" "100.") => false
  (valid-latlon? "100" 1) => false
  (valid-latlon? "100" "-.1") => true)

(facts
  "Test str->num-or-empty-str"
  (str->num-or-empty-str "1.2") => 1.2
  (str->num-or-empty-str "\\N") => "")

(facts
  "Test handle-zeros"
  (handle-zeros "3.") => "3"
  (handle-zeros "3.0") => "3"
  (handle-zeros "3.0000") => "3"
  (handle-zeros "3.00100") => "3.00100"
  (handle-zeros "3") => "3"
  (handle-zeros "3.445480") => "3.445480"
  (handle-zeros "3.1234567890") => "3.1234567890")

(future-fact "Test gen-uuid")
(future-fact "Test quoter")
(future-fact "Test quote-master")

(facts
  "Test round-to"
  (round-to 7 3) => "3"
  (round-to 7 3.1234567890) => "3.1234568"
  (round-to 7 3.0) => "3"
  (round-to 7 3.120) => "3.12"
  (round-to 7 3.1000000) => "3.1"
  (round-to 7 3.10000009) => "3.1000001"
  (round-to 7 300) => "300"
  (round-to 7 300.0) => "300"
  (round-to 7 300.123456789) => "300.1234568"
  (round-to 7 -3) => "-3"
  (round-to 7 -3.1234567890) => "-3.1234568"
  (round-to 7 -3.0) => "-3"
  (round-to 7 -3.120) => "-3.12"
  (round-to 7 -3.1000000) => "-3.1"
  (round-to 7 -3.10000009) => "-3.1000001"
  (round-to 7 -300) => "-300"
  (round-to 7 -300.0) => "-300"
  (round-to 7 -300.123456789) => "-300.1234568")

(fact
  "Test parse-hemisphere"
  (parse-hemisphere "N") => {0 "winter" 1 "spring" 2 "summer" 3 "fall"}
  (parse-hemisphere "S") => {0 "summer" 1 "fall" 2 "winter" 3 "spring"})

(fact
  "Test get-season-idx"
  (get-season-idx 1) => 0
  (get-season-idx 3) => 1
  (get-season-idx 4) => 1
  (get-season-idx 6) => 2
  (get-season-idx 7) => 2)

(fact
  "Test get-season"
  (get-season 1 1) => "0"
  (get-season -1 1) => "6"
  (get-season 1 3) => "1"
  (get-season -1 3) => "7"
  (get-season 1 4) => "1"
  (get-season -1 4) => "7"
  (get-season 1 7) => "2"
  (get-season -1 7) => "4"
  (get-season 1 10) => "3"
  (get-season -1 10) => "5")
