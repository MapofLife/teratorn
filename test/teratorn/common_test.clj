(ns teratorn.common-test
  "Unit test the teratorn.common namespace."
  (:use teratorn.common
        [midje sweet]))

(fact "Check `kw->field-str`."
  (kw->field-str :pubdate) => "?pubdate"
  (kw->field-str "pubdate") => "?pubdate")
