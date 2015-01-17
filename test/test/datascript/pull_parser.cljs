(ns test.datascript.pull-parser
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
    [cemerick.cljs.test :as t]
    [datascript.pull-parser :as pp]))

(deftest test-parse-pattern
  (are [pattern expected] (= expected (pp/parse-pull pattern))
    '[:db/id :foo/bar]
    (pp/PullPattern. [(pp/PullAttrName. :db/id)
                      (pp/PullAttrName. :foo/bar)])
    
    '[(limit :foo 1)]
    (pp/PullPattern. [(pp/PullLimitExpr. (pp/PullAttrName. :foo) 1)])

    '[* (default :foo "bar")]
    (pp/PullPattern. [(pp/PullWildcard. #{:foo})
                      (pp/PullDefaultExpr. (pp/PullAttrName. :foo)
                                           "bar")])))

#_(t/test-ns 'test.datascript.pull-parser)
