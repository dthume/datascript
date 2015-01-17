(ns test.datascript.pull-api
  (:require-macros
    [cemerick.cljs.test :refer [is are deftest testing]])
  (:require
   [cemerick.cljs.test :as t]
   [datascript :as d]))

(defn- create-test-conn
  []
  (let [conn (d/create-conn {:aka    { :db/cardinality :db.cardinality/many }
                             :child  { :db/cardinality :db.cardinality/many
                                       :db/valueType :db.type/ref }
                             :friend { :db/cardinality :db.cardinality/many
                                       :db/valueType :db.type/ref }
                             :enemy  { :db/cardinality :db.cardinality/many
                                       :db/valueType :db.type/ref }
                             :father { :db/valueType :db.type/ref }

                             :part   { :db/valueType :db.type/ref
                                       :db/isComponent true
                                       :db/cardinality :db.cardinality/many }})]
    (d/transact! conn [[:db/add 1 :name  "Ivan"]])
    (d/transact! conn [[:db/add 1 :name  "Petr"]])
    (d/transact! conn [[:db/add 1 :aka   "Devil"]])
    (d/transact! conn [[:db/add 1 :aka   "Tupen"]])
    (d/transact! conn [[:db/add 2 :name  "David"]])
    (d/transact! conn [[:db/add 3 :name  "Thomas"]])
    (d/transact! conn [[:db/add 4 :name  "Lucy"]])
    (d/transact! conn [[:db/add 5 :name  "Elizabeth"]])
    (d/transact! conn [[:db/add 6 :name  "Matthew"]])
    (d/transact! conn [[:db/add 7 :name  "Eunan"]])
    (d/transact! conn [[:db/add 8 :name  "Kerri"]])
    (d/transact! conn [[:db/add 9 :name  "Rebecca"]])
    (d/transact! conn [[:db/add 1 :child 2]])
    (d/transact! conn [[:db/add 1 :child 3]])
    (d/transact! conn [[:db/add 6 :father 3]])

    (d/transact! conn [[:db/add 10 :name  "Part A"]])
    (d/transact! conn [[:db/add 11 :name  "Part A.A"]])
    (d/transact! conn [[:db/add 10 :part 11]])
    (d/transact! conn [[:db/add 12 :name  "Part A.A.A"]])
    (d/transact! conn [[:db/add 11 :part 12]])
    (d/transact! conn [[:db/add 13 :name  "Part A.A.A.A"]])
    (d/transact! conn [[:db/add 12 :part 13]])
    (d/transact! conn [[:db/add 14 :name  "Part A.A.A.B"]])
    (d/transact! conn [[:db/add 12 :part 14]])
    (d/transact! conn [[:db/add 15 :name  "Part A.B"]])
    (d/transact! conn [[:db/add 10 :part 15]])
    (d/transact! conn [[:db/add 16 :name  "Part A.B.A"]])
    (d/transact! conn [[:db/add 15 :part 16]])
    (d/transact! conn [[:db/add 17 :name  "Part A.B.A.A"]])
    (d/transact! conn [[:db/add 16 :part 17]])
    (d/transact! conn [[:db/add 18 :name  "Part A.B.A.B"]])
    (d/transact! conn [[:db/add 16 :part 18]])
    
    conn))

(deftest test-pull-attr-spec
  (let [conn (create-test-conn)]
    (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
           (d/pull @conn '[:name :aka] 1)))

    (is (= {:name "Matthew" :father {:db/id 3} :db/id 6}
           (d/pull @conn '[:name :father :db/id] 6)))

    (is (= [{:name "Petr"} {:name "Elizabeth"}
            {:name "Eunan"} {:name "Rebecca"}]
           (d/pull-many @conn '[:name] [1 5 7 9])))))

(deftest test-pull-reverse-attr-spec
  (let [conn (create-test-conn)]
    (is (= {:name "David" :_child [{:db/id 1}]}
           (d/pull @conn '[:name :_child] 2)))

    (is (= {:name "David" :_child [{:name "Petr"}]}
           (d/pull @conn '[:name {:_child [:name]}] 2)))))

(deftest test-pull-component-attr
  (let [conn  (create-test-conn)
        parts {:name "Part A",
               :part
               [{:name "Part A.A",
                 :part
                 [{:name "Part A.A.A",
                   :part
                   [{:name "Part A.A.A.A"}
                    {:name "Part A.A.A.B"}]}]}
                {:name "Part A.B",
                 :part
                 [{:name "Part A.B.A",
                   :part
                   [{:name "Part A.B.A.A"}
                    {:name "Part A.B.A.B"}]}]}]}]
    (testing "Component entities are expanded recursively"
      (is (= parts (d/pull @conn '[:name :part] 10))))

    (testing "Reverse component references yield a single result"
      (is (= {:name "Part A.A" :_part {:db/id 10}}
             (d/pull @conn [:name :_part] 11))))))

(deftest test-pull-wildcard
  (let [conn (create-test-conn)]
    (is (= {:name "Petr" :aka ["Devil" "Tupen"]
            :child [{:db/id 2} {:db/id 3}]}
           (d/pull @conn '[*] 1)))))

(deftest test-pull-limit
  (let [conn (create-test-conn)]
    (d/transact! conn [[:db/add 4 :friend 5]])
    (d/transact! conn [[:db/add 4 :friend 6]])
    (d/transact! conn [[:db/add 4 :friend 7]])
    (d/transact! conn [[:db/add 4 :friend 8]])

    (dotimes [idx 2000]
      (d/transact! conn [[:db/add 8 :aka (str "aka-" idx)]]))

    (testing "Without an explicit limit, the default is 1000"
      (is (->> (d/pull @conn '[:aka] 8)
               :aka
               count
               (= 1000))))

    (testing "Explicit limit can reduce the default"
      (is (->> (d/pull @conn '[(limit :aka 500)] 8)
               :aka
               count
               (= 500))))

    (testing "Explicit limit can increase the default"
      (is (->> (d/pull @conn '[(limit :aka 1500)] 8)
               :aka
               count
               (= 1500))))

    (testing "A nil limit produces unlimited results"
      (is (->> (d/pull @conn '[(limit :aka nil)] 8)
               :aka
               count
               (= 2000))))

    (testing "Limits can be used as map specification keys"
      (is (= {:name "Lucy"
              :friend [{:name "Elizabeth"} {:name "Matthew"}]}
             (d/pull @conn '[:name {(limit :friend 2) [:name]}] 4))))))

(deftest test-pull-default
  (let [conn (create-test-conn)]
    (testing "Empty results return nil"
      (is (nil? (d/pull @conn '[:foo] 1))))

    (testing "A default can be used to replace nil results"
      (is (= {:foo "bar"}
             (d/pull @conn '[(default :foo "bar")] 1))))))

(deftest test-pull-map
  (let [conn (create-test-conn)]
    (is (= {:name "Petr" :child [{:name "David"}
                                 {:name "Thomas"}]}
           (d/pull @conn '[:name {:child [:name]}] 1)))

    (is (= {:name "Matthew" :father {:name "Thomas"}}
           (d/pull @conn '[:name {:father [:name]}] 6)))

    (is (= {:name "Petr"}
           (d/pull @conn '[:name {:father [:name]}] 1)))))

(deftest test-pull-recursion
  (let [conn    (create-test-conn)
        friends {:name "Lucy"
                 :friend
                 [{:name "Elizabeth"
                   :friend
                   [{:name "Matthew"
                     :friend
                     [{:name "Eunan"
                       :friend
                       [{:name "Kerri"}]}]}]}]}
        enemies {:name "Lucy"
                 :friend
                 [{:name "Elizabeth"
                   :friend
                   [{:name "Matthew"
                     :enemy [{:name "Kerri"}]}]
                   :enemy
                   [{:name "Eunan"
                     :friend [{:name "Kerri"}]
                     :enemy
                     [{:name "Lucy"
                       :friend [{:name "Elizabeth"}]}]}]}]
                 :enemy
                 [{:name "Matthew"
                   :friend
                   [{:name "Eunan"
                     :friend [{:name "Kerri"}]
                     :enemy [{:db/id 4}]}]
                   :enemy [{:name "Kerri"}]}]}]

    (d/transact! conn [[:db/add 4 :friend 5]])
    (d/transact! conn [[:db/add 5 :friend 6]])
    (d/transact! conn [[:db/add 6 :friend 7]])
    (d/transact! conn [[:db/add 7 :friend 8]])

    (d/transact! conn [[:db/add 4 :enemy 6]])
    (d/transact! conn [[:db/add 5 :enemy 7]])
    (d/transact! conn [[:db/add 6 :enemy 8]])
    (d/transact! conn [[:db/add 7 :enemy 4]])

    (testing "Infinite recursion"
      (is (= friends (d/pull @conn '[:name {:friend ...}] 4))))

    (testing "Multiple recursion specs in one pattern"
      (is (= enemies (d/pull @conn '[:name {:friend 2 :enemy 2}] 4))))
    
    (d/transact! conn [[:db/add 8 :friend 4]])

    (testing "Cycles are handled by returning only the :db/id of entities which have been seen before"
      (is (= (update-in friends (take 8 (cycle [:friend 0]))
                        assoc :friend [{:db/id 4}])
             (d/pull @conn '[:name {:friend ...}] 4))))))

(deftest test-deep-recursion
  (let [conn    (create-test-conn)
        start   100
        depth   10000]
    (d/transact! conn [[:db/add start :name (str "Person-" start)]])
    (doseq [idx (range (inc start) depth)]
      (d/transact! conn [[:db/add idx :name (str "Person-" idx)]])
      (d/transact! conn [[:db/add (dec idx) :friend idx]]))

    (is (= (str "Person-" (dec depth))
           (->> (d/pull @conn '[:name {:friend ...}] start)
                (iterate #(get-in % [:friend 0]))
                (drop (dec (- depth start)))
                first
                :name)))))


#_(t/test-ns 'test.datascript.pull)
