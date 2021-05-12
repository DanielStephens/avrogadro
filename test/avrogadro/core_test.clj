(ns avrogadro.core-test
  (:require [clojure.test :refer :all]
            [avrogadro.core :as a]
            [clojure.spec.alpha :as s]
            [abracad.avro :as avro]
            [avrogadro.internal :as i])
  (:import [java.time LocalDate]))

(use-fixtures :each (fn [f] (i/clear-defs) (f)))

(deftest core
  (a/def-schema ::country-code #{:gb :fr})
  (a/def-schema ::address
                (a/keys {:line1 :string
                         :postcode :string
                         :country (a/field ::country-code :default :gb)}))
  (a/def-conformer ::date :int #(.toEpochDay %) #(LocalDate/ofEpochDay %))
  (a/def-schema ::user
                (a/keys {:name :string
                         :middle-names (a/coll-of :string)
                         :age :int
                         :date-of-birth ::date
                         :active :boolean
                         :email (a/field [:nil :string] :default nil)
                         :address (a/field (a/or :nil ::address)
                                           :default nil)
                         :numberwang-winners (a/map-of [:int :long])}))

  (is (= {:name "user"
          :namespace "avrogadro.core_test"
          :type "record"
          :fields [{:name "name" :type "string"}
                   {:name "middle_names" :type {:type "array" :items "string"}}
                   {:name "age" :type "int"}
                   {:name "date_of_birth"
                    :type {:logicalType "date" :type "int"}}
                   {:name "active" :type "boolean"}
                   {:name "email"
                    :type ["null" "string"]
                    :default nil}
                   {:name "address"
                    :type ["null"
                           {:name "address"
                            :type "record"
                            :fields [{:name "line1" :type "string"}
                                     {:name "postcode" :type "string"}
                                     {:name "country"
                                      :type {:name "country_code"
                                             :symbols ["fr" "gb"]
                                             :type "enum"}
                                      :default "gb"}]}]
                    :default nil}
                   {:name "numberwang_winners"
                    :type {:type "map" :values ["int" "long"]}}]}
         (avro/unparse-schema (a/schema ::user))))

  (let [user {:name "Dan"
              :middle-names ["J"]
              :age 25
              :date-of-birth (LocalDate/parse "2019-01-01")
              :active false
              :address {:line1 "not"
                        :postcode "there"
                        :country :fr}
              :numberwang-winners {"my fav" 13
                                   "not bad" Long/MAX_VALUE}}]
    (is (bytes? (a/conform ::user user)))
    (is (= (assoc user :email nil)
           (a/unform (a/conform ::user user))))))
