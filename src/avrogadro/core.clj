(ns avrogadro.core
  (:refer-clojure :exclude [or keys merge])
  (:require [avrogadro.internal :as i]))

(alias 'c 'clojure.core)

(defn enum
  [vals & {:as opts}]
  (i/mk-enum vals opts))

(defn or
  [& forms]
  (i/mk-or forms))

(defn keys
  [key-form-map & {:as opts}]
  (i/mk-keys key-form-map opts))

(defn merge
  [& key-forms]
  (i/mk-merge key-forms nil))

(defn map-of
  [v-form & {:as opts}]
  (i/mk-map-of v-form opts))

(defn coll-of
  [form & {:as opts}]
  (i/mk-coll-of form opts))

(defn fixed
  [size & {:as opts}]
  (i/mk-fixed size opts))

(defn conformed
  [form conformer & {:as conformer-fields}]
  (i/mk-conformed form conformer conformer-fields))

(defn field
  [form & {:as opts}]
  (i/mk-field form opts))

(defn nilable
  [form]
  (i/mk-nilable-field form))

(defn named
  [k form]
  (i/mk-named k form))

(defn def-conformer
  [logical-type base-type f unf]
  (i/def-conformer logical-type base-type f unf))

(defn def-schema
  [k form]
  (i/def-schema k form))

(defn schema
  [k]
  (i/schema k))

(defn conform
  ([k topic data schema-registry-url opts]
   (i/conform k topic data schema-registry-url opts))
  ([k data]
   (i/conform k data)))

(defn unform
  ([topic data schema-registry-url opts]
   (i/unform topic data schema-registry-url opts))
  ([data]
   (i/unform data)))

(defn invalid?
  [k data]
  (i/invalid? k data))

(defn valid?
  [k data]
  (i/valid? k data))

(defn explain
  [k data]
  (i/explain k data))
