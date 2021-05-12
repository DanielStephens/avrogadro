(ns avrogadro.internal
  (:require [abracad.avro :as avro]
            [jackdaw.serdes.avro :refer [+base-schema-type-registry+
                                         SchemaCoercion avro->clj clj->avro]]
            [jackdaw.serdes.avro.confluent :refer [serde]]
            [clojure.string :as str]))

;; STATE ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce ^:private schemas (atom {}))
(defonce ^:private dep-order-schemas (atom []))
(defonce ^:private type-registry (atom +base-schema-type-registry+))

;; GENERAL UTILS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro throw-inline
  [msg & {:as args}]
  `(throw (ex-info ~msg ~args)))

;; AVRO TYPE UTILS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private primitive-type?
  #{:nil :boolean :string :int :long :float :double :bytes})

(def ^:private complex-type?
  #{:enum :array :map :record})

(def ^:private named-type?
  #{:enum :record})

(defn- to-abracad-type
  "abracad uses :null over :nil but I consistently get it wrong, this allows us
  to use :nil and converts for the underlying abracad library"
  [type]
  {:pre [(or (contains? primitive-type? type)
             (contains? complex-type? type))]}
  (if (= :nil type)
    :null
    type))

(defn named-form?
  [form]
  (and (map? form) (named-type? (:type form))))

;; NAMING UTILS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- munge-str
  [s]
  (str/replace s #"\-" "_"))

(defn- unmunge-str
  [s]
  (str/replace s #"_" "-"))

(defn- kw->avro-fqn-split
  [kw]
  {:pre [(qualified-keyword? kw)]}
  {:namespace (munge-str (namespace kw))
   :name (munge-str (name kw))})

(defn- kw->avro-fqn
  [kw]
  (let [{:keys [name namespace]} (kw->avro-fqn-split kw)]
    (str namespace "." name)))

(defn- avro-fqn->kw
  [avro-fqn]
  (let [parts (map unmunge-str (str/split avro-fqn #"\."))
        ns-parts (drop-last parts)
        n-part (last parts)]
    (keyword (str/join "." ns-parts) n-part)))

;; SCHEMA FN DECLARATIONS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare mk-enum mk-or)

;; FORM RESOLUTION ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn parse-special-forms
  [form]
  (cond
    (contains? primitive-type? form) (to-abracad-type form)
    (set? form) (mk-enum form nil)
    (vector? form) (mk-or form)
    :else form))

(defn resolve-form
  [form]
  (if (qualified-keyword? form)
    (if-let [f (get @schemas form)]
      (if (and (map? f) (contains? f :logicalType))
        {:internal-ref form :avro-ref f :form f}
        {:internal-ref form :avro-ref (kw->avro-fqn form) :form f})
      (throw-inline (str "Unable to resolve schema" :key form)))
    (let [f (parse-special-forms form)]
      {:internal-ref f :avro-ref f :form f})))

;; SCHEMA FNS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn mk-enum
  [vals opts]
  (when-not (every? simple-keyword? vals)
    (throw-inline "Enum values must be simple keywords."
                  :bad-vals (remove simple-keyword? vals)))
  (merge opts
         {:type :enum
          :symbols (vec (set vals))}))

(defn mk-or
  [forms]
  (->> forms
       (map (comp :avro-ref resolve-form))
       (distinct)
       (mapcat (fn [f] (if (vector? f) f [f])))
       (vec)))

(defn mk-keys
  [key-form-map opts]
  (merge opts
         {:type :record
          :fields (mapv (fn [[k v]]
                          (when-not (simple-keyword? k) (throw (ex-info "Key must be a simple keyword." {})))
                          (if (and (map? v)
                                   (= ::field (:type v)))
                            (merge (:opts v)
                                   {:type (:form v)
                                    :name (name k)})
                            {:name (name k) :type (:avro-ref (resolve-form v))}))
                        key-form-map)}))

(defn mk-merge
  [key-forms opts]
  (let [fs (->> key-forms
                (map (comp :form resolve-form))
                (map (fn [arg f] (if (and (map? f)
                                          (= :record (:type f)))
                                   (select-keys f [:type :fields :default])
                                   (throw-inline "Forms must be or refer to forms made with 'keys'"
                                                 :bad-arg arg :bad-form f)))
                     key-forms)
                (doall))
        fields (->> fs
                    (mapcat :fields)
                    (reduce (fn [agg field]
                              (let [n (:name field)
                                    existing-field (get agg n)
                                    fields-differ? (not= field existing-field)]
                                (if (and existing-field fields-differ?)
                                  (throw-inline "Fields by the same name cannot be merged as they differ."
                                                :field-name n
                                                :field-a existing-field
                                                :field-b field)
                                  (assoc agg n field))))
                            {})
                    vals
                    vec)
        default (->> fs
                     (map :default)
                     (reduce (fn
                               ([] ::no-default)
                               ([agg d] (merge agg d)))))]
    (cond-> (merge opts
                   {:type :record
                    :fields fields})
            (not= ::no-default default) (assoc :default default))))

(defn mk-map-of
  [v-form opts]
  (merge opts
         {:type :map
          :values (:avro-ref (resolve-form v-form))}))

(defn mk-coll-of
  [form opts]
  (merge opts
         {:type :array
          :items (:avro-ref (resolve-form form))}))

(defn mk-fixed
  [size opts]
  (merge opts
         {:type :fixed
          :size size}))

;; CONFORMED SCHEMA FN ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn mk-conformed
  [form conformer conformer-fields]
  (let [f (:form (resolve-form form))
        f-type (if (map? f) (:type f) f)
        c-form (:form (resolve-form conformer))]
    (assert (= f-type (:type c-form))
            "The type of form must match the type of the conformer")
    (if (map? f)
      (merge (dissoc f :name :namespace)
             c-form
             conformer-fields)
      (merge c-form conformer-fields))))

;; KEYS FIELD FNS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn mk-field
  [form opts]
  {:type ::field
   :form (:avro-ref (resolve-form form))
   :opts opts})

(defn mk-nilable-field
  [form]
  {:type ::field
   :form (mk-or [:nil form])
   :opts {:default nil}})

;; NAMING SCHEMA FNS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn mk-named
  [k form]
  {:pre [(qualified-keyword? k)]}
  (if (named-form? form)
    (merge form (kw->avro-fqn-split k))
    form))

;; CONFORMERS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord ConformerType [logical-type base-type base-conf f unf]
  SchemaCoercion
  (avro->clj [_ x] (try (unf (avro->clj base-conf x))
                        (catch Exception e
                          (throw (ex-info (format "avro data could not be converted from %s to %s" base-type logical-type)
                                          {:data x}
                                          e)))))
  (clj->avro [_ x path] (try (clj->avro base-conf (f x) path)
                             (catch Exception e
                               (throw (ex-info (format "clj data could not be converted from %s to %s" logical-type base-type)
                                               {:path path, :data x}
                                               e)))))
  (match-avro? [this x] (try (do (avro->clj this x) true)
                             (catch Exception _ false)))
  (match-clj? [this x] (try (do (clj->avro this x nil) true)
                            (catch Exception _ false))))

(defn mk-conformer
  [logical-type base-type f unf]
  {:pre [(qualified-keyword? logical-type)
         (simple-keyword? base-type)]}
  (let [str-base-type (name (to-abracad-type base-type))
        base-conf (get @type-registry {:type str-base-type})
        _ (when-not base-conf (throw-inline "The base-type was unknown." :base-type base-type))

        conf (fn [a b]
               (->ConformerType logical-type base-type (base-conf a b) f unf))
        conf-id {:type str-base-type
                 :logical-type (name logical-type)}

        t {:type base-type
           :logicalType (keyword (name logical-type))}]
    {:conformer-id conf-id
     :conformer conf
     :internal-ref logical-type
     :form t}))

;; MUTATING DEFS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn def-conformer
  [logical-type base-type f unf]
  (let [{:keys [conformer-id conformer
                internal-ref form]} (mk-conformer logical-type base-type f unf)]
    (swap! type-registry assoc conformer-id conformer)
    (swap! schemas assoc internal-ref form)
    (swap! dep-order-schemas conj internal-ref)
    internal-ref))

(defn def-schema
  [k form]
  {:pre [(qualified-keyword? k)]}
  (let [f (mk-named k (:form (resolve-form form)))]
    (swap! schemas assoc k f)
    (swap! dep-order-schemas conj k)
    k))

;; SCHEMA USE FNS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private mock-client
  (jackdaw.serdes.avro.schema-registry/mock-client))

(defn- mk-serde
  [schema schema-registry-url opts]
  (let [tr @type-registry]
    (serde schema-registry-url
           (str schema)
           false
           (merge {:type-registry tr}
                  opts))))

(defn schema
  [k]
  {:pre [(qualified-keyword? k)]}
  (let [s @schemas
        deps (->> @dep-order-schemas
                  (take-while #(not= k %))
                  (map (fn [dep-k] (get s dep-k)))
                  (filter named-form?))
        form (get s k)]
    (when-not form
      (throw-inline (str "Unable to resolve schema" :key k)))
    (apply avro/parse-schema (concat deps
                                     [form]))))

(def invalid-kw :avrogadro/invalid)

(defn conform
  ([k topic data schema-registry-url opts]
   {:pre [(qualified-keyword? k)]}
   (let [schema (schema k)
         serde (mk-serde schema schema-registry-url opts)]
     (try (.serialize (.serializer serde) topic data)
          (catch Exception _ invalid-kw))))
  ([k data]
   (let [schema (schema k)
         serde (mk-serde schema
                         "http://mock:9092" {:schema-registry-client mock-client})]
     (try (.serialize (.serializer serde) nil data)
          (catch Exception _ invalid-kw)))))

(defn invalid?
  [k data]
  (identical? invalid-kw (conform k data)))

(defn valid?
  [k data]
  (not (invalid? k data)))

(defn explain
  [k data]
  (when (invalid? k data)
    "Some reason"))

(defn unform
  ([topic data schema-registry-url opts]
   (let [serde (mk-serde (avro/parse-schema :string)
                         schema-registry-url opts)]
     (try (.deserialize (.deserializer serde) topic data)
          (catch Exception _ ::invalid))))
  ([data]
   (let [serde (mk-serde (avro/parse-schema :string)
                         "http://mock:9092" {:schema-registry-client mock-client})]
     (try (.deserialize (.deserializer serde) nil data)
          (catch Exception _ ::invalid)))))

(defn get-schemas []
  @schemas)

(defn clear-defs []
  (reset! schemas {})
  (reset! dep-order-schemas [])
  (reset! type-registry +base-schema-type-registry+))
