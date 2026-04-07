pub mod parser;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, Map};
use uuid::Uuid;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Document {
    pub id: Uuid,
    pub data: Value,
}

impl Document {
    pub fn new(data: Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            data,
        }
    }

    pub fn to_json_flat(&self) -> Value {
        let mut map = Map::new();
        map.insert("id".to_string(), json!(self.id));
        if let Some(obj) = self.data.as_object() {
            for (k, v) in obj {
                map.insert(k.clone(), v.clone());
            }
        }
        Value::Object(map)
    }
}

pub struct Collection {
    pub name: String,
    path: PathBuf,
}

impl Collection {
    pub fn new(name: &str, base_path: &Path) -> Result<Self> {
        let path = base_path.join(name);
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        Ok(Self {
            name: name.to_string(),
            path,
        })
    }

    pub fn insert(&self, doc: Document) -> Result<Uuid> {
        let file_path = self.path.join(format!("{}.json", doc.id));
        let content = serde_json::to_string_pretty(&doc)?;
        fs::write(file_path, content)?;
        Ok(doc.id)
    }

    pub fn find_all(&self) -> Result<Vec<Document>> {
        let mut docs = Vec::new();
        if !self.path.exists() {
            return Ok(docs);
        }
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) == Some("json") {
                let content = fs::read_to_string(entry.path())?;
                let doc: Document = serde_json::from_str(&content)?;
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    pub fn find_by_id(&self, id: Uuid) -> Result<Option<Document>> {
        let file_path = self.path.join(format!("{}.json", id));
        if !file_path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(file_path)?;
        let doc: Document = serde_json::from_str(&content)?;
        Ok(Some(doc))
    }

    pub fn find<F>(&self, mut filter: F) -> Result<Vec<Document>>
    where
        F: FnMut(&Document) -> bool,
    {
        let mut results = Vec::new();
        for doc in self.find_all()? {
            if filter(&doc) {
                results.push(doc);
            }
        }
        Ok(results)
    }

    pub fn update(&self, id: Uuid, new_data: Value) -> Result<bool> {
        let file_path = self.path.join(format!("{}.json", id));
        if !file_path.exists() {
            return Ok(false);
        }
        let mut doc: Document = serde_json::from_str(&fs::read_to_string(&file_path)?)?;
        doc.data = new_data;
        let content = serde_json::to_string_pretty(&doc)?;
        fs::write(file_path, content)?;
        Ok(true)
    }

    pub fn delete(&self, id: Uuid) -> Result<bool> {
        let file_path = self.path.join(format!("{}.json", id));
        if file_path.exists() {
            fs::remove_file(file_path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

pub struct Database {
    pub path: PathBuf,
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
        let base_path = PathBuf::from(path);
        if !base_path.exists() {
            fs::create_dir_all(&base_path)?;
        }
        Ok(Self { path: base_path })
    }

    pub fn get_collection(&self, name: &str) -> Result<Collection> {
        Collection::new(name, &self.path)
    }

    pub fn drop_collection(&self, name: &str) -> Result<bool> {
        let path = self.path.join(name);
        if path.exists() {
            fs::remove_dir_all(path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn execute_ast(&self, query: parser::Query) -> Result<String> {
        match query {
            parser::Query::Insert { collection, data } => {
                let coll = self.get_collection(&collection)?;
                if let Some(array) = data.as_array() {
                    let mut ids = Vec::new();
                    for item in array {
                        let id = coll.insert(Document::new(item.clone()))?;
                        ids.push(id.to_string());
                    }
                    Ok(format!("{} documento(s) inserido(s). IDs: {:?}", ids.len(), ids))
                } else {
                    let id = coll.insert(Document::new(data))?;
                    Ok(format!("Documento inserido com ID: {}", id))
                }
            }
            parser::Query::Select { fields, collection, join, filter, group_by, order_by, limit } => {
                let coll = self.get_collection(&collection.name)?;
                let base_docs = if let Some(ref f) = filter {
                    coll.find(|d| matches_filter(self, d, f).unwrap_or(false))?
                } else {
                    coll.find_all()?
                };

                let left_alias = collection.alias.clone().unwrap_or(collection.name.clone());
                let mut joined_raw = Vec::new();

                if let Some(join_info) = join {
                    let right_coll = self.get_collection(&join_info.collection.name)?;
                    let right_docs = right_coll.find_all()?;
                    
                    let right_alias = join_info.collection.alias.clone().unwrap_or(join_info.collection.name.clone());

                    let left_field = strip_table_prefix(&join_info.left_field, &collection.name, collection.alias.as_deref());
                    let right_field = strip_table_prefix(&join_info.right_field, &join_info.collection.name, join_info.collection.alias.as_deref());

                    match join_info.join_type {
                        parser::JoinType::Inner => {
                            for left in base_docs {
                                for right in &right_docs {
                                    if left.data[left_field] == right.data[right_field] {
                                        joined_raw.push(merge_docs_flat(&collection, &left, &join_info.collection, right));
                                    }
                                }
                            }
                        }
                        parser::JoinType::Left => {
                            for left in base_docs {
                                let mut matched = false;
                                for right in &right_docs {
                                    if left.data[left_field] == right.data[right_field] {
                                        joined_raw.push(merge_docs_flat(&collection, &left, &join_info.collection, right));
                                        matched = true;
                                    }
                                }
                                if !matched {
                                    joined_raw.push(merge_docs_flat(&collection, &left, &join_info.collection, &Document::new(json!({}))));
                                }
                            }
                        }
                        parser::JoinType::Right => {
                            for right in right_docs {
                                let mut matched = false;
                                for left in &base_docs {
                                    if left.data[left_field] == right.data[right_field] {
                                        joined_raw.push(merge_docs_flat(&collection, left, &join_info.collection, &right));
                                        matched = true;
                                    }
                                }
                                if !matched {
                                    joined_raw.push(merge_docs_flat(&collection, &Document::new(json!({})), &join_info.collection, &right));
                                }
                            }
                        }
                    }
                } else {
                    for doc in base_docs {
                        joined_raw.push(doc.to_json_flat());
                    }
                }

                // Determinar se e consulta agrupada
                let has_aggr = match &fields {
                    parser::SelectFields::All => false,
                    parser::SelectFields::Specific(fs) => fs.iter().any(|f| f.function.is_some()),
                };

                let mut buckets: Vec<Vec<Value>> = Vec::new();

                if let Some(gb_fields) = group_by {
                    // Hashmap baseado nas strings serializadas dos valores dos campos
                    let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
                    for row in joined_raw {
                        if let Some(obj) = row.as_object() {
                            let mut key_parts = Vec::new();
                            for g_field in &gb_fields {
                                let val = resolve_scoped_field(g_field, obj, &left_alias, "");
                                key_parts.push(val.to_string());
                            }
                            let key = key_parts.join("||");
                            groups.entry(key).or_insert_with(Vec::new).push(row);
                        }
                    }
                    for (_, bucket) in groups {
                        buckets.push(bucket);
                    }
                } else if has_aggr {
                    // Sem group by, mas com MAX/COUNT -> trata tudo como um grande bucket unico
                    buckets.push(joined_raw);
                } else {
                    // Sem Agrupamento, cada row e um bucket de tamanho 1
                    for row in joined_raw {
                        buckets.push(vec![row]);
                    }
                }

                let mut final_results = Vec::new();
                for bucket in buckets {
                    final_results.push(project_fields_aggr(&fields, &bucket, &left_alias, ""));
                }

                // ORDER BY
                if let Some((field, dir)) = order_by {
                    final_results.sort_by(|a, b| {
                        let va = a.get(&field).unwrap_or(&Value::Null);
                        let vb = b.get(&field).unwrap_or(&Value::Null);
                        let res = compare_values_ord(va, vb);
                        match dir {
                            parser::SortDir::Asc => res,
                            parser::SortDir::Desc => res.reverse(),
                        }
                    });
                }

                // LIMIT
                if let Some(l) = limit {
                    final_results.truncate(l);
                }

                Ok(serde_json::to_string_pretty(&final_results)?)
            }
            parser::Query::Delete { collection, filter } => {
                let coll = self.get_collection(&collection)?;
                let targets = coll.find(|d| matches_filter(self, d, &filter).unwrap_or(false))?;
                let count = targets.len();
                for doc in targets {
                    coll.delete(doc.id)?;
                }
                Ok(format!("{} documento(s) removido(s).", count))
            }
            parser::Query::Update { collection, updates, filter } => {
                let coll = self.get_collection(&collection)?;
                let targets = coll.find(|d| matches_filter(self, d, &filter).unwrap_or(false))?;
                let count = targets.len();
                
                let update_map = updates.as_object().ok_or_else(|| {
                    anyhow::anyhow!("O SET do UPDATE deve ser um objeto JSON")
                })?;

                for doc in targets {
                    let mut current_data = doc.data.clone();
                    if let Some(obj) = current_data.as_object_mut() {
                        for (k, v) in update_map {
                            if k != "id" {
                                obj.insert(k.clone(), v.clone());
                            }
                        }
                    }
                    coll.update(doc.id, current_data)?;
                }
                Ok(format!("{} documento(s) atualizado(s).", count))
            }
            parser::Query::DropCollection { collection } => {
                if self.drop_collection(&collection)? {
                    Ok(format!("Coleção '{}' removida com sucesso.", collection))
                } else {
                    Ok(format!("Coleção '{}' não encontrada.", collection))
                }
            }
            parser::Query::ShowCollections => {
                let mut collections = Vec::new();
                for entry in std::fs::read_dir(&self.path)? {
                    let entry = entry?;
                    if entry.path().is_dir() {
                        collections.push(entry.file_name().to_string_lossy().to_string());
                    }
                }
                Ok(format!("Coleções: {:?}", collections))
            }
        }
    }

    pub fn execute(&self, query_str: &str) -> Result<String> {
        let (_, query) = parser::parse_query(query_str).map_err(|e| {
            anyhow::anyhow!("Erro de sintaxe OxiQL: {:?}", e)
        })?;
        self.execute_ast(query)
    }
}

fn strip_table_prefix<'a>(field: &'a str, table_name: &str, table_alias: Option<&str>) -> &'a str {
    if field.starts_with(table_name) && field[table_name.len()..].starts_with('.') {
        &field[table_name.len() + 1..]
    } else if let Some(alias) = table_alias {
        if field.starts_with(alias) && field[alias.len()..].starts_with('.') {
            &field[alias.len() + 1..]
        } else {
            field
        }
    } else {
        field
    }
}

pub fn matches_filter(db: &Database, doc: &Document, filter: &parser::Filter) -> Result<bool> {
    let flat_doc = doc.to_json_flat();
    match filter {
        parser::Filter::Standard { field, operator, value } => {
            let clean_field = if let Some(pos) = field.find('.') {
                &field[pos+1..]
            } else {
                field
            };
            
            let doc_val = if flat_doc.as_object().unwrap().contains_key(clean_field) {
                &flat_doc[clean_field]
            } else {
                &flat_doc[field]
            };

            Ok(compare_values(doc_val, *operator, value))
        }
        parser::Filter::Json(filter_val) => {
            if let (Some(filter_obj), Some(doc_obj)) = (filter_val.as_object(), flat_doc.as_object()) {
                for (k, v) in filter_obj {
                    if doc_obj.get(k) != Some(v) {
                        return Ok(false);
                    }
                }
                Ok(true)
            } else {
                Ok(flat_doc == *filter_val)
            }
        }
        parser::Filter::And(filters) => {
            for f in filters {
                if !matches_filter(db, doc, f)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        parser::Filter::Or(filters) => {
            for f in filters {
                if matches_filter(db, doc, f)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        parser::Filter::InSubquery { field, query } => {
            let res_json = db.execute_ast(*query.clone())?;
            let values: Vec<Value> = serde_json::from_str(&res_json)?;
            
            let clean_field = if let Some(pos) = field.find('.') {
                &field[pos+1..]
            } else {
                field
            };
            let doc_val = if flat_doc.as_object().unwrap().contains_key(clean_field) {
                &flat_doc[clean_field]
            } else {
                &flat_doc[field]
            };
            
            for row in values {
                if let Some(obj) = row.as_object() {
                    if obj.values().any(|v| v == doc_val) {
                        return Ok(true);
                    }
                } else if row == *doc_val {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        parser::Filter::ScalarSubquery { field, operator, query } => {
            let res_json = db.execute_ast(*query.clone())?;
            let values: Vec<Value> = serde_json::from_str(&res_json)?;
            
            if let Some(first_row) = values.get(0) {
                let scalar_val = if let Some(obj) = first_row.as_object() {
                    obj.values().next().unwrap_or(&Value::Null)
                } else {
                    first_row
                };
                
                let clean_field = if let Some(pos) = field.find('.') {
                    &field[pos+1..]
                } else {
                    field
                };
                let doc_val = if flat_doc.as_object().unwrap().contains_key(clean_field) {
                    &flat_doc[clean_field]
                } else {
                    &flat_doc[field]
                };

                Ok(compare_values(doc_val, *operator, scalar_val))
            } else {
                Ok(false)
            }
        }
    }
}

fn compare_values(left: &Value, op: parser::Operator, right: &Value) -> bool {
    match op {
        parser::Operator::Eq => left == right,
        parser::Operator::Ne => left != right,
        parser::Operator::Gt => {
            compare_values_ord(left, right) == std::cmp::Ordering::Greater
        }
        parser::Operator::Lt => {
            compare_values_ord(left, right) == std::cmp::Ordering::Less
        }
        parser::Operator::Gte => {
            let res = compare_values_ord(left, right);
            res == std::cmp::Ordering::Greater || res == std::cmp::Ordering::Equal
        }
        parser::Operator::Lte => {
            let res = compare_values_ord(left, right);
            res == std::cmp::Ordering::Less || res == std::cmp::Ordering::Equal
        }
    }
}

fn compare_values_ord(a: &Value, b: &Value) -> std::cmp::Ordering {
    if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
        af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
    } else if let (Some(as_str), Some(bs_str)) = (a.as_str(), b.as_str()) {
        as_str.cmp(bs_str)
    } else {
        a.to_string().cmp(&b.to_string())
    }
}

fn merge_docs_flat(left_info: &parser::TableInfo, left: &Document, right_info: &parser::TableInfo, right: &Document) -> Value {
    let mut merged = Map::new();
    let left_flat = left.to_json_flat();
    let right_flat = right.to_json_flat();

    let l_prefix = left_info.alias.as_ref().unwrap_or(&left_info.name);
    let r_prefix = right_info.alias.as_ref().unwrap_or(&right_info.name);

    if let Some(l_obj) = left_flat.as_object() {
        for (k, v) in l_obj {
            merged.insert(format!("{}.{}", l_prefix, k), v.clone());
        }
    }

    if let Some(r_obj) = right_flat.as_object() {
        for (k, v) in r_obj {
            merged.insert(format!("{}.{}", r_prefix, k), v.clone());
        }
    }

    Value::Object(merged)
}

fn resolve_scoped_field<'a>(field_name: &str, obj: &'a Map<String, Value>, left_alias: &str, right_alias: &str) -> &'a Value {
    if let Some(val) = obj.get(field_name) {
        return val;
    }
    let clean_field = if let Some(pos) = field_name.find('.') {
        let prefix = &field_name[..pos];
        if prefix == left_alias || prefix == right_alias {
            &field_name[pos+1..]
        } else {
            field_name
        }
    } else {
        field_name
    };
    obj.get(clean_field).unwrap_or(&Value::Null)
}

fn project_fields_aggr(fields: &parser::SelectFields, bucket: &Vec<Value>, left_alias: &str, right_alias: &str) -> Value {
    match fields {
        parser::SelectFields::All => {
            // Em SELECT * SEM GROUP BY, bucket tem tam 1
            if bucket.is_empty() { return Value::Null; }
            bucket[0].clone()
        },
        parser::SelectFields::Specific(fields) => {
            let mut projected = Map::new();
            if bucket.is_empty() { return Value::Object(projected); }

            for f in fields {
                let out_name = f.alias.as_ref().unwrap_or(&f.name);
                
                if let Some(aggr) = f.function {
                    let mut vals = Vec::new();
                    for row in bucket {
                        if let Some(obj) = row.as_object() {
                            if f.name == "*" {
                                vals.push(Value::Null); // Apenas preenche
                            } else {
                                let v = resolve_scoped_field(&f.name, obj, left_alias, right_alias);
                                if !v.is_null() { vals.push(v.clone()); }
                            }
                        }
                    }

                    match aggr {
                        parser::AggrFunc::Count => {
                            projected.insert(out_name.clone(), json!(vals.len()));
                        }
                        parser::AggrFunc::Sum => {
                            let mut sum = 0.0;
                            for v in vals { if let Some(nf) = v.as_f64() { sum += nf; } }
                            projected.insert(out_name.clone(), json!(sum));
                        }
                        parser::AggrFunc::Avg => {
                            let mut sum = 0.0;
                            let mut c = 0.0;
                            for v in &vals { if let Some(nf) = v.as_f64() { sum += nf; c += 1.0; } }
                            let avg = if c > 0.0 { sum / c } else { 0.0 };
                            projected.insert(out_name.clone(), json!(avg));
                        }
                        parser::AggrFunc::Min => {
                            let min = vals.into_iter().min_by(compare_values_ord).unwrap_or(Value::Null);
                            projected.insert(out_name.clone(), min);
                        }
                        parser::AggrFunc::Max => {
                            let max = vals.into_iter().max_by(compare_values_ord).unwrap_or(Value::Null);
                            projected.insert(out_name.clone(), max);
                        }
                    }

                } else {
                    // Sem função agregada, assume valor da primeira row do bucket (padrão MYSQL p/ GROUP BY)
                    if let Some(obj) = bucket[0].as_object() {
                        let v = resolve_scoped_field(&f.name, obj, left_alias, right_alias);
                        projected.insert(out_name.clone(), v.clone());
                    }
                }
            }
            Value::Object(projected)
        }
    }
}