use serde_json::Value;
use nom::{
    IResult,
    bytes::complete::{tag, tag_no_case, take_while1, take_while},
    character::complete::{multispace0, multispace1, char, digit1},
    combinator::{opt, verify, map_res},
    branch::alt,
    multi::separated_list1,
    sequence::{delimited, tuple},
    Parser,
    error::Error,
};

#[derive(Debug, PartialEq, Clone)]
pub enum Query {
    Insert { collection: String, data: Value },
    Select { 
        fields: SelectFields,
        collection: TableInfo, 
        join: Option<JoinInfo>,
        filter: Option<Filter>,
        order_by: Option<(String, SortDir)>,
        limit: Option<usize>,
    },
    Update { collection: String, updates: Value, filter: Filter },
    Delete { collection: String, filter: Filter },
    DropCollection { collection: String },
    ShowCollections,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectField {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct TableInfo {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectFields {
    All,
    Specific(Vec<SelectField>),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinInfo {
    pub join_type: JoinType,
    pub collection: TableInfo,
    pub left_field: String,
    pub right_field: String,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Operator {
    Eq,
    Ne, // !=
    Gt, // >
    Lt, // <
    Gte, // >=
    Lte, // <=
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Filter {
    Standard { field: String, operator: Operator, value: Value },
    Json(Value),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    InSubquery { field: String, query: Box<Query> },
    ScalarSubquery { field: String, operator: Operator, query: Box<Query> },
}

type Res<'a, T> = IResult<&'a str, T, Error<&'a str>>;

fn is_reserved_keyword(s: &str) -> bool {
    let s = s.to_uppercase();
    matches!(
        s.as_str(),
        "SELECT" | "FROM" | "WHERE" | "JOIN" | "ON" | "AS" | "IN" | "AND" | "OR" | 
        "INNER" | "LEFT" | "RIGHT" | "INSERT" | "UPDATE" | "DELETE" | "SET" | 
        "DROP" | "SHOW" | "COLLECTIONS" | "COLLECTION" | "ORDER" | "BY" | "LIMIT" |
        "ASC" | "DESC"
    )
}

fn identifier(input: &str) -> Res<'_, String> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.')
        .map(|s: &str| s.to_string())
        .parse(input)
}

fn alias_identifier(input: &str) -> Res<'_, String> {
    verify(identifier, |s: &str| !is_reserved_keyword(s)).parse(input)
}

pub fn parse_query(input: &str) -> Res<'_, Query> {
    let (input, _) = multispace0::<&str, Error<&str>>.parse(input)?;
    
    // INSERT INTO
    if let Ok((input, _)) = tag_no_case::<&str, &str, Error<&str>>("INSERT INTO").parse(input) {
        let (input, _) = multispace1::<&str, Error<&str>>.parse(input)?;
        let (input, coll) = identifier(input)?;
        let (input, _) = multispace1::<&str, Error<&str>>.parse(input)?;
        let (input, data) = json_value_to_end(input)?;
        return Ok((input, Query::Insert { collection: coll, data }));
    }
    
    // SELECT
    if let Ok((input, _)) = tag_no_case::<&str, &str, Error<&str>>("SELECT").parse(input) {
        let (input, _) = multispace1::<&str, Error<&str>>.parse(input)?;
        
        let (input, fields) = alt((
            tag::<&str, &str, Error<&str>>("*").map(|_| SelectFields::All),
            separated_list1(
                (multispace0::<&str, Error<&str>>, char::<&str, Error<&str>>(','), multispace0::<&str, Error<&str>>),
                parse_select_field
            ).map(SelectFields::Specific)
        )).parse(input)?;

        let (input, _) = (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("FROM"), multispace1::<&str, Error<&str>>).parse(input)?;
        let (input, coll) = parse_table_info(input)?;
        
        let (input, join) = opt(parse_join).parse(input)?;
        
        let (input, where_opt) = opt((
            multispace1::<&str, Error<&str>>, 
            tag_no_case::<&str, &str, Error<&str>>("WHERE"), 
            multispace1::<&str, Error<&str>>
        )).parse(input)?;

        let (input, filter) = if where_opt.is_some() {
            let (input, f) = parse_expression(input)?;
            (input, Some(f))
        } else {
            (input, None)
        };

        let (input, order_by) = opt(parse_order_by).parse(input)?;
        let (input, limit) = opt(parse_limit).parse(input)?;

        return Ok((input, Query::Select { 
            fields, 
            collection: coll, 
            join, 
            filter,
            order_by,
            limit,
        }));
    }

    // UPDATE
    if let Ok((input, _)) = tag_no_case::<&str, &str, Error<&str>>("UPDATE").parse(input) {
        let (input, _) = multispace1::<&str, Error<&str>>.parse(input)?;
        let (input, coll) = identifier(input)?;
        let (input, _) = (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("SET"), multispace1::<&str, Error<&str>>).parse(input)?;
        
        let lower = input.to_lowercase();
        let where_pos = lower.find(" where ").ok_or_else(|| {
            nom::Err::Error(Error::new(input, nom::error::ErrorKind::TakeUntil))
        })?;
        
        let (json_str, input) = input.split_at(where_pos);
        let updates = serde_json::from_str(json_str.trim()).map_err(|_| {
            nom::Err::Error(Error::new(input, nom::error::ErrorKind::Tag))
        })?;
        
        let (input, _) = (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("WHERE"), multispace1::<&str, Error<&str>>).parse(input)?;
        let (input, filter) = parse_expression(input)?;
        return Ok((input, Query::Update { collection: coll, updates, filter }));
    }

    // DELETE FROM
    if let Ok((input, _)) = tag_no_case::<&str, &str, Error<&str>>("DELETE FROM").parse(input) {
        let (input, _) = multispace1::<&str, Error<&str>>.parse(input)?;
        let (input, coll) = identifier(input)?;
        let (input, _) = (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("WHERE"), multispace1::<&str, Error<&str>>).parse(input)?;
        let (input, filter) = parse_expression(input)?;
        return Ok((input, Query::Delete { collection: coll, filter }));
    }

    // DROP COLLECTION
    if let Ok((input, _)) = tag_no_case::<&str, &str, Error<&str>>("DROP COLLECTION").parse(input) {
        let (input, _) = multispace1::<&str, Error<&str>>.parse(input)?;
        let (input, coll) = identifier(input)?;
        return Ok((input, Query::DropCollection { collection: coll }));
    }

    // SHOW COLLECTIONS
    if let Ok((input, _)) = tag_no_case::<&str, &str, Error<&str>>("SHOW COLLECTIONS").parse(input) {
        return Ok((input, Query::ShowCollections));
    }

    Err(nom::Err::Error(Error::new(input, nom::error::ErrorKind::Tag)))
}

fn parse_select_field(input: &str) -> Res<'_, SelectField> {
    let (input, name) = identifier(input)?;
    let (input, alias) = opt(tuple((
        multispace1::<&str, Error<&str>>,
        opt(tag_no_case::<&str, &str, Error<&str>>("AS")),
        multispace0::<&str, Error<&str>>,
        alias_identifier
    ))).map(|opt| opt.map(|(_, _, _, id)| id)).parse(input)?;
    Ok((input, SelectField { name, alias }))
}

fn parse_table_info(input: &str) -> Res<'_, TableInfo> {
    let (input, name) = identifier(input)?;
    let (input, alias) = opt(tuple((
        multispace1::<&str, Error<&str>>,
        opt(tag_no_case::<&str, &str, Error<&str>>("AS")),
        multispace0::<&str, Error<&str>>,
        alias_identifier
    ))).map(|opt| opt.map(|(_, _, _, id)| id)).parse(input)?;
    Ok((input, TableInfo { name, alias }))
}

fn parse_join(input: &str) -> Res<'_, JoinInfo> {
    let (input, _) = multispace1::<&str, Error<&str>>.parse(input)?;
    
    let (input, join_type) = opt(alt((
        tag_no_case::<&str, &str, Error<&str>>("INNER").map(|_| JoinType::Inner),
        tag_no_case::<&str, &str, Error<&str>>("LEFT").map(|_| JoinType::Left),
        tag_no_case::<&str, &str, Error<&str>>("RIGHT").map(|_| JoinType::Right),
    ))).parse(input)?;
    let join_type = join_type.unwrap_or(JoinType::Inner);
    
    let (input, _) = (multispace0::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("JOIN"), multispace1::<&str, Error<&str>>).parse(input)?;
    let (input, collection) = parse_table_info(input)?;
    
    let (input, _) = (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("ON"), multispace1::<&str, Error<&str>>).parse(input)?;
    let (input, left_field) = identifier(input)?;
    let (input, _) = (multispace0::<&str, Error<&str>>, tag::<&str, &str, Error<&str>>("="), multispace0::<&str, Error<&str>>).parse(input)?;
    let (input, right_field) = identifier(input)?;
    
    Ok((input, JoinInfo { join_type, collection, left_field, right_field }))
}

pub fn parse_expression(input: &str) -> Res<'_, Filter> {
    parse_or_expr(input)
}

fn parse_or_expr(input: &str) -> Res<'_, Filter> {
    let (input, filters) = separated_list1(
        (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("OR"), multispace1::<&str, Error<&str>>),
        parse_and_expr
    ).parse(input)?;
    
    if filters.len() == 1 {
        Ok((input, filters.into_iter().next().unwrap()))
    } else {
        Ok((input, Filter::Or(filters)))
    }
}

fn parse_and_expr(input: &str) -> Res<'_, Filter> {
    let (input, filters) = separated_list1(
        (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("AND"), multispace1::<&str, Error<&str>>),
        parse_primary_filter
    ).parse(input)?;
    
    if filters.len() == 1 {
        Ok((input, filters.into_iter().next().unwrap()))
    } else {
        Ok((input, Filter::And(filters)))
    }
}

fn parse_order_by(input: &str) -> Res<'_, (String, SortDir)> {
    let (input, _) = (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("ORDER"), multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("BY"), multispace1::<&str, Error<&str>>).parse(input)?;
    let (input, field) = identifier(input)?;
    let (input, dir) = opt((
        multispace1::<&str, Error<&str>>,
        alt((
            tag_no_case::<&str, &str, Error<&str>>("ASC").map(|_| SortDir::Asc),
            tag_no_case::<&str, &str, Error<&str>>("DESC").map(|_| SortDir::Desc),
        ))
    )).map(|opt| opt.map(|(_, d)| d).unwrap_or(SortDir::Asc)).parse(input)?;
    Ok((input, (field, dir)))
}

fn parse_limit(input: &str) -> Res<'_, usize> {
    let (input, _) = (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("LIMIT"), multispace1::<&str, Error<&str>>).parse(input)?;
    let (input, limit) = map_res(digit1, |s: &str| s.parse::<usize>()).parse(input)?;
    Ok((input, limit))
}

fn parse_operator(input: &str) -> Res<'_, Operator> {
    alt((
        tag(">=").map(|_| Operator::Gte),
        tag("<=").map(|_| Operator::Lte),
        tag("!=").map(|_| Operator::Ne),
        tag("=").map(|_| Operator::Eq),
        tag(">").map(|_| Operator::Gt),
        tag("<").map(|_| Operator::Lt),
    )).parse(input)
}

fn parse_primary_filter(input: &str) -> Res<'_, Filter> {
    // 1. Tentar IN Subquery
    if let Ok((input_sub, field)) = identifier(input) {
        if let Ok((input_sub, _)) = (multispace1::<&str, Error<&str>>, tag_no_case::<&str, &str, Error<&str>>("IN"), multispace1::<&str, Error<&str>>).parse(input_sub) {
            if let Ok((rest, query)) = delimited(char('('), parse_query, char(')')).parse(input_sub) {
                return Ok((rest, Filter::InSubquery { field, query: Box::new(query) }));
            }
        }
    }

    // 2. Tentar Scalar Subquery
    if let Ok((input_sub, field)) = identifier(input) {
        if let Ok((input_sub, _)) = multispace0::<&str, Error<&str>>.parse(input_sub) {
            if let Ok((input_sub, operator)) = parse_operator(input_sub) {
                if let Ok((input_sub, _)) = multispace0::<&str, Error<&str>>.parse(input_sub) {
                    if let Ok((rest, query)) = delimited(char('('), parse_query, char(')')).parse(input_sub) {
                        return Ok((rest, Filter::ScalarSubquery { field, operator, query: Box::new(query) }));
                    }
                }
            }
        }
    }

    // 3. Tentar Standard Comparison
    if let Ok((input_std, field)) = identifier(input) {
        if let Ok((input_std, _)) = multispace0::<&str, Error<&str>>.parse(input_std) {
            if let Ok((input_std, operator)) = parse_operator(input_std) {
                if let Ok((input_std, _)) = multispace0::<&str, Error<&str>>.parse(input_std) {
                    if let Ok((rest, val_str)) = capture_value_blob(input_std) {
                        if let Ok(value) = serde_json::from_str::<Value>(val_str.trim()) {
                            return Ok((rest, Filter::Standard { field, operator, value }));
                        }
                    }
                }
            }
        }
    }

    // 4. Tentar JSON Subset Match
    let (rest, val_str) = capture_value_blob(input)?;
    if val_str.trim().starts_with('{') {
        let value = serde_json::from_str::<Value>(val_str.trim()).map_err(|_| {
            nom::Err::Error(Error::new(input, nom::error::ErrorKind::Tag))
        })?;
        return Ok((rest, Filter::Json(value)));
    }

    Err(nom::Err::Error(Error::new(input, nom::error::ErrorKind::Tag)))
}

fn capture_value_blob(input: &str) -> Res<'_, &str> {
    let lower = input.to_lowercase();
    let mut end_pos = input.len();
    if let Some(pos) = lower.find(" and ") { end_pos = end_pos.min(pos); }
    if let Some(pos) = lower.find(" or ") { end_pos = end_pos.min(pos); }
    if let Some(pos) = lower.find(")") { end_pos = end_pos.min(pos); }
    if let Some(pos) = lower.find(";") { end_pos = end_pos.min(pos); }
    if let Some(pos) = lower.find("\n") { end_pos = end_pos.min(pos); }
    let (pre, post) = input.split_at(end_pos);
    Ok((post, pre))
}

fn json_value_to_end(input: &str) -> Res<'_, Value> {
    let (input, json_str) = take_while::<_, &str, Error<&str>>(|c: char| c != ';' && c != '\n').parse(input)?;
    let value = serde_json::from_str(json_str.trim()).map_err(|_| {
        nom::Err::Error(Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    Ok((input, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_alias() {
        let q = "SELECT name AS nome FROM users AS u";
        let (_, query) = parse_query(q).unwrap();
        if let Query::Select { fields, collection, .. } = query {
            if let SelectFields::Specific(f) = fields {
                assert_eq!(f[0].alias, Some("nome".to_string()));
            }
            assert_eq!(collection.alias, Some("u".to_string()));
        } else { panic!("Wrong query variant"); }
    }

    #[test]
    fn test_parse_subquery() {
        let q = "SELECT * FROM users WHERE role_id IN (SELECT id FROM roles)";
        let (_, query) = parse_query(q).unwrap();
        if let Query::Select { filter: Some(Filter::InSubquery { field, .. }), .. } = query {
            assert_eq!(field, "role_id");
        } else { 
            panic!("Wrong filter variant"); 
        }
    }

    #[test]
    fn test_parse_scalar_subquery() {
        let q = "SELECT * FROM users WHERE age = (SELECT age FROM config)";
        let (_, query) = parse_query(q).unwrap();
        if let Query::Select { filter: Some(Filter::ScalarSubquery { field, operator, .. }), .. } = query {
            assert_eq!(field, "age");
            assert_eq!(operator, Operator::Eq);
        } else {
            panic!("Wrong filter variant: {:?}", query);
        }
    }

    #[test]
    fn test_parse_order_limit() {
        let q = "SELECT * FROM users ORDER BY age DESC LIMIT 10";
        let (_, query) = parse_query(q).unwrap();
        if let Query::Select { order_by, limit, .. } = query {
            assert_eq!(order_by, Some(("age".to_string(), SortDir::Desc)));
            assert_eq!(limit, Some(10));
        } else { panic!("Wrong query variant"); }
    }
}
