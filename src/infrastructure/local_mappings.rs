use std::io::BufRead;

use thiserror::Error;
use trakkin_mappings_language::{Record, parse, visit_records};

pub struct LocalMappingDocument {
    records: Vec<Record>,
}

impl LocalMappingDocument {
    pub fn read(
        reader: impl BufRead,
        maximum_record_bytes: usize,
    ) -> Result<Self, LocalMappingDocumentError> {
        let mut records = Vec::new();
        visit_records(reader, maximum_record_bytes, |located| {
            records.push(located.record);
            Ok(())
        })
        .map_err(|error| LocalMappingDocumentError::Stored(error.to_string()))?;
        Ok(Self { records })
    }

    pub fn create(&mut self, source: &str) -> Result<(), LocalMappingDocumentError> {
        let record = parse_record(source)?;
        if self
            .records
            .iter()
            .any(|existing| existing.statement.relation_key() == record.statement.relation_key())
        {
            return Err(LocalMappingDocumentError::AlreadyExists);
        }
        self.records.push(record);
        Ok(())
    }

    pub fn update(
        &mut self,
        statement_id: &str,
        source: &str,
    ) -> Result<(), LocalMappingDocumentError> {
        let position = self
            .records
            .iter()
            .position(|record| record.statement.id() == statement_id)
            .ok_or(LocalMappingDocumentError::NotFound)?;
        let record = parse_record(source)?;
        if self.records.iter().enumerate().any(|(index, existing)| {
            index != position
                && existing.statement.relation_key() == record.statement.relation_key()
        }) {
            return Err(LocalMappingDocumentError::AlreadyExists);
        }
        self.records[position] = record;
        Ok(())
    }

    pub fn remove(&mut self, statement_id: &str) -> Result<(), LocalMappingDocumentError> {
        let position = self
            .records
            .iter()
            .position(|record| record.statement.id() == statement_id)
            .ok_or(LocalMappingDocumentError::NotFound)?;
        self.records.remove(position);
        Ok(())
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.records
            .into_iter()
            .flat_map(|record| record.canonical().into_bytes())
            .collect()
    }
}

fn parse_record(source: &str) -> Result<Record, LocalMappingDocumentError> {
    let mut records =
        parse(source).map_err(|error| LocalMappingDocumentError::Invalid(error.to_string()))?;
    if records.len() != 1 {
        return Err(LocalMappingDocumentError::Invalid(
            "exactly one mapping record is required".to_owned(),
        ));
    }
    Ok(records.remove(0))
}

#[derive(Debug, Error)]
pub enum LocalMappingDocumentError {
    #[error("local mapping record is invalid: {0}")]
    Invalid(String),
    #[error("local mapping already exists")]
    AlreadyExists,
    #[error("local mapping was not found")]
    NotFound,
    #[error("stored local mappings are invalid: {0}")]
    Stored(String),
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use trakkin_mappings_language::parse;

    use super::{LocalMappingDocument, LocalMappingDocumentError};

    #[test]
    fn local_records_are_created_updated_and_removed_by_statement_identity() {
        let source = "#@note original\na://one <=> b://one\n";
        let original_id = parse(source).unwrap()[0].statement.id();
        let mut document = LocalMappingDocument::read(Cursor::new(source), 1024 * 1024).unwrap();

        document.create("a://two => b://two").unwrap();
        document
            .update(&original_id, "#@note updated\na://three <~> b://three")
            .unwrap();
        let updated_id = parse("a://three <~> b://three").unwrap()[0].statement.id();
        document.remove(&updated_id).unwrap();

        assert_eq!(
            String::from_utf8(document.into_bytes()).unwrap(),
            "a://two => b://two\n"
        );
    }

    #[test]
    fn local_record_changes_reject_ambiguous_or_missing_rows() {
        let mut document = LocalMappingDocument::read(
            Cursor::new("a://one <=> b://one\na://two => b://two\n"),
            1024 * 1024,
        )
        .unwrap();
        assert!(matches!(
            document.create("b://one <=> a://one"),
            Err(LocalMappingDocumentError::AlreadyExists)
        ));
        assert!(matches!(
            document.create("a://three => b://three\na://four => b://four"),
            Err(LocalMappingDocumentError::Invalid(_))
        ));
        assert!(matches!(
            document.remove("missing"),
            Err(LocalMappingDocumentError::NotFound)
        ));
    }
}
