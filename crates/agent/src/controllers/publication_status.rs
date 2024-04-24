use crate::publications;
use chrono::{DateTime, Utc};
use models::Id;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use super::PublicationResult;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PublicationStatus {
    pub id: Id,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<publications::JobStatus>,
}

impl PublicationStatus {
    pub fn is_success(&self) -> bool {
        // TODO: should EmptyDraft be considered successful?
        self.result.as_ref().is_some_and(|s| s.is_success())
    }

    pub fn is_incompatible_collection_error(
        &self,
        collection_name: &str,
    ) -> Option<Vec<publications::builds::AffectedConsumer>> {
        match &self.result {
            Some(publications::JobStatus::BuildFailed {
                incompatible_collections,
                ..
            }) => incompatible_collections
                .iter()
                .find(|ic| ic.collection == collection_name)
                .map(|ic| ic.affected_materializations.clone()),
            _ => None,
        }
    }

    pub fn created(id: Id, time: DateTime<Utc>) -> Self {
        PublicationStatus {
            id,
            created: Some(time),
            completed: None,
            result: None,
        }
    }

    pub fn observed(publication: &PublicationResult) -> Self {
        PublicationStatus {
            id: publication.publication_id,
            created: None,
            completed: Some(publication.completed_at),
            result: Some(publication.publication_status.clone()),
        }
    }

    pub fn with_update(mut self, publication: PublicationStatus) -> Self {
        self.update(publication);
        self
    }

    pub fn update(&mut self, publication: PublicationStatus) {
        assert_eq!(
            self.id, publication.id,
            "complete must be invoked for the same publication id"
        );
        self.completed = publication.completed;
        self.result = publication.result.clone();
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PublicationHistory {
    pub pending: Option<PublicationStatus>,
    pub history: VecDeque<PublicationStatus>,
    pub last_observed: Option<PublicationStatus>,
}

impl PublicationHistory {
    const MAX_HISTORY: usize = 3;

    pub fn initial(observed: PublicationStatus) -> Self {
        PublicationHistory {
            pending: None,
            history: VecDeque::new(),
            last_observed: Some(observed),
        }
    }

    pub fn is_pending(&self, publication_id: Id) -> bool {
        self.pending.iter().any(|p| p.id == publication_id)
    }

    /// Updates status to reflect the recently observed publication. Returns `Some` if this publication
    /// was one that the controller itself has created and was waiting on.
    pub fn observe(&mut self, publication: PublicationStatus) -> Option<PublicationStatus> {
        if self
            .pending
            .as_ref()
            .is_some_and(|p| p.id == publication.id)
        {
            let completed = self.pending.take().unwrap().with_update(publication);
            self.history.push_front(completed.clone());
            while self.history.len() > PublicationHistory::MAX_HISTORY {
                self.history.pop_back();
            }
            self.last_observed = Some(completed.clone());
            Some(completed)
        } else {
            self.last_observed = Some(publication.clone());
            None
        }
    }
}
