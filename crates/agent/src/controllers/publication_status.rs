use crate::publications;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use tables::Id;

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
    /// Returns a status for a newly created publication that is still pending.
    pub fn created(id: Id, time: DateTime<Utc>) -> Self {
        PublicationStatus {
            id,
            created: Some(time),
            completed: None,
            result: None,
        }
    }

    /// Creates a status for a publication that has just completed.
    pub fn observed(publication: &PublicationResult) -> Self {
        PublicationStatus {
            id: publication.publication_id,
            created: None,
            completed: Some(publication.completed_at),
            result: Some(publication.publication_status.clone()),
        }
    }

    /// Updates the status with the result of a publication that has just completed.
    /// Panics if the id of the publication does not match the id of this status.
    pub fn with_update(mut self, publication: PublicationStatus) -> Self {
        self.update(publication);
        self
    }

    /// Updates the status with the result of a publication that has just completed.
    /// Panics if the id of the publication does not match the id of this status.
    pub fn update(&mut self, publication: PublicationStatus) {
        assert_eq!(
            self.id, publication.id,
            "complete must be invoked for the same publication id"
        );
        self.completed = publication.completed;
        self.result = publication.result.clone();
    }
}

/// A controller status showing the history of publications that have been created or observed.
/// Note that failed publications are not recorded here if they were not created by the controller.
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

    /// Returns true if the given publication is currently pending.
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
