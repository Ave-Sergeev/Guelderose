use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InputMessage {
    pub id: String,
    pub image_path: String,
    pub result_path: String,
    pub predict_type: PredictType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredictType {
    TypeOne,
    TypeTwo,
}
