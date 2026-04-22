use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::procedural_galaxy::SystemId;

use crate::game_state::{ColonyBuildingKind, ColonyBuildingSite, SurveyStage};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GameEvent {
    DiscoveredSystem {
        at_year: f32,
        system: SystemId,
        by_faction: String,
    },
    HomeSystemSelected {
        at_year: f32,
        system: SystemId,
    },
    SurveyedSystem {
        at_year: f32,
        system: SystemId,
        by_faction: String,
        stage: SurveyStage,
        #[serde(default)]
        surveyed_body_count: u16,
        #[serde(default)]
        habitable_body_count: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        viable_body_index: Option<u16>,
    },
    FoundedColony {
        at_year: f32,
        colony_id: u64,
        colony_name: String,
        founder_faction: String,
        system: SystemId,
        body_index: u16,
        #[serde(default)]
        habitable_site: bool,
        #[serde(default)]
        earth_like_world: bool,
        #[serde(default)]
        system_pos: [f32; 3],
        #[serde(default)]
        element_resource_profile: HashMap<String, f32>,
        #[serde(default)]
        atmosphere_resource_profile: HashMap<String, f32>,
        #[serde(default)]
        atmosphere_pressure_atm: f32,
        #[serde(default)]
        colonists_sent: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_colony_id: Option<u64>,
    },
    StartingColonySelected {
        at_year: f32,
        colony_id: u64,
    },
    FactionRelationChanged {
        at_year: f32,
        from_faction: String,
        to_faction: String,
        delta: i16,
        reason: String,
    },
    CompletedColonyBuilding {
        at_year: f32,
        colony_id: u64,
        kind: ColonyBuildingKind,
        #[serde(default)]
        site: ColonyBuildingSite,
        target_level: u16,
    },
    TreatyEstablished {
        at_year: f32,
        faction_a: String,
        faction_b: String,
        treaty: crate::game_state::DiplomaticTreatyKind,
        expires_year: f32,
        reason: String,
    },
    TreatyDissolved {
        at_year: f32,
        faction_a: String,
        faction_b: String,
        treaty: crate::game_state::DiplomaticTreatyKind,
        reason: String,
    },
    SanctionImposed {
        at_year: f32,
        by_faction: String,
        target_faction: String,
        expires_year: f32,
        reason: String,
    },
    SanctionLifted {
        at_year: f32,
        by_faction: String,
        target_faction: String,
        reason: String,
    },
    PowerplayOperationResolved {
        at_year: f32,
        actor_faction: String,
        target_faction: String,
        system: SystemId,
        operation: crate::game_state::PowerplayOperationKind,
        success: bool,
        strength: f32,
        reason: String,
    },
}
