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
        #[serde(default)]
        internal_operation: bool,
        #[serde(default)]
        treasury_cost: i64,
        reason: String,
    },
    ArmyRecruited {
        at_year: f32,
        faction_id: String,
        recruited_units: u32,
        treasury_cost: i64,
        population_cost: f64,
    },
    ArmyUpkeepApplied {
        at_year: f32,
        faction_id: String,
        unit_count: u32,
        treasury_cost: i64,
        population_attrition: f64,
    },
    MilitaryCampaignStarted {
        at_year: f32,
        campaign_id: u64,
        attacker_faction: String,
        defender_faction: String,
        target_colony_id: u64,
        target_system: SystemId,
        #[serde(default)]
        outcome: crate::game_state::MilitaryCampaignOutcome,
    },
    MilitaryCampaignProgressed {
        at_year: f32,
        campaign_id: u64,
        progress: f32,
        attacker_strength: f32,
        defender_strength: f32,
        phase: crate::game_state::MilitaryCampaignPhase,
    },
    MilitaryCampaignAborted {
        at_year: f32,
        campaign_id: u64,
        attacker_faction: String,
        defender_faction: String,
        target_colony_id: u64,
        reason: String,
    },
    ArmyDispatched {
        at_year: f32,
        army_id: u64,
        faction_id: String,
        units: u32,
        from_system: SystemId,
        target_system: SystemId,
        #[serde(default)]
        campaign_id: Option<u64>,
    },
    ArmyAdvanced {
        at_year: f32,
        army_id: u64,
        faction_id: String,
        from_system: SystemId,
        to_system: SystemId,
        progress: f32,
    },
    ArmiesIntercepted {
        at_year: f32,
        attacker_army_id: u64,
        defender_army_id: u64,
        system: SystemId,
    },
    ArmyBattleResolved {
        at_year: f32,
        attacker_army_id: u64,
        defender_army_id: u64,
        system: SystemId,
        attacker_roll: u8,
        defender_roll: u8,
        attacker_loss: u32,
        defender_loss: u32,
        winner_faction: String,
    },
    ArmyRetreated {
        at_year: f32,
        army_id: u64,
        faction_id: String,
        from_system: SystemId,
        to_system: SystemId,
    },
    ArmyDisbanded {
        at_year: f32,
        army_id: u64,
        faction_id: String,
        system: SystemId,
        reason: String,
    },
    ColonyCapturedByForce {
        at_year: f32,
        campaign_id: u64,
        attacker_faction: String,
        defender_faction: String,
        colony_id: u64,
        system: SystemId,
        stability_hit: f32,
    },
    ColonySackedByForce {
        at_year: f32,
        campaign_id: u64,
        attacker_faction: String,
        defender_faction: String,
        colony_id: u64,
        system: SystemId,
        treasury_stolen: i64,
        population_lost: f64,
        stability_hit: f32,
    },
}
