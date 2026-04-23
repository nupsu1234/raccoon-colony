use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::events::GameEvent;
use crate::procedural_galaxy::SystemId;

mod survey_records_serde {
    use super::*;
    use serde::{Deserializer, Serializer, ser::SerializeSeq};

    pub fn serialize<S>(map: &HashMap<SystemId, SystemSurveyRecord>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(map.len()))?;
        for record in map.values() {
            seq.serialize_element(record)?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<SystemId, SystemSurveyRecord>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<SystemSurveyRecord> = Vec::deserialize(deserializer)?;
        Ok(vec.into_iter().map(|record| (record.system, record)).collect())
    }
}

mod system_sim_serde {
    use super::*;
    use serde::{Deserializer, Serializer, ser::SerializeSeq};

    pub fn serialize<S>(
        map: &HashMap<SystemId, SystemSimState>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(map.len()))?;
        for state in map.values() {
            seq.serialize_element(state)?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<SystemId, SystemSimState>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<SystemSimState> = Vec::deserialize(deserializer)?;
        Ok(vec.into_iter().map(|state| (state.system, state)).collect())
    }
}

mod faction_relations_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct RelationEntry {
        a: String,
        b: String,
        value: i16,
    }

    pub fn serialize<S>(
        map: &HashMap<(String, String), i16>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let entries: Vec<RelationEntry> = map
            .iter()
            .map(|((a, b), value)| RelationEntry {
                a: a.clone(),
                b: b.clone(),
                value: *value,
            })
            .collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<(String, String), i16>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<RelationEntry>::deserialize(deserializer)?;
        let mut map = HashMap::new();
        for entry in entries {
            map.insert((entry.a, entry.b), entry.value);
        }
        Ok(map)
    }
}

mod diplomacy_treaties_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TreatyEntry {
        a: String,
        b: String,
        value: DiplomacyTreatyState,
    }

    pub fn serialize<S>(
        map: &HashMap<(String, String), DiplomacyTreatyState>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let entries: Vec<TreatyEntry> = map
            .iter()
            .map(|((a, b), value)| TreatyEntry {
                a: a.clone(),
                b: b.clone(),
                value: value.clone(),
            })
            .collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<(String, String), DiplomacyTreatyState>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<TreatyEntry>::deserialize(deserializer)?;
        let mut map = HashMap::new();
        for entry in entries {
            map.insert((entry.a, entry.b), entry.value);
        }
        Ok(map)
    }
}

pub const PLAYER_FACTION_ID: &str = "raccoon-flood";

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum SurveyStage {
    #[default]
    Unknown,
    Located,
    StellarSurvey,
    PlanetarySurvey,
    ColonyAssessment,
}

impl SurveyStage {
    pub fn label(self) -> &'static str {
        match self {
            SurveyStage::Unknown => "Unknown",
            SurveyStage::Located => "Located",
            SurveyStage::StellarSurvey => "Stellar Survey",
            SurveyStage::PlanetarySurvey => "Planetary Survey",
            SurveyStage::ColonyAssessment => "Colony Assessment",
        }
    }

    pub fn next(self) -> Option<Self> {
        match self {
            SurveyStage::Unknown => Some(SurveyStage::Located),
            SurveyStage::Located => Some(SurveyStage::StellarSurvey),
            SurveyStage::StellarSurvey => Some(SurveyStage::PlanetarySurvey),
            SurveyStage::PlanetarySurvey => Some(SurveyStage::ColonyAssessment),
            SurveyStage::ColonyAssessment => None,
        }
    }

    #[allow(dead_code)]
    pub fn action_label(self) -> &'static str {
        match self {
            SurveyStage::Unknown => "Chart System",
            SurveyStage::Located => "Survey Star",
            SurveyStage::StellarSurvey => "Survey Worlds",
            SurveyStage::PlanetarySurvey => "Assess Colony",
            SurveyStage::ColonyAssessment => "Survey Complete",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SystemSurveyRecord {
    pub system: SystemId,
    #[serde(default)]
    pub stage: SurveyStage,
    #[serde(default)]
    pub surveyed_body_count: u16,
    #[serde(default)]
    pub habitable_body_count: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub viable_body_index: Option<u16>,
    #[serde(default)]
    pub last_updated_year: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingSurveyScan {
    pub system: SystemId,
    pub by_faction: String,
    pub start_year: f32,
    pub complete_year: f32,
    pub target_stage: SurveyStage,
    #[serde(default)]
    pub surveyed_body_count: u16,
    #[serde(default)]
    pub habitable_body_count: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub viable_body_index: Option<u16>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingColonyFounding {
    pub colony_id: u64,
    pub colony_name: String,
    pub founder_faction: String,
    pub system: SystemId,
    pub body_index: u16,
    #[serde(default)]
    pub habitable_site: bool,
    #[serde(default)]
    pub earth_like_world: bool,
    #[serde(default)]
    pub system_pos: [f32; 3],
    #[serde(default)]
    pub element_resource_profile: HashMap<String, f32>,
    #[serde(default)]
    pub atmosphere_resource_profile: HashMap<String, f32>,
    #[serde(default)]
    pub atmosphere_pressure_atm: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_colony_id: Option<u64>,
    pub colonists_sent: u32,
    pub start_year: f32,
    pub complete_year: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingPopulationTransfer {
    pub source_colony_id: u64,
    pub target_colony_id: u64,
    pub colonists: u32,
    pub start_year: f32,
    pub complete_year: f32,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ColonyBuildingKind {
    SpaceStation,
    IndustrialHub,
    AgriDome,
    DeepMantleMiningStation,
    AtmosphereHarvester,
    FuelScoopDroneSwarm,
    TradingHub,
    EntertainmentPlaza,
    LogisticsExchange,
    HabitatArcology,
    DefenseGrid,
    SystemsAdministration,
    CatalyticRefinery,
}

impl ColonyBuildingKind {
    #[allow(dead_code)]
    pub const ALL: [Self; 13] = [
        ColonyBuildingKind::SpaceStation,
        ColonyBuildingKind::IndustrialHub,
        ColonyBuildingKind::AgriDome,
        ColonyBuildingKind::DeepMantleMiningStation,
        ColonyBuildingKind::AtmosphereHarvester,
        ColonyBuildingKind::FuelScoopDroneSwarm,
        ColonyBuildingKind::TradingHub,
        ColonyBuildingKind::EntertainmentPlaza,
        ColonyBuildingKind::LogisticsExchange,
        ColonyBuildingKind::HabitatArcology,
        ColonyBuildingKind::DefenseGrid,
        ColonyBuildingKind::SystemsAdministration,
        ColonyBuildingKind::CatalyticRefinery,
    ];

    #[allow(dead_code)]
    pub fn all() -> [Self; 13] {
        Self::ALL
    }

    /// Returns the full static definition for this building kind.
    pub fn definition(self) -> &'static ColonyBuildingDefinition {
        match self {
            Self::SpaceStation => &BUILDING_DEF_SPACE_STATION,
            Self::IndustrialHub => &BUILDING_DEF_INDUSTRIAL_HUB,
            Self::AgriDome => &BUILDING_DEF_AGRI_DOME,
            Self::DeepMantleMiningStation => &BUILDING_DEF_DEEP_MANTLE_MINING,
            Self::AtmosphereHarvester => &BUILDING_DEF_ATMOSPHERE_HARVESTER,
            Self::FuelScoopDroneSwarm => &BUILDING_DEF_FUEL_SCOOP_DRONE_SWARM,
            Self::TradingHub => &BUILDING_DEF_TRADING_HUB,
            Self::EntertainmentPlaza => &BUILDING_DEF_ENTERTAINMENT_PLAZA,
            Self::LogisticsExchange => &BUILDING_DEF_LOGISTICS_EXCHANGE,
            Self::HabitatArcology => &BUILDING_DEF_HABITAT_ARCOLOGY,
            Self::DefenseGrid => &BUILDING_DEF_DEFENSE_GRID,
            Self::SystemsAdministration => &BUILDING_DEF_SYSTEMS_ADMINISTRATION,
            Self::CatalyticRefinery => &BUILDING_DEF_CATALYTIC_REFINERY,
        }
    }

    pub fn label(self) -> &'static str { self.definition().label }
    pub fn max_level(self) -> u16 { self.definition().max_level }
    #[allow(dead_code)]
    pub fn queue_button_label(self) -> &'static str { self.definition().queue_button_label }
    pub fn is_player_queueable(self) -> bool { self.definition().is_player_queueable }
    pub fn consumes_site_slot(self) -> bool { self.is_player_queueable() }
    #[allow(dead_code)]
    pub fn role_description(self) -> &'static str { self.definition().role_description }
    pub fn requires_solid_planet_surface(self) -> bool { self.definition().requires_solid_surface }
    pub fn requires_atmosphere(self) -> bool { self.definition().requires_atmosphere }
    pub fn requires_scoopable_star(self) -> bool { self.definition().requires_scoopable_star }

    pub fn supports_site(self, site: ColonyBuildingSite) -> bool {
        match self.definition().site_type {
            ColonyBuildingSiteType::Orbital => matches!(site, ColonyBuildingSite::Orbital),
            ColonyBuildingSiteType::Star => matches!(site, ColonyBuildingSite::Star(_)),
            ColonyBuildingSiteType::Planet => matches!(site, ColonyBuildingSite::Planet(_)),
        }
    }

    #[allow(dead_code)]
    pub fn effect_preview_per_level(self) -> ColonyBuildingEffectPreview {
        let modifiers = self.definition().economy_profile.per_level_modifiers;
        ColonyBuildingEffectPreview {
            food_production_bonus: modifiers.food_production_bonus,
            industry_production_bonus: modifiers.industry_production_bonus,
            energy_production_bonus: modifiers.energy_production_bonus,
            food_demand_bonus: modifiers.food_demand_bonus,
            industry_demand_bonus: modifiers.industry_demand_bonus,
            energy_demand_bonus: modifiers.energy_demand_bonus,
            element_extraction_bonus: modifiers.element_extraction_bonus,
            atmosphere_harvest_bonus: modifiers.atmosphere_harvest_bonus,
            treasury_production_bonus: modifiers.treasury_production_bonus,
            stability_bonus: modifiers.stability_bonus,
            growth_bonus: modifiers.growth_bonus,
            annual_upkeep: modifiers.annual_upkeep,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ColonyBuildingSite {
    #[default]
    Orbital,
    Star(u16),
    Planet(u16),
}

impl ColonyBuildingSite {
    pub fn label(self) -> Cow<'static, str> {
        match self {
            ColonyBuildingSite::Orbital => Cow::Borrowed("Orbital"),
            ColonyBuildingSite::Star(index) => Cow::Owned(format!("Star {}", index as usize + 1)),
            ColonyBuildingSite::Planet(index) => Cow::Owned(format!("Planet {}", index as usize + 1)),
        }
    }

    #[allow(dead_code)]
    pub fn host_for_body_index(body_index: u16) -> Self {
        if body_index == u16::MAX {
            Self::Orbital
        } else {
            Self::Planet(body_index)
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColonyBuildingSiteProfile {
    pub planet_is_gas_giant: Option<bool>,
    pub planet_habitable: Option<bool>,
    pub planet_building_slot_capacity: Option<u16>,
    pub planet_has_atmosphere: Option<bool>,
    pub star_is_scoopable: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColonyBuildingState {
    pub kind: ColonyBuildingKind,
    #[serde(default)]
    pub site: ColonyBuildingSite,
    pub level: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingColonyBuilding {
    pub colony_id: u64,
    pub kind: ColonyBuildingKind,
    #[serde(default)]
    pub site: ColonyBuildingSite,
    pub target_level: u16,
    pub start_year: f32,
    pub complete_year: f32,
    #[serde(default)]
    pub deferred_treasury_due: i64,
    #[serde(default)]
    pub annual_construction_upkeep: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlayerState {
    pub faction_id: String,
    pub location: Option<SystemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_system: Option<SystemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starting_colony_id: Option<u64>,
}

impl Default for PlayerState {
    fn default() -> Self {
        Self {
            faction_id: PLAYER_FACTION_ID.to_owned(),
            location: None,
            home_system: None,
            starting_colony_id: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FactionState {
    pub id: String,
    pub display_name: String,
    pub treasury: i64,
    #[serde(default)]
    pub colonization_tech_level: u32,
    #[serde(default)]
    pub colonization_tech_progress: f32,
    /// The first colony founded by this faction, which receives reduced upkeep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starting_colony_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ConflictState {
    Calm,
    Tense,
    ProxyWar,
    Embargo,
    PatrolSurge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DiplomaticTreatyKind {
    Alliance,
    NonAggressionPact,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiplomacyTreatyState {
    pub kind: DiplomaticTreatyKind,
    pub started_year: f32,
    pub expires_year: f32,
    #[serde(default)]
    pub cohesion: f32,
    #[serde(default)]
    pub strain: f32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PowerplayOperationKind {
    UndermineInfluence,
    SupportAlly,
    EconomicPressure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PowerplayOperationRecord {
    pub at_year: f32,
    pub actor_faction: String,
    pub target_faction: String,
    pub system: SystemId,
    pub operation: PowerplayOperationKind,
    pub success: bool,
}

impl Default for ConflictState {
    fn default() -> Self {
        Self::Calm
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SystemSimState {
    pub system: SystemId,
    #[serde(default)]
    pub influence_by_faction: HashMap<String, f32>,
    #[serde(default = "SystemSimState::default_security")]
    pub security: f32,
    #[serde(default = "SystemSimState::default_stability")]
    pub stability: f32,
    #[serde(default)]
    pub econ_pressure: f32,
    #[serde(default)]
    pub trade_flow: f32,
    #[serde(default)]
    pub scarcity: f32,
    #[serde(default)]
    pub conflict: ConflictState,
}

impl SystemSimState {
    fn default_security() -> f32 {
        0.55
    }

    fn default_stability() -> f32 {
        0.60
    }
}

impl Default for SystemSimState {
    fn default() -> Self {
        Self {
            system: SystemId {
                sector: crate::procedural_galaxy::SectorCoord { x: 0, y: 0 },
                local_index: 0,
            },
            influence_by_faction: HashMap::new(),
            security: Self::default_security(),
            stability: Self::default_stability(),
            econ_pressure: 0.0,
            trade_flow: 0.0,
            scarcity: 0.0,
            conflict: ConflictState::Calm,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MissionKind {
    SupplyRelief,
    ReconSweep,
    InfluenceOp,
    AllianceSupport,
    SanctionRunning,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MissionState {
    pub id: u64,
    pub issuer_faction: String,
    pub target_system: SystemId,
    pub kind: MissionKind,
    pub title: String,
    pub description: String,
    pub reward_credits: i64,
    pub reward_tech: f32,
    pub reward_reputation: i16,
    #[serde(default)]
    pub risk: f32,
    #[serde(default)]
    pub expires_year: f32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AiBuildTelemetry {
    #[serde(default)]
    pub intent_recovery: u32,
    #[serde(default)]
    pub intent_extraction: u32,
    #[serde(default)]
    pub intent_throughput: u32,
    #[serde(default)]
    pub intent_growth: u32,
    #[serde(default)]
    pub reject_reserve: u32,
    #[serde(default)]
    pub reject_substitution_stress: u32,
    #[serde(default)]
    pub reject_site_invalid: u32,
    #[serde(default)]
    pub avg_reserve_depth: f32,
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub struct NearestColonyInfo {
    pub system_pos: [f32; 3],
    pub distance: f32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ColonyStage {
    Outpost,
    Settlement,
    City,
    CoreWorld,
}

impl ColonyStage {
    pub fn label(self) -> &'static str {
        match self {
            ColonyStage::Outpost => "Outpost",
            ColonyStage::Settlement => "Settlement",
            ColonyStage::City => "City",
            ColonyStage::CoreWorld => "CoreWorld",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ColonyPolicy {
    #[default]
    Balanced,
    Growth,
    Industry,
    Fortress,
}

pub struct ColonyPolicyDefinition {
    pub label: &'static str,
    #[allow(dead_code)]
    pub description: &'static str,
    /// Baseline per-tick production rates: (food, industry, energy, defense).
    pub production_rates: (f32, f32, f32, f32),
    /// Annual stability drift contributed by this policy.
    pub stability_bonus: f32,
    /// Migration modifier applied each tick.
    pub migration_bonus: f32,
    /// Multiplier on colony population carrying capacity.
    pub carrying_capacity_multiplier: f32,
    /// Multiplier on colony upkeep costs.
    pub upkeep_multiplier: f64,
}

const POLICY_DEF_BALANCED: ColonyPolicyDefinition = ColonyPolicyDefinition {
    label: "Balanced",
    description: "Steady all-round development with no major bonuses or penalties.",
    production_rates: (0.0020, 0.0015, 0.0018, 0.0012),
    stability_bonus: 0.001,
    migration_bonus: 0.0,
    carrying_capacity_multiplier: 1.0,
    upkeep_multiplier: 1.0,
};
const POLICY_DEF_GROWTH: ColonyPolicyDefinition = ColonyPolicyDefinition {
    label: "Growth",
    description: "Prioritises population growth and food. Industry suffers and rapid expansion causes unrest.",
    production_rates: (0.0034, 0.0008, 0.0018, 0.0006),
    stability_bonus: -0.005,
    migration_bonus: 0.003,
    carrying_capacity_multiplier: 1.15,
    upkeep_multiplier: 1.10,
};
const POLICY_DEF_INDUSTRY: ColonyPolicyDefinition = ColonyPolicyDefinition {
    label: "Industry",
    description: "Maximises industrial and energy output. Harsh working conditions reduce stability and deter immigration.",
    production_rates: (0.0008, 0.0034, 0.0026, 0.0010),
    stability_bonus: -0.006,
    migration_bonus: -0.002,
    carrying_capacity_multiplier: 0.90,
    upkeep_multiplier: 1.15,
};
const POLICY_DEF_FORTRESS: ColonyPolicyDefinition = ColonyPolicyDefinition {
    label: "Fortress",
    description: "Strong defense and law enforcement boost stability. Martial law deters immigration and limits growth.",
    production_rates: (0.0012, 0.0012, 0.0016, 0.0038),
    stability_bonus: 0.008,
    migration_bonus: -0.003,
    carrying_capacity_multiplier: 0.85,
    upkeep_multiplier: 1.30,
};

impl ColonyPolicy {
    pub fn definition(self) -> &'static ColonyPolicyDefinition {
        match self {
            ColonyPolicy::Balanced => &POLICY_DEF_BALANCED,
            ColonyPolicy::Growth => &POLICY_DEF_GROWTH,
            ColonyPolicy::Industry => &POLICY_DEF_INDUSTRY,
            ColonyPolicy::Fortress => &POLICY_DEF_FORTRESS,
        }
    }

    pub fn label(self) -> &'static str {
        self.definition().label
    }

    #[allow(dead_code)]
    pub fn description(self) -> &'static str {
        self.definition().description
    }

    #[allow(dead_code)]
    pub fn all() -> [Self; 4] {
        [
            ColonyPolicy::Balanced,
            ColonyPolicy::Growth,
            ColonyPolicy::Industry,
            ColonyPolicy::Fortress,
        ]
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaxationPolicy {
    Low,
    #[default]
    Standard,
    High,
    Extractive,
}

pub struct TaxationPolicyDefinition {
    pub label: &'static str,
    #[allow(dead_code)]
    pub description: &'static str,
    /// Revenue multiplier relative to standard taxation.
    pub revenue_multiplier: f64,
    /// Annual stability drift caused by this tax level.
    pub stability_effect: f32,
    /// Annual growth rate modifier (positive = more births/immigration).
    pub growth_effect: f32,
}

const TAX_DEF_LOW: TaxationPolicyDefinition = TaxationPolicyDefinition {
    label: "Low",
    description: "Low taxes. Citizens are happy: +stability, +growth, but 60% revenue.",
    revenue_multiplier: 0.60,
    stability_effect: 0.005,
    growth_effect: 0.0004,
};
const TAX_DEF_STANDARD: TaxationPolicyDefinition = TaxationPolicyDefinition {
    label: "Standard",
    description: "Standard tax rate with no side-effects.",
    revenue_multiplier: 1.00,
    stability_effect: 0.0,
    growth_effect: 0.0,
};
const TAX_DEF_HIGH: TaxationPolicyDefinition = TaxationPolicyDefinition {
    label: "High",
    description: "High taxes squeeze more revenue. Stability drops and growth slows.",
    revenue_multiplier: 1.40,
    stability_effect: -0.012,
    growth_effect: -0.0003,
};
const TAX_DEF_EXTRACTIVE: TaxationPolicyDefinition = TaxationPolicyDefinition {
    label: "Extractive",
    description: "Maximum extraction. Heavy stability and growth penalties.",
    revenue_multiplier: 1.80,
    stability_effect: -0.028,
    growth_effect: -0.0008,
};

impl TaxationPolicy {
    pub fn definition(self) -> &'static TaxationPolicyDefinition {
        match self {
            TaxationPolicy::Low => &TAX_DEF_LOW,
            TaxationPolicy::Standard => &TAX_DEF_STANDARD,
            TaxationPolicy::High => &TAX_DEF_HIGH,
            TaxationPolicy::Extractive => &TAX_DEF_EXTRACTIVE,
        }
    }

    pub fn label(self) -> &'static str {
        self.definition().label
    }

    #[allow(dead_code)]
    pub fn description(self) -> &'static str {
        self.definition().description
    }

    #[allow(dead_code)]
    pub fn all() -> [Self; 4] {
        [
            TaxationPolicy::Low,
            TaxationPolicy::Standard,
            TaxationPolicy::High,
            TaxationPolicy::Extractive,
        ]
    }

    pub fn multiplier(self) -> f64 {
        self.definition().revenue_multiplier
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColonyState {
    pub id: u64,
    pub name: String,
    pub owner_faction: String,
    pub system: SystemId,
    pub body_index: u16,
    #[serde(default)]
    pub habitable_site: bool,
    #[serde(default)]
    pub earth_like_world: bool,
    #[serde(default)]
    pub system_pos: [f32; 3],
    #[serde(default)]
    pub policy: ColonyPolicy,
    #[serde(default)]
    pub taxation_policy: TaxationPolicy,
    pub stage: ColonyStage,
    pub population: f64,
    pub stability: f32,
    pub food_balance: f32,
    pub industry_balance: f32,
    pub energy_balance: f32,
    pub defense_balance: f32,
    #[serde(default = "ColonyState::default_stockpile_capacity")]
    pub stockpile_capacity: f32,
    #[serde(default)]
    pub food_stockpile: f32,
    #[serde(default)]
    pub industry_stockpile: f32,
    #[serde(default)]
    pub energy_stockpile: f32,
    #[serde(default)]
    pub element_stockpiles: HashMap<String, f32>,
    #[serde(default)]
    pub atmosphere_stockpiles: HashMap<String, f32>,
    #[serde(default)]
    pub element_resource_profile: HashMap<String, f32>,
    #[serde(default)]
    pub atmosphere_resource_profile: HashMap<String, f32>,
    #[serde(default)]
    pub atmosphere_pressure_atm: f32,
    #[serde(default)]
    pub buildings: Vec<ColonyBuildingState>,
    #[serde(default)]
    pub last_tax_revenue_annual: i64,
    #[serde(default)]
    pub last_upkeep_cost_annual: i64,
    #[serde(default)]
    pub last_net_revenue_annual: i64,
}

impl ColonyState {
    fn default_stockpile_capacity() -> f32 {
        100.0
    }

    pub fn building_level_at_site(&self, kind: ColonyBuildingKind, site: ColonyBuildingSite) -> u16 {
        self.buildings
            .iter()
            .find(|building| building.kind == kind && building.site == site)
            .map(|building| building.level)
            .unwrap_or(0)
    }

    pub fn occupied_building_slots_at_site(&self, site: ColonyBuildingSite) -> u16 {
        self.buildings
            .iter()
            .filter(|building| {
                building.site == site
                    && building.level > 0
                    && building.kind.consumes_site_slot()
            })
            .count() as u16
    }

    fn set_building_level(&mut self, kind: ColonyBuildingKind, site: ColonyBuildingSite, level: u16) {
        if let Some(existing) = self
            .buildings
            .iter_mut()
            .find(|b| b.kind == kind && b.site == site)
        {
            existing.level = existing.level.max(level);
            return;
        }
        self.buildings.push(ColonyBuildingState { kind, site, level });
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GameState {
    pub current_year: f32,
    pub next_colony_id: u64,
    #[serde(default = "GameState::default_base_colonization_range_world")]
    pub base_colonization_range_world: f32,
    pub player: PlayerState,
    pub factions: HashMap<String, FactionState>,
    #[serde(default, with = "survey_records_serde")]
    pub survey_records: HashMap<SystemId, SystemSurveyRecord>,
    #[serde(default)]
    pub pending_survey_scans: Vec<PendingSurveyScan>,
    #[serde(default)]
    pub pending_colony_foundings: Vec<PendingColonyFounding>,
    #[serde(default)]
    pub pending_colony_buildings: Vec<PendingColonyBuilding>,
    #[serde(default)]
    pub pending_population_transfers: Vec<PendingPopulationTransfer>,
    #[serde(default, with = "system_sim_serde")]
    pub system_sim: HashMap<SystemId, SystemSimState>,
    #[serde(default, with = "faction_relations_serde")]
    pub faction_relations: HashMap<(String, String), i16>,
    #[serde(default, with = "diplomacy_treaties_serde")]
    pub diplomacy_treaties: HashMap<(String, String), DiplomacyTreatyState>,
    #[serde(default)]
    pub active_sanctions: HashMap<(String, String), f32>,
    #[serde(default)]
    pub recent_powerplay_ops: Vec<PowerplayOperationRecord>,
    #[serde(default)]
    pub player_reputation: HashMap<String, i16>,
    #[serde(default)]
    pub missions: Vec<MissionState>,
    #[serde(default = "GameState::default_next_mission_id")]
    pub next_mission_id: u64,
    #[serde(default)]
    pub ai_build_telemetry: AiBuildTelemetry,
    pub explored_systems: HashSet<SystemId>,
    pub colonies: HashMap<u64, ColonyState>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ColonyBuildingCostPreview {
    pub target_level: u16,
    pub duration_years: f32,
    pub treasury: i64,
    pub food: f32,
    pub industry: f32,
    pub energy: f32,
    pub element_costs: Vec<(String, f32)>,
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(dead_code)]
pub struct ColonyBuildingEffectPreview {
    pub food_production_bonus: f32,
    pub industry_production_bonus: f32,
    pub energy_production_bonus: f32,
    pub food_demand_bonus: f32,
    pub industry_demand_bonus: f32,
    pub energy_demand_bonus: f32,
    pub element_extraction_bonus: f32,
    pub atmosphere_harvest_bonus: f32,
    pub treasury_production_bonus: f32,
    pub stability_bonus: f32,
    pub growth_bonus: f32,
    pub annual_upkeep: i64,
}

#[derive(Clone, Copy, Debug, Default)]
struct ColonyBuildingResourceCost {
    treasury: i64,
    food: f32,
    industry: f32,
    energy: f32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ElementCost {
    pub(crate) symbol: &'static str,
    pub(crate) amount: f32,
}

/// Groups of elements that can substitute for one another during construction.
/// Using a substitute costs `ELEMENT_SUBSTITUTION_PENALTY` times more material.
pub const ELEMENT_SUBSTITUTION_PENALTY: f32 = 1.5;

pub fn element_substitution_penalty(symbol: &str) -> f32 {
    match element_substitution_group(symbol) {
        Some("transition_structural") => 1.30,
        Some("refractory") => 1.95,
        Some("conductive") => 1.75,
        Some("metalloid") => 1.65,
        Some("nonmetal_organic") => 1.25,
        Some("atmospheric") => 1.10,
        Some("light_metal") => 1.45,
        Some("halogen_noble") => 1.55,
        _ => ELEMENT_SUBSTITUTION_PENALTY,
    }
}

/// Returns the substitution group for a given element symbol, if any.
/// Elements in the same group can substitute for each other.
#[allow(dead_code)]
pub fn element_substitution_group(symbol: &str) -> Option<&'static str> {
    match symbol {
        // Transition metals (structural)
        "Fe" | "Ni" | "Co" | "Mn" | "Cr" | "V" => Some("transition_structural"),
        // Refractory / heavy metals
        "Ti" | "W" | "Mo" | "Ir" | "Pt" => Some("refractory"),
        // Light metals
        "Al" | "Mg" | "Ca" | "Na" | "K" | "Li" => Some("light_metal"),
        // Conductive metals
        "Cu" | "Zn" => Some("conductive"),
        // Metalloids / semiconductor
        "Si" | "B" => Some("metalloid"),
        // Non-metals (life-essential)
        "C" | "S" | "P" => Some("nonmetal_organic"),
        // Atmospheric non-metals
        "N" | "O" | "H" | "He" => Some("atmospheric"),
        // Halogens / noble gases
        "F" | "Cl" | "Ne" | "Ar" => Some("halogen_noble"),
        _ => None,
    }
}

/// Returns the list of possible substitutes for `symbol` (excluding itself).
pub fn element_substitutes(symbol: &str) -> &'static [&'static str] {
    match symbol {
        "Fe" => &["Ni", "Co", "Mn", "Cr", "V"],
        "Ni" => &["Fe", "Co", "Mn", "Cr", "V"],
        "Co" => &["Fe", "Ni", "Mn", "Cr", "V"],
        "Mn" => &["Fe", "Ni", "Co", "Cr", "V"],
        "Cr" => &["Fe", "Ni", "Co", "Mn", "V"],
        "V"  => &["Fe", "Ni", "Co", "Mn", "Cr"],
        "Ti" => &["W", "Mo", "Ir", "Pt"],
        "W"  => &["Ti", "Mo", "Ir", "Pt"],
        "Mo" => &["Ti", "W", "Ir", "Pt"],
        "Ir" => &["Ti", "W", "Mo", "Pt"],
        "Pt" => &["Ti", "W", "Mo", "Ir"],
        "Al" => &["Mg", "Ca", "Na", "K", "Li"],
        "Mg" => &["Al", "Ca", "Na", "K", "Li"],
        "Ca" => &["Al", "Mg", "Na", "K", "Li"],
        "Na" => &["Al", "Mg", "Ca", "K", "Li"],
        "K"  => &["Al", "Mg", "Ca", "Na", "Li"],
        "Li" => &["Al", "Mg", "Ca", "Na", "K"],
        "Cu" => &["Zn"],
        "Zn" => &["Cu"],
        "Si" => &["B"],
        "B"  => &["Si"],
        "C"  => &["S", "P"],
        "S"  => &["C", "P"],
        "P"  => &["C", "S"],
        "N"  => &["O", "H"],
        "O"  => &["N", "H"],
        "H"  => &["N", "O"],
        _ => &[],
    }
}

/// Resolves element costs against a colony's stockpiles, using substitutes
/// where the primary element is insufficient. Returns the resolved costs as
/// (symbol, amount) pairs drawing from actual available stockpiles.
/// Returns `None` if costs cannot be met even with substitution.
pub fn resolve_element_costs_with_substitution(
    stockpiles: &HashMap<String, f32>,
    element_costs: &[ElementCost],
) -> Option<Vec<(String, f32)>> {
    // Track remaining availability after each allocation.
    let mut remaining: HashMap<&str, f32> = HashMap::new();
    for (sym, &amt) in stockpiles.iter() {
        remaining.insert(sym.as_str(), amt);
    }

    let mut resolved: Vec<(String, f32)> = Vec::new();

    for cost in element_costs {
        let available = remaining.get(cost.symbol).copied().unwrap_or(0.0);
        if available + 0.0001 >= cost.amount {
            // Can afford directly.
            *remaining.entry(cost.symbol).or_insert(0.0) -= cost.amount;
            resolved.push((cost.symbol.to_owned(), cost.amount));
            continue;
        }

        // Use what we have of the primary element, then fill remainder with substitutes.
        let mut still_needed = cost.amount - available.max(0.0);
        let primary_used = available.max(0.0);
        if primary_used > 0.0 {
            *remaining.entry(cost.symbol).or_insert(0.0) = 0.0;
            resolved.push((cost.symbol.to_owned(), primary_used));
        }

        let substitutes = element_substitutes(cost.symbol);
        for &sub in substitutes {
            if still_needed <= 0.0001 {
                break;
            }
            let sub_available = remaining.get(sub).copied().unwrap_or(0.0);
            if sub_available < 0.01 {
                continue;
            }
            // Substitutes cost more (penalty multiplier).
            let penalty = element_substitution_penalty(cost.symbol);
            let sub_needed = still_needed * penalty;
            let sub_used = sub_available.min(sub_needed);
            *remaining.entry(sub).or_insert(0.0) -= sub_used;
            // How much of the original requirement does this cover?
            let original_covered = sub_used / penalty;
            still_needed -= original_covered;
            resolved.push((sub.to_owned(), sub_used));
        }

        if still_needed > 0.0001 {
            return None; // Cannot meet this cost even with substitution.
        }
    }

    Some(resolved)
}

#[derive(Clone, Copy, Debug)]
pub struct ElementCostScale {
    pub symbol: &'static str,
    pub base: f32,
    pub step_per_level: f32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ColonyBuildingPerLevelModifiers {
    pub food_production_bonus: f32,
    pub industry_production_bonus: f32,
    pub energy_production_bonus: f32,
    pub food_demand_bonus: f32,
    pub industry_demand_bonus: f32,
    pub energy_demand_bonus: f32,
    pub element_extraction_bonus: f32,
    pub atmosphere_harvest_bonus: f32,
    pub treasury_production_bonus: f32,
    pub stability_bonus: f32,
    pub growth_bonus: f32,
    pub annual_upkeep: i64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ColonyBuildingEconomyProfile {
    pub treasury_base_cost: i64,
    pub treasury_level_step: i64,
    pub food_base_cost: f32,
    pub food_level_step: f32,
    pub industry_base_cost: f32,
    pub industry_level_step: f32,
    pub energy_base_cost: f32,
    pub energy_level_step: f32,
    pub duration_base_years: f32,
    pub duration_level_step_years: f32,
    pub per_level_modifiers: ColonyBuildingPerLevelModifiers,
}

/// Describes which site category a building can be placed on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColonyBuildingSiteType {
    Orbital,
    Planet,
    Star,
}

/// Complete, self-contained definition of a colony building type.
///
/// To add a new building kind, create a new `const ColonyBuildingDefinition`,
/// add the enum variant to [`ColonyBuildingKind`], and register it in
/// [`ColonyBuildingKind::definition`].  Everything else (UI, costs, tick
/// effects) is derived automatically from the data here.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub struct ColonyBuildingDefinition {
    pub label: &'static str,
    pub queue_button_label: &'static str,
    pub max_level: u16,
    pub role_description: &'static str,
    pub site_type: ColonyBuildingSiteType,
    pub requires_solid_surface: bool,
    pub requires_atmosphere: bool,
    pub requires_scoopable_star: bool,
    pub is_player_queueable: bool,
    pub economy_profile: ColonyBuildingEconomyProfile,
    pub element_cost_scales: &'static [ElementCostScale],
}

// ---------------------------------------------------------------------------
// Static building definitions – one const per ColonyBuildingKind variant.
// To add a new building, duplicate an existing block, adjust the values,
// add the enum variant, and register it in ColonyBuildingKind::definition().
// ---------------------------------------------------------------------------

const BUILDING_DEF_SPACE_STATION: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Space Station",
    queue_button_label: "Station+",
    max_level: 1,
    role_description: "Automated orbital support layer that boosts baseline colony output.",
    site_type: ColonyBuildingSiteType::Orbital,
    requires_solid_surface: false,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: false,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 0,
        treasury_level_step: 0,
        food_base_cost: 0.0,
        food_level_step: 0.0,
        industry_base_cost: 0.0,
        industry_level_step: 0.0,
        energy_base_cost: 0.0,
        energy_level_step: 0.0,
        duration_base_years: 0.0,
        duration_level_step_years: 0.0,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.00022,
            industry_production_bonus: 0.00022,
            energy_production_bonus: 0.00022,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.0,
            energy_demand_bonus: 0.0,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 0.0,
            stability_bonus: 0.0,
            growth_bonus: 0.0,
            annual_upkeep: 400,
        },
    },
    element_cost_scales: &[],
};

const BUILDING_DEF_INDUSTRIAL_HUB: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Industrial Hub",
    queue_button_label: "Hub+",
    max_level: 4,
    role_description: "Heavy industry complex that increases manufacturing throughput at added energy demand.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: true,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 30_000,
        treasury_level_step: 22_000,
        food_base_cost: 3.5,
        food_level_step: 1.5,
        industry_base_cost: 14.0,
        industry_level_step: 7.5,
        energy_base_cost: 7.0,
        energy_level_step: 3.0,
        duration_base_years: 0.65,
        duration_level_step_years: 0.28,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0015,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.0,
            energy_demand_bonus: 0.00022,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 0.0,
            stability_bonus: 0.0,
            growth_bonus: 0.0,
            annual_upkeep: 800,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 6.0, step_per_level: 4.0 },
        ElementCostScale { symbol: "Al", base: 4.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Si", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Cu", base: 2.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Ti", base: 1.0, step_per_level: 1.0 },
        ElementCostScale { symbol: "Ni", base: 2.0, step_per_level: 1.0 },
    ],
};

const BUILDING_DEF_AGRI_DOME: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Agri Dome",
    queue_button_label: "Agri+",
    max_level: 4,
    role_description: "Controlled biosphere farms that raise food output at added industrial demand.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: true,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 25_000,
        treasury_level_step: 17_500,
        food_base_cost: 6.0,
        food_level_step: 3.0,
        industry_base_cost: 8.5,
        industry_level_step: 4.5,
        energy_base_cost: 5.0,
        energy_level_step: 2.5,
        // 0.65 * 0.9, 0.28 * 0.85
        duration_base_years: 0.585,
        duration_level_step_years: 0.238,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0017,
            industry_production_bonus: 0.0,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.00020,
            energy_demand_bonus: 0.0,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 0.0,
            stability_bonus: 0.0,
            growth_bonus: 0.0,
            annual_upkeep: 750,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 4.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Al", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Si", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "C",  base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "N",  base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "P",  base: 2.0, step_per_level: 1.0 },
        ElementCostScale { symbol: "S",  base: 2.0, step_per_level: 1.0 },
    ],
};

const BUILDING_DEF_DEEP_MANTLE_MINING: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Deep Mantle Mining Station",
    queue_button_label: "Mantle+",
    max_level: 4,
    role_description: "Planetary extraction shafts that mine element stockpiles based on local composition.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: true,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 44_000,
        treasury_level_step: 27_000,
        food_base_cost: 5.0,
        food_level_step: 2.5,
        industry_base_cost: 17.0,
        industry_level_step: 8.0,
        energy_base_cost: 10.0,
        energy_level_step: 5.0,
        // 0.65 * 1.15, 0.28 * 1.05
        duration_base_years: 0.7475,
        duration_level_step_years: 0.294,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.0,
            energy_demand_bonus: 0.00018,
            element_extraction_bonus: 0.055,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 0.0,
            stability_bonus: 0.0,
            growth_bonus: 0.0,
            annual_upkeep: 900,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 8.0, step_per_level: 5.0 },
        ElementCostScale { symbol: "Ni", base: 6.0, step_per_level: 4.0 },
        ElementCostScale { symbol: "Ti", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Si", base: 6.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Al", base: 5.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Cu", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "W",  base: 1.0, step_per_level: 1.0 },
        ElementCostScale { symbol: "Mo", base: 1.0, step_per_level: 1.0 },
    ],
};

const BUILDING_DEF_ATMOSPHERE_HARVESTER: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Atmosphere Harvester",
    queue_button_label: "Atmo+",
    max_level: 4,
    role_description: "High-altitude processing rigs that harvest atmospheric gas stockpiles by gas mix and pressure.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: false,
    requires_atmosphere: true,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 35_000,
        treasury_level_step: 23_000,
        food_base_cost: 3.5,
        food_level_step: 1.5,
        industry_base_cost: 12.0,
        industry_level_step: 5.5,
        energy_base_cost: 8.5,
        energy_level_step: 4.0,
        duration_base_years: 0.65,
        duration_level_step_years: 0.28,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.0,
            energy_demand_bonus: 0.00018,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.050,
            treasury_production_bonus: 0.0,
            stability_bonus: 0.0,
            growth_bonus: 0.0,
            annual_upkeep: 700,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Al", base: 5.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Ti", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "C",  base: 5.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "N",  base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Si", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Cu", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Ni", base: 2.0, step_per_level: 1.0 },
        ElementCostScale { symbol: "P",  base: 2.0, step_per_level: 1.0 },
    ],
};

const BUILDING_DEF_FUEL_SCOOP_DRONE_SWARM: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Fuel Scoop Drone Swarm",
    queue_button_label: "Scoop+",
    max_level: 4,
    role_description: "Autonomous stellar scoop drones that harvest plasma from hydrogen-fusing stars into colony energy output.",
    site_type: ColonyBuildingSiteType::Star,
    requires_solid_surface: false,
    requires_atmosphere: false,
    requires_scoopable_star: true,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 40_000,
        treasury_level_step: 24_000,
        food_base_cost: 3.5,
        food_level_step: 1.5,
        industry_base_cost: 15.0,
        industry_level_step: 7.0,
        energy_base_cost: 7.5,
        energy_level_step: 3.0,
        // 0.65 * 1.05
        duration_base_years: 0.6825,
        duration_level_step_years: 0.28,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0,
            energy_production_bonus: 0.0022,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.00008,
            energy_demand_bonus: 0.0,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 0.0,
            stability_bonus: 0.0,
            growth_bonus: 0.0,
            annual_upkeep: 650,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Al", base: 6.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Ti", base: 5.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Ni", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Cu", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Si", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "C",  base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "W",  base: 1.0, step_per_level: 1.0 },
        ElementCostScale { symbol: "Mo", base: 1.0, step_per_level: 1.0 },
    ],
};

const BUILDING_DEF_TRADING_HUB: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Trading Hub",
    queue_button_label: "Trade+",
    max_level: 4,
    role_description: "Commercial exchange complex that generates treasury income scaled by colony population.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: true,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 28_000,
        treasury_level_step: 20_000,
        food_base_cost: 3.0,
        food_level_step: 1.5,
        industry_base_cost: 10.0,
        industry_level_step: 5.0,
        energy_base_cost: 6.0,
        energy_level_step: 3.0,
        // 0.65 * 0.95, 0.28 * 0.90
        duration_base_years: 0.6175,
        duration_level_step_years: 0.252,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.0,
            energy_demand_bonus: 0.00015,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 2_200.0,
            stability_bonus: 0.0,
            growth_bonus: 0.0,
            annual_upkeep: 1_500,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 4.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Al", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Cu", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Si", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Ti", base: 1.0, step_per_level: 1.0 },
        ElementCostScale { symbol: "Ni", base: 1.0, step_per_level: 1.0 },
    ],
};

const BUILDING_DEF_ENTERTAINMENT_PLAZA: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Entertainment Plaza",
    queue_button_label: "Ent+",
    max_level: 4,
    role_description: "Public recreation and cultural district that boosts colony stability and population growth.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: true,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 22_000,
        treasury_level_step: 16_000,
        food_base_cost: 4.0,
        food_level_step: 2.0,
        industry_base_cost: 9.0,
        industry_level_step: 4.5,
        energy_base_cost: 5.5,
        energy_level_step: 2.5,
        // 0.65 * 0.85, 0.28 * 0.80
        duration_base_years: 0.5525,
        duration_level_step_years: 0.224,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.00018,
            industry_demand_bonus: 0.0,
            energy_demand_bonus: 0.0,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 0.0,
            stability_bonus: 0.0035,
            growth_bonus: 0.00025,
            annual_upkeep: 1_000,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Al", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Si", base: 2.0, step_per_level: 1.0 },
        ElementCostScale { symbol: "Cu", base: 2.0, step_per_level: 1.0 },
        ElementCostScale { symbol: "C",  base: 2.0, step_per_level: 1.0 },
    ],
};

const BUILDING_DEF_LOGISTICS_EXCHANGE: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Logistics Exchange",
    queue_button_label: "Logi+",
    max_level: 4,
    role_description: "Cargo routing and warehousing network that expands practical stockpile usage.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: true,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 30_000,
        treasury_level_step: 20_000,
        food_base_cost: 3.0,
        food_level_step: 1.5,
        industry_base_cost: 11.0,
        industry_level_step: 5.0,
        energy_base_cost: 6.0,
        energy_level_step: 3.0,
        duration_base_years: 0.62,
        duration_level_step_years: 0.25,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.00045,
            industry_production_bonus: 0.00045,
            energy_production_bonus: 0.00045,
            food_demand_bonus: -0.00008,
            industry_demand_bonus: -0.00008,
            energy_demand_bonus: -0.00006,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 350.0,
            stability_bonus: 0.0006,
            growth_bonus: 0.0,
            annual_upkeep: 1_200,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 5.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Al", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Si", base: 3.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Cu", base: 3.0, step_per_level: 2.0 },
    ],
};

const BUILDING_DEF_HABITAT_ARCOLOGY: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Habitat Arcology",
    queue_button_label: "Arcology+",
    max_level: 4,
    role_description: "Dense megahabitat blocks that improve growth and social resilience at high upkeep.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: true,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 55_000,
        treasury_level_step: 38_000,
        food_base_cost: 6.0,
        food_level_step: 2.5,
        industry_base_cost: 15.0,
        industry_level_step: 7.0,
        energy_base_cost: 11.0,
        energy_level_step: 5.0,
        duration_base_years: 0.90,
        duration_level_step_years: 0.35,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.00012,
            industry_demand_bonus: 0.00005,
            energy_demand_bonus: 0.00020,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 0.0,
            stability_bonus: 0.0025,
            growth_bonus: 0.00055,
            annual_upkeep: 2_200,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 7.0, step_per_level: 4.0 },
        ElementCostScale { symbol: "Al", base: 6.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Si", base: 5.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "C", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "N", base: 3.0, step_per_level: 2.0 },
    ],
};

const BUILDING_DEF_DEFENSE_GRID: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Defense Grid",
    queue_button_label: "Defense+",
    max_level: 4,
    role_description: "Orbital and planetary defense mesh that hardens colonies under instability.",
    site_type: ColonyBuildingSiteType::Orbital,
    requires_solid_surface: false,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 48_000,
        treasury_level_step: 30_000,
        food_base_cost: 3.0,
        food_level_step: 1.0,
        industry_base_cost: 14.0,
        industry_level_step: 6.0,
        energy_base_cost: 9.0,
        energy_level_step: 4.0,
        duration_base_years: 0.72,
        duration_level_step_years: 0.30,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.0,
            energy_demand_bonus: 0.00012,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 0.0,
            stability_bonus: 0.0012,
            growth_bonus: 0.0,
            annual_upkeep: 1_900,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 7.0, step_per_level: 4.0 },
        ElementCostScale { symbol: "Ni", base: 5.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Ti", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Cu", base: 3.0, step_per_level: 2.0 },
    ],
};

const BUILDING_DEF_SYSTEMS_ADMINISTRATION: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Systems Administration Nexus",
    queue_button_label: "Admin+",
    max_level: 4,
    role_description: "Civic administration complex that improves stability and faction income reliability.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: true,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 36_000,
        treasury_level_step: 24_000,
        food_base_cost: 3.0,
        food_level_step: 1.0,
        industry_base_cost: 9.0,
        industry_level_step: 4.0,
        energy_base_cost: 6.0,
        energy_level_step: 2.5,
        duration_base_years: 0.64,
        duration_level_step_years: 0.27,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0002,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.0,
            energy_demand_bonus: 0.00010,
            element_extraction_bonus: 0.0,
            atmosphere_harvest_bonus: 0.0,
            treasury_production_bonus: 1_250.0,
            stability_bonus: 0.0018,
            growth_bonus: 0.00015,
            annual_upkeep: 1_550,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 4.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Si", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Cu", base: 4.0, step_per_level: 2.0 },
        ElementCostScale { symbol: "Al", base: 3.0, step_per_level: 2.0 },
    ],
};

const BUILDING_DEF_CATALYTIC_REFINERY: ColonyBuildingDefinition = ColonyBuildingDefinition {
    label: "Catalytic Refinery",
    queue_button_label: "Refine+",
    max_level: 4,
    role_description: "Advanced refining chain that boosts extractive throughput at high power demand.",
    site_type: ColonyBuildingSiteType::Planet,
    requires_solid_surface: true,
    requires_atmosphere: false,
    requires_scoopable_star: false,
    is_player_queueable: true,
    economy_profile: ColonyBuildingEconomyProfile {
        treasury_base_cost: 52_000,
        treasury_level_step: 34_000,
        food_base_cost: 3.0,
        food_level_step: 1.5,
        industry_base_cost: 16.0,
        industry_level_step: 8.0,
        energy_base_cost: 12.0,
        energy_level_step: 5.0,
        duration_base_years: 0.78,
        duration_level_step_years: 0.32,
        per_level_modifiers: ColonyBuildingPerLevelModifiers {
            food_production_bonus: 0.0,
            industry_production_bonus: 0.0009,
            energy_production_bonus: 0.0,
            food_demand_bonus: 0.0,
            industry_demand_bonus: 0.00010,
            energy_demand_bonus: 0.00025,
            element_extraction_bonus: 0.045,
            atmosphere_harvest_bonus: 0.015,
            treasury_production_bonus: 300.0,
            stability_bonus: 0.0,
            growth_bonus: 0.0,
            annual_upkeep: 2_100,
        },
    },
    element_cost_scales: &[
        ElementCostScale { symbol: "Fe", base: 6.0, step_per_level: 4.0 },
        ElementCostScale { symbol: "Ni", base: 5.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Ti", base: 4.0, step_per_level: 3.0 },
        ElementCostScale { symbol: "Mo", base: 2.0, step_per_level: 1.0 },
        ElementCostScale { symbol: "W", base: 1.0, step_per_level: 1.0 },
    ],
};

impl ColonyBuildingKind {
    fn economy_profile(self) -> ColonyBuildingEconomyProfile {
        self.definition().economy_profile
    }

    fn element_cost_scale(self) -> &'static [ElementCostScale] {
        self.definition().element_cost_scales
    }
}

impl Default for GameState {
    fn default() -> Self {
        let mut factions = HashMap::new();
        factions.insert(
            PLAYER_FACTION_ID.to_owned(),
            FactionState {
                id: PLAYER_FACTION_ID.to_owned(),
                display_name: "Flood of Raccoons".to_owned(),
                treasury: 1_250_000,
                colonization_tech_level: 0,
                colonization_tech_progress: 0.0,
                starting_colony_id: None,
            },
        );
        factions.insert(
            "brewer-corporation".to_owned(),
            FactionState {
                id: "brewer-corporation".to_owned(),
                display_name: "Brewer Corporation".to_owned(),
                treasury: 2_800_000,
                colonization_tech_level: 0,
                colonization_tech_progress: 0.0,
                starting_colony_id: None,
            },
        );
        factions.insert(
            "wanderers-library".to_owned(),
            FactionState {
                id: "wanderers-library".to_owned(),
                display_name: "Wanderer's Library".to_owned(),
                treasury: 2_100_000,
                colonization_tech_level: 0,
                colonization_tech_progress: 0.0,
                starting_colony_id: None,
            },
        );
        factions.insert(
            "drifters".to_owned(),
            FactionState {
                id: "drifters".to_owned(),
                display_name: "Drifters".to_owned(),
                treasury: 2_000_000,
                colonization_tech_level: 0,
                colonization_tech_progress: 0.0,
                starting_colony_id: None,
            },
        );
        factions.insert(
            "new-providence".to_owned(),
            FactionState {
                id: "new-providence".to_owned(),
                display_name: "New Providence".to_owned(),
                treasury: 2_250_000,
                colonization_tech_level: 0,
                colonization_tech_progress: 0.0,
                starting_colony_id: None,
            },
        );
        factions.insert(
            "hypercapitalist-foundation".to_owned(),
            FactionState {
                id: "hypercapitalist-foundation".to_owned(),
                display_name: "Hypercapitalist Foundation".to_owned(),
                treasury: 3_000_000,
                colonization_tech_level: 0,
                colonization_tech_progress: 0.0,
                starting_colony_id: None,
            },
        );
        factions.insert(
            "greater-armenia".to_owned(),
            FactionState {
                id: "greater-armenia".to_owned(),
                display_name: "Greater Armenia".to_owned(),
                treasury: 2_150_000,
                colonization_tech_level: 0,
                colonization_tech_progress: 0.0,
                starting_colony_id: None,
            },
        );
        factions.insert(
            "battle-pilgrims".to_owned(),
            FactionState {
                id: "battle-pilgrims".to_owned(),
                display_name: "Battle Pilgrims".to_owned(),
                treasury: 2_350_000,
                colonization_tech_level: 0,
                colonization_tech_progress: 0.0,
                starting_colony_id: None,
            },
        );

        Self {
            current_year: 3300.0,
            next_colony_id: 1,
            base_colonization_range_world: Self::default_base_colonization_range_world(),
            player: PlayerState::default(),
            factions,
            survey_records: HashMap::new(),
            pending_survey_scans: Vec::new(),
            pending_colony_foundings: Vec::new(),
            pending_colony_buildings: Vec::new(),
            pending_population_transfers: Vec::new(),
            system_sim: HashMap::new(),
            faction_relations: HashMap::new(),
            diplomacy_treaties: HashMap::new(),
            active_sanctions: HashMap::new(),
            recent_powerplay_ops: Vec::new(),
            player_reputation: HashMap::new(),
            missions: Vec::new(),
            next_mission_id: Self::default_next_mission_id(),
            ai_build_telemetry: AiBuildTelemetry::default(),
            explored_systems: HashSet::new(),
            colonies: HashMap::new(),
        }
    }
}

impl GameState {
    const COLONIZATION_RANGE_PER_TECH_LEVEL_WORLD: f32 = 10.0;
    const ELEMENT_STOCKPILE_CAPACITY_MULTIPLIER: f32 = 12.0;
    const BASE_BIRTH_RATE_ANNUAL: f64 = 0.0045;
    const STARTUP_BIRTH_BOOST_ANNUAL: f64 = 0.0080;
    const STARTUP_GROWTH_CURVE: f64 = 0.70;
    const BASE_DEATH_RATE_ANNUAL: f64 = 0.0048;
    const STABILITY_DEATH_PENALTY_ANNUAL: f64 = 0.0042;
    const MIN_ANNUAL_GROWTH_HABITABLE: f64 = -0.008;
    const MIN_ANNUAL_GROWTH_HOSTILE: f64 = -0.011;
    const TAXABLE_POPULATION_SATURATION: f64 = 9_000_000.0;
    const POPULATION_UPKEEP_LINEAR_PER_PERSON: f64 = 0.016;
    const POPULATION_UPKEEP_QUADRATIC_PER_PERSON_SQUARED: f64 = 0.000000004;
    const STARTING_COLONY_MIN_POPULATION: u32 = 10_000;
    const MISSION_REFRESH_INTERVAL_YEARS: f32 = 0.65;
    const CONSTRUCTION_UPFRONT_PAYMENT_RATIO: f32 = 0.60;
    const CONSTRUCTION_ANNUAL_UPKEEP_RATIO: f32 = 0.16;

    fn default_next_mission_id() -> u64 {
        1
    }

    fn colony_stage_for_population(population: f64) -> ColonyStage {
        if population >= 46_000_000.0 {
            ColonyStage::CoreWorld
        } else if population >= 2_100_000.0 {
            ColonyStage::City
        } else if population >= 120_000.0 {
            ColonyStage::Settlement
        } else {
            ColonyStage::Outpost
        }
    }

    fn stage_output_multiplier(population: f64, earth_like_world: bool) -> f32 {
        let pop = population.max(0.0);
        let outpost = 0.92f32;
        let settlement = 1.00f32;
        let city = 1.13f32;
        let core = 1.26f32;
        let base = if pop < 120_000.0 {
            outpost
        } else if pop < 2_100_000.0 {
            let t = ((pop - 120_000.0) / (2_100_000.0 - 120_000.0)) as f32;
            outpost + (settlement - outpost) * t
        } else if pop < 46_000_000.0 {
            let t = ((pop - 2_100_000.0) / (46_000_000.0 - 2_100_000.0)) as f32;
            settlement + (city - settlement) * t
        } else {
            core
        };
        if earth_like_world {
            base * 1.06
        } else {
            base
        }
    }

    fn element_distribution_weights() -> &'static [(&'static str, f32)] {
        const WEIGHTS: [(&str, f32); 12] = [
            ("Fe", 0.22),
            ("Si", 0.16),
            ("Al", 0.10),
            ("Cu", 0.06),
            ("Ti", 0.04),
            ("Ni", 0.05),
            ("C", 0.08),
            ("N", 0.08),
            ("O", 0.10),
            ("Mg", 0.06),
            ("S", 0.03),
            ("P", 0.02),
        ];
        &WEIGHTS
    }

    fn default_element_stockpiles_for_site(
        earth_like_world: bool,
        habitable_site: bool,
    ) -> HashMap<String, f32> {
        let base_total = if earth_like_world {
            140.0
        } else if habitable_site {
            95.0
        } else {
            60.0
        };

        let mut stockpiles = HashMap::new();
        for (symbol, weight) in Self::element_distribution_weights() {
            stockpiles.insert((*symbol).to_owned(), base_total * *weight);
        }
        stockpiles
    }

    fn starting_colony_element_stockpile_targets() -> &'static [(&'static str, f32)] {
        const TARGETS: [(&str, f32); 12] = [
            ("Fe", 140.0),
            ("Si", 120.0),
            ("Al", 100.0),
            ("Cu", 75.0),
            ("Ti", 70.0),
            ("Ni", 80.0),
            ("C", 95.0),
            ("N", 90.0),
            ("P", 45.0),
            ("S", 45.0),
            ("W", 20.0),
            ("Mo", 20.0),
        ];
        &TARGETS
    }

    fn seed_starting_colony_element_stockpiles(colony: &mut ColonyState) {
        for (symbol, min_amount) in Self::starting_colony_element_stockpile_targets() {
            let entry = colony
                .element_stockpiles
                .entry((*symbol).to_owned())
                .or_insert(0.0);
            *entry = entry.max(*min_amount);
        }
    }

    fn default_element_resource_profile() -> HashMap<String, f32> {
        Self::element_distribution_weights()
            .iter()
            .map(|(symbol, weight)| ((*symbol).to_owned(), *weight))
            .collect()
    }

    fn normalize_resource_profile(raw: &HashMap<String, f32>) -> HashMap<String, f32> {
        let total = raw
            .values()
            .copied()
            .filter(|value| value.is_finite() && *value > 0.0)
            .sum::<f32>();

        if total <= f32::EPSILON {
            return HashMap::new();
        }

        raw.iter()
            .filter_map(|(key, value)| {
                if value.is_finite() && *value > 0.0 {
                    Some((key.clone(), *value / total))
                } else {
                    None
                }
            })
            .collect()
    }

    fn normalized_element_resource_profile(raw: &HashMap<String, f32>) -> HashMap<String, f32> {
        let normalized = Self::normalize_resource_profile(raw);
        if normalized.is_empty() {
            Self::default_element_resource_profile()
        } else {
            normalized
        }
    }

    fn normalized_atmosphere_resource_profile(raw: &HashMap<String, f32>) -> HashMap<String, f32> {
        Self::normalize_resource_profile(raw)
    }

    fn add_profiled_stockpile(
        stockpiles: &mut HashMap<String, f32>,
        profile: &HashMap<String, f32>,
        total_addition: f32,
    ) {
        if total_addition <= 0.0 || profile.is_empty() {
            return;
        }

        for (symbol, weight) in profile {
            if !weight.is_finite() || *weight <= 0.0 {
                continue;
            }
            let entry = stockpiles.entry(symbol.clone()).or_insert(0.0);
            *entry += total_addition * *weight;
        }
    }

    fn colony_building_resource_cost(
        kind: ColonyBuildingKind,
        target_level: u16,
    ) -> ColonyBuildingResourceCost {
        let level_offset_i64 = i64::from(target_level.max(1).saturating_sub(1));
        let level_offset_f32 = f32::from(target_level.max(1).saturating_sub(1));
        let profile = kind.economy_profile();

        ColonyBuildingResourceCost {
            treasury: profile
                .treasury_base_cost
                .saturating_add(level_offset_i64.saturating_mul(profile.treasury_level_step)),
            food: profile.food_base_cost + level_offset_f32 * profile.food_level_step,
            industry: profile.industry_base_cost + level_offset_f32 * profile.industry_level_step,
            energy: profile.energy_base_cost + level_offset_f32 * profile.energy_level_step,
        }
    }

    fn colony_building_element_costs(kind: ColonyBuildingKind, target_level: u16) -> Vec<ElementCost> {
        let level_offset_f32 = f32::from(target_level.max(1).saturating_sub(1));
        kind.element_cost_scale()
            .iter()
            .map(|entry| ElementCost {
                symbol: entry.symbol,
                amount: entry.base + level_offset_f32 * entry.step_per_level,
            })
            .collect()
    }

    pub fn colony_building_cost_preview(
        kind: ColonyBuildingKind,
        target_level: u16,
    ) -> ColonyBuildingCostPreview {
        let normalized_target_level = target_level.max(1);
        let resource_cost = Self::colony_building_resource_cost(kind, normalized_target_level);
        let element_costs = Self::colony_building_element_costs(kind, normalized_target_level)
            .into_iter()
            .map(|entry| (entry.symbol.to_owned(), entry.amount))
            .collect::<Vec<_>>();

        ColonyBuildingCostPreview {
            target_level: normalized_target_level,
            duration_years: Self::colony_building_duration_years(kind, normalized_target_level),
            treasury: resource_cost.treasury,
            food: resource_cost.food,
            industry: resource_cost.industry,
            energy: resource_cost.energy,
            element_costs,
        }
    }

    fn can_afford_colony_resource_cost(
        colony: &ColonyState,
        cost: ColonyBuildingResourceCost,
        element_costs: &[ElementCost],
    ) -> bool {
        colony.food_stockpile + 0.0001 >= cost.food
            && colony.industry_stockpile + 0.0001 >= cost.industry
            && colony.energy_stockpile + 0.0001 >= cost.energy
            && resolve_element_costs_with_substitution(&colony.element_stockpiles, element_costs)
                .is_some()
    }

    fn spend_colony_resource_cost(
        colony: &mut ColonyState,
        cost: ColonyBuildingResourceCost,
        element_costs: &[ElementCost],
    ) {
        // Resolve with substitution, then spend accordingly.
        if let Some(resolved) = resolve_element_costs_with_substitution(&colony.element_stockpiles, element_costs) {
            for (symbol, amount) in resolved {
                let value = colony
                    .element_stockpiles
                    .entry(symbol)
                    .or_insert(0.0);
                *value = (*value - amount).max(0.0);
            }
        } else {
            // Fallback: direct deduction (should not happen if can_afford was checked).
            for entry in element_costs {
                let value = colony
                    .element_stockpiles
                    .entry(entry.symbol.to_owned())
                    .or_insert(0.0);
                *value = (*value - entry.amount).max(0.0);
            }
        }
        colony.food_stockpile = (colony.food_stockpile - cost.food).max(0.0);
        colony.industry_stockpile = (colony.industry_stockpile - cost.industry).max(0.0);
        colony.energy_stockpile = (colony.energy_stockpile - cost.energy).max(0.0);
    }

    fn total_resource_stockpile(stockpiles: &HashMap<String, f32>) -> f32 {
        stockpiles.values().copied().sum()
    }

    fn total_element_stockpile(colony: &ColonyState) -> f32 {
        Self::total_resource_stockpile(&colony.element_stockpiles)
    }

    fn default_base_colonization_range_world() -> f32 {
        100.0
    }

    #[allow(dead_code)]
    pub fn planet_building_slot_capacity_for_radius(radius_earth: f32) -> u16 {
        let radius = radius_earth.max(0.02);
        if radius < 0.45 {
            1
        } else if radius < 0.80 {
            2
        } else if radius < 1.20 {
            3
        } else if radius < 1.80 {
            4
        } else if radius < 2.50 {
            5
        } else {
            6
        }
    }

    pub fn building_site_slot_capacity(
        site: ColonyBuildingSite,
        site_profile: ColonyBuildingSiteProfile,
    ) -> Option<u16> {
        if matches!(site, ColonyBuildingSite::Planet(_)) {
            site_profile.planet_building_slot_capacity
        } else {
            None
        }
    }

    fn colony_building_duration_years(kind: ColonyBuildingKind, target_level: u16) -> f32 {
        let level_offset_f32 = f32::from(target_level.max(1).saturating_sub(1));
        let profile = kind.economy_profile();
        (profile.duration_base_years + level_offset_f32 * profile.duration_level_step_years)
            .max(0.0)
    }

    fn colony_building_modifiers(colony: &ColonyState) -> (f32, f32, f32, f32, f32, f32, f32, f32, f32, f32, f32, i64) {
        let mut totals = ColonyBuildingPerLevelModifiers::default();
        for building in &colony.buildings {
            let level = building.level.max(1) as f32;
            let profile = building.kind.economy_profile().per_level_modifiers;

            totals.food_production_bonus += profile.food_production_bonus * level;
            totals.industry_production_bonus += profile.industry_production_bonus * level;
            totals.energy_production_bonus += profile.energy_production_bonus * level;
            totals.food_demand_bonus += profile.food_demand_bonus * level;
            totals.industry_demand_bonus += profile.industry_demand_bonus * level;
            totals.energy_demand_bonus += profile.energy_demand_bonus * level;
            totals.element_extraction_bonus += profile.element_extraction_bonus * level;
            totals.atmosphere_harvest_bonus += profile.atmosphere_harvest_bonus * level;
            totals.treasury_production_bonus += profile.treasury_production_bonus * level;
            totals.stability_bonus += profile.stability_bonus * level;
            totals.growth_bonus += profile.growth_bonus * level;
            totals.annual_upkeep = totals.annual_upkeep.saturating_add(
                profile
                    .annual_upkeep
                    .saturating_mul(i64::from(building.level.max(1))),
            );
        }

        (
            totals.food_production_bonus,
            totals.industry_production_bonus,
            totals.energy_production_bonus,
            totals.food_demand_bonus,
            totals.industry_demand_bonus,
            totals.energy_demand_bonus,
            totals.element_extraction_bonus,
            totals.atmosphere_harvest_bonus,
            totals.treasury_production_bonus,
            totals.stability_bonus,
            totals.growth_bonus,
            totals.annual_upkeep,
        )
    }

    pub fn is_building_site_supported(kind: ColonyBuildingKind, site: ColonyBuildingSite) -> bool {
        kind.supports_site(site)
    }

    pub fn building_site_support_error(
        kind: ColonyBuildingKind,
        site: ColonyBuildingSite,
        site_profile: ColonyBuildingSiteProfile,
    ) -> Option<&'static str> {
        if !Self::is_building_site_supported(kind, site) {
            return Some("That building cannot be constructed at the selected site type.");
        }

        if matches!(site, ColonyBuildingSite::Planet(_)) {
            if site_profile.planet_is_gas_giant == Some(true)
                && kind.requires_solid_planet_surface()
            {
                return Some("This building requires a solid planet surface (not a gas giant).");
            }

            if kind.requires_atmosphere() && site_profile.planet_has_atmosphere == Some(false) {
                return Some("This building requires a planet with a sustained atmosphere.");
            }
        }

        if matches!(site, ColonyBuildingSite::Star(_)) {
            if kind.requires_scoopable_star() && site_profile.star_is_scoopable != Some(true) {
                return Some("This building requires a hydrogen-fusing star (spectral class O, B, A, F, G, K, or M).");
            }
        }

        None
    }

    #[allow(dead_code)]
    pub fn player_faction_name(&self) -> &str {
        self.factions
            .get(&self.player.faction_id)
            .map(|f| f.display_name.as_str())
            .unwrap_or("Player")
    }

    fn resolve_faction_id(&self, token: &str) -> Option<String> {
        if self.factions.contains_key(token) {
            return Some(token.to_owned());
        }

        self.factions.iter().find_map(|(id, faction)| {
            if faction.display_name == token {
                Some(id.clone())
            } else {
                None
            }
        })
    }

    fn award_exploration_rewards(&mut self, faction_token: &str, stage: SurveyStage) {
        let (faction_treasury, tech_progress, reputation_gain) = Self::survey_stage_rewards(stage);
        if faction_treasury == 0 && tech_progress <= 0.0 && reputation_gain == 0 {
            return;
        }

        let Some(faction_id) = self.resolve_faction_id(faction_token) else {
            return;
        };

        if let Some(faction) = self.factions.get_mut(&faction_id) {
            faction.treasury = faction.treasury.saturating_add(faction_treasury);
            faction.colonization_tech_progress += tech_progress;
            while faction.colonization_tech_progress >= 1.0 {
                faction.colonization_tech_progress -= 1.0;
                faction.colonization_tech_level = faction.colonization_tech_level.saturating_add(1);
            }
        }
        let rep_entry = self.player_reputation.entry(faction_id).or_insert(0);
        *rep_entry = (*rep_entry + reputation_gain).clamp(-100, 100);
    }

    pub fn survey_stage_rewards(stage: SurveyStage) -> (i64, f32, i16) {
        match stage {
            SurveyStage::Unknown => (0, 0.0, 0),
            SurveyStage::Located => (350, 0.10, 1),
            SurveyStage::StellarSurvey => (600, 0.14, 2),
            SurveyStage::PlanetarySurvey => (950, 0.20, 3),
            SurveyStage::ColonyAssessment => (1_600, 0.28, 5),
        }
    }

    fn relation_key(a: &str, b: &str) -> (String, String) {
        if a <= b {
            (a.to_owned(), b.to_owned())
        } else {
            (b.to_owned(), a.to_owned())
        }
    }

    fn get_relation(&self, a: &str, b: &str) -> i16 {
        if a == b {
            return 100;
        }
        self.faction_relations
            .get(&Self::relation_key(a, b))
            .copied()
            .unwrap_or(0)
    }

    fn set_relation(&mut self, a: &str, b: &str, value: i16) {
        if a == b {
            return;
        }
        self.faction_relations
            .insert(Self::relation_key(a, b), value.clamp(-100, 100));
    }

    pub fn relation_between(&self, a: &str, b: &str) -> i16 {
        self.get_relation(a, b)
    }

    pub fn treaty_between(&self, a: &str, b: &str) -> Option<&DiplomacyTreatyState> {
        self.diplomacy_treaties.get(&Self::relation_key(a, b))
    }

    pub fn hostility_score_between(&self, a: &str, b: &str) -> f32 {
        let mut hostility = -(self.get_relation(a, b) as f32) / 100.0;
        if let Some(treaty) = self.treaty_between(a, b) {
            hostility -= match treaty.kind {
                DiplomaticTreatyKind::Alliance => 0.55,
                DiplomaticTreatyKind::NonAggressionPact => 0.30,
            };
        }
        if self.has_sanction(a, b) || self.has_sanction(b, a) {
            hostility += 0.35;
        }
        hostility.clamp(-1.0, 1.0)
    }

    pub fn has_sanction(&self, by_faction: &str, target_faction: &str) -> bool {
        self.active_sanctions
            .get(&(by_faction.to_owned(), target_faction.to_owned()))
            .is_some_and(|expires| *expires > self.current_year)
    }

    pub fn diplomacy_summary_counts(&self) -> (usize, usize, usize) {
        let alliance_count = self
            .diplomacy_treaties
            .values()
            .filter(|t| t.kind == DiplomaticTreatyKind::Alliance && t.expires_year > self.current_year)
            .count();
        let pact_count = self
            .diplomacy_treaties
            .values()
            .filter(|t| {
                t.kind == DiplomaticTreatyKind::NonAggressionPact && t.expires_year > self.current_year
            })
            .count();
        let sanction_count = self
            .active_sanctions
            .values()
            .filter(|expires| **expires > self.current_year)
            .count();
        (alliance_count, pact_count, sanction_count)
    }

    fn ensure_system_sim_state(&mut self, system: SystemId) -> &mut SystemSimState {
        self.system_sim.entry(system).or_insert_with(|| SystemSimState {
            system,
            ..SystemSimState::default()
        })
    }

    fn regenerate_missions(&mut self) {
        self.missions.retain(|mission| mission.expires_year > self.current_year);
        if self.missions.len() >= 8 {
            return;
        }
        let mut candidate_systems: Vec<SystemSimState> = self
            .system_sim
            .values()
            .filter(|sim| sim.econ_pressure > 0.25 || sim.conflict != ConflictState::Calm)
            .cloned()
            .collect();
        candidate_systems.sort_by(|a, b| b.econ_pressure.total_cmp(&a.econ_pressure));
        candidate_systems.truncate(6);

        for sim in candidate_systems {
            if self.missions.iter().any(|m| m.target_system == sim.system) {
                continue;
            }
            let mut dominant = self.player.faction_id.clone();
            let mut dominant_influence = -1.0_f32;
            let mut second = self.player.faction_id.clone();
            let mut second_influence = -1.0_f32;
            for (faction_id, influence) in &sim.influence_by_faction {
                if *influence > dominant_influence {
                    second = dominant.clone();
                    second_influence = dominant_influence;
                    dominant = faction_id.clone();
                    dominant_influence = *influence;
                } else if *influence > second_influence {
                    second = faction_id.clone();
                    second_influence = *influence;
                }
            }
            let treaty_kind = self.treaty_between(&dominant, &second).map(|t| t.kind);
            let sanction_pressure = self.has_sanction(&dominant, &second)
                || self.has_sanction(&second, &dominant);
            let hostility = self.hostility_score_between(&dominant, &second);
            let (kind, title, description, reward_rep, risk) = if sanction_pressure {
                (
                    MissionKind::SanctionRunning,
                    "Sanction Pressure Run".to_owned(),
                    "Move critical supplies through hostile trade filters before shortages spread."
                        .to_owned(),
                    6,
                    0.60,
                )
            } else if treaty_kind == Some(DiplomaticTreatyKind::Alliance) {
                (
                    MissionKind::AllianceSupport,
                    "Alliance Support Convoy".to_owned(),
                    "Reinforce allied logistics and maintain bloc cohesion under regional stress."
                        .to_owned(),
                    4,
                    0.40,
                )
            } else {
                match sim.conflict {
                ConflictState::Embargo => (
                    MissionKind::SupplyRelief,
                    "Break the Embargo".to_owned(),
                    "Deliver emergency goods and stabilize local markets.".to_owned(),
                    5,
                    0.45,
                ),
                ConflictState::ProxyWar | ConflictState::PatrolSurge => (
                    MissionKind::ReconSweep,
                    "Recon Conflict Lanes".to_owned(),
                    "Map patrol activity and recover tactical intel.".to_owned(),
                    4,
                    0.55,
                ),
                _ => (
                    MissionKind::InfluenceOp,
                    if hostility > 0.45 {
                        "Rival Influence Push".to_owned()
                    } else {
                        "Influence Opportunity".to_owned()
                    },
                    if hostility > 0.45 {
                        "Exploit faction rivalries to undermine dominant influence.".to_owned()
                    } else {
                        "Support local interests to shift faction control.".to_owned()
                    },
                    if hostility > 0.45 { 4 } else { 3 },
                    if hostility > 0.45 { 0.45 } else { 0.35 },
                ),
            }
            };

            self.missions.push(MissionState {
                id: self.next_mission_id,
                issuer_faction: dominant,
                target_system: sim.system,
                kind,
                title,
                description,
                reward_credits: (1_500.0 + sim.econ_pressure.max(0.0) * 4_000.0) as i64,
                reward_tech: 0.04 + sim.econ_pressure.max(0.0) * 0.08,
                reward_reputation: reward_rep,
                risk,
                expires_year: self.current_year + Self::MISSION_REFRESH_INTERVAL_YEARS,
            });
            self.next_mission_id = self.next_mission_id.saturating_add(1);
            if self.missions.len() >= 8 {
                break;
            }
        }
    }

    fn colony_population_carrying_capacity(colony: &ColonyState) -> f64 {
        let site_base_capacity = if colony.earth_like_world {
            12_000_000.0
        } else if colony.habitable_site {
            5_000_000.0
        } else {
            1_600_000.0
        };
        let stage_multiplier = match colony.stage {
            ColonyStage::Outpost => 0.8,
            ColonyStage::Settlement => 1.0,
            ColonyStage::City => 1.6,
            ColonyStage::CoreWorld => 2.4,
        };
        let policy_multiplier = colony.policy.definition().carrying_capacity_multiplier as f64;
        let infrastructure_bonus = colony.stockpile_capacity.max(20.0) as f64 * 12_000.0;
        let building_capacity_bonus = colony
            .buildings
            .iter()
            .map(|building| f64::from(building.level.max(1)))
            .sum::<f64>()
            * 260_000.0;

        ((site_base_capacity + infrastructure_bonus + building_capacity_bonus)
            * stage_multiplier
            * policy_multiplier)
            .max(150_000.0)
    }

    fn taxable_population(population: f64) -> f64 {
        if population <= 0.0 {
            return 0.0;
        }

        let saturation = Self::TAXABLE_POPULATION_SATURATION;
        (population * saturation) / (population + saturation)
    }

    fn colony_tax_revenue_annual(colony: &ColonyState) -> i64 {
        let base_per_person = 0.64_f64;
        let stability_factor = colony.stability.clamp(0.2, 1.0) as f64;
        let policy_factor = colony.taxation_policy.multiplier();
        let taxable_population = Self::taxable_population(colony.population);
        (taxable_population * base_per_person * stability_factor * policy_factor)
            .round()
            .max(0.0) as i64
    }

    fn colony_upkeep_cost_annual(colony: &ColonyState) -> i64 {
        let stage_base = match colony.stage {
            ColonyStage::Outpost => 4_500,
            ColonyStage::Settlement => 18_000,
            ColonyStage::City => 78_000,
            ColonyStage::CoreWorld => 340_000,
        };
        let policy_factor = colony.policy.definition().upkeep_multiplier;
        let defense_factor = 1.0 + colony.defense_balance.max(0.0) as f64 * 1.6;
        let population_component_linear =
            colony.population * Self::POPULATION_UPKEEP_LINEAR_PER_PERSON;
        let population_component_quadratic = colony.population
            * colony.population
            * Self::POPULATION_UPKEEP_QUADRATIC_PER_PERSON_SQUARED;
        let population_component = population_component_linear + population_component_quadratic;

        ((stage_base as f64 + population_component) * policy_factor * defense_factor)
            .round()
            .max(0.0) as i64
    }

    pub fn is_system_explored(&self, system: SystemId) -> bool {
        self.explored_systems.contains(&system)
    }

    pub fn survey_stage(&self, system: SystemId) -> SurveyStage {
        self.survey_record(system)
            .map(|record| record.stage)
            .unwrap_or_else(|| {
                if self.explored_systems.contains(&system) {
                    SurveyStage::Located
                } else {
                    SurveyStage::Unknown
                }
            })
    }

    pub fn survey_record(&self, system: SystemId) -> Option<&SystemSurveyRecord> {
        self.survey_records.get(&system)
    }

    pub fn fully_surveyed_system_count(&self) -> usize {
        self.survey_records
            .values()
            .filter(|record| record.stage >= SurveyStage::ColonyAssessment)
            .count()
    }

    pub fn pending_scan_for(&self, system: SystemId) -> Option<&PendingSurveyScan> {
        self.pending_survey_scans
            .iter()
            .find(|scan| scan.system == system)
    }

    pub fn pending_colony_founding_for_system(
        &self,
        system: SystemId,
    ) -> Option<&PendingColonyFounding> {
        self.pending_colony_foundings
            .iter()
            .find(|founding| founding.system == system)
    }

    pub fn pending_colony_founding_for_target(
        &self,
        system: SystemId,
        body_index: u16,
    ) -> Option<&PendingColonyFounding> {
        self.pending_colony_foundings
            .iter()
            .find(|founding| founding.system == system && founding.body_index == body_index)
    }

    pub fn pending_colony_building_for_colony(
        &self,
        colony_id: u64,
    ) -> Option<&PendingColonyBuilding> {
        self.pending_colony_buildings
            .iter()
            .find(|pending| pending.colony_id == colony_id)
    }

    #[allow(dead_code)]
    pub fn queue_colony_building(
        &mut self,
        current_year: f32,
        colony_id: u64,
        kind: ColonyBuildingKind,
        site: ColonyBuildingSite,
    ) -> Result<(f32, i64, u16), &'static str> {
        self.queue_colony_building_with_profile(
            current_year,
            colony_id,
            kind,
            site,
            ColonyBuildingSiteProfile::default(),
        )
    }

    pub fn queue_colony_building_with_profile(
        &mut self,
        current_year: f32,
        colony_id: u64,
        kind: ColonyBuildingKind,
        site: ColonyBuildingSite,
        site_profile: ColonyBuildingSiteProfile,
    ) -> Result<(f32, i64, u16), &'static str> {
        let Some(colony) = self.colonies.get(&colony_id) else {
            return Err("Selected colony no longer exists.");
        };

        if kind == ColonyBuildingKind::SpaceStation {
            return Err("Space Station is established automatically when a colony is founded.");
        }
        if let Some(error) = Self::building_site_support_error(kind, site, site_profile) {
            return Err(error);
        }

        if self.pending_colony_building_for_colony(colony_id).is_some() {
            return Err("Another building is already under construction for this colony.");
        }

        let current_level = colony.building_level_at_site(kind, site);
        let target_level = current_level.saturating_add(1);
        if target_level > kind.max_level() {
            return Err("This building has reached its maximum level.");
        }

        let stage_rank = match colony.stage {
            ColonyStage::Outpost => 0,
            ColonyStage::Settlement => 1,
            ColonyStage::City => 2,
            ColonyStage::CoreWorld => 3,
        };
        let min_stage_rank = match kind {
            ColonyBuildingKind::HabitatArcology => 1,
            ColonyBuildingKind::SystemsAdministration => 1,
            ColonyBuildingKind::DefenseGrid => 1,
            ColonyBuildingKind::CatalyticRefinery => 2,
            _ => 0,
        };
        if stage_rank < min_stage_rank {
            return Err("Colony stage is too low for this building.");
        }

        let requires_new_slot = current_level == 0 && kind.consumes_site_slot();
        if requires_new_slot {
            if let Some(slot_capacity) = Self::building_site_slot_capacity(site, site_profile) {
                let occupied_slots = colony.occupied_building_slots_at_site(site);
                if occupied_slots >= slot_capacity {
                    return Err("No free building slots remain on this planet.");
                }
            }
        }
        if matches!(site, ColonyBuildingSite::Orbital) {
            let orbital_count = colony
                .buildings
                .iter()
                .filter(|b| matches!(b.site, ColonyBuildingSite::Orbital))
                .count();
            if current_level == 0 && orbital_count >= 2 {
                return Err("Orbital capacity is fully allocated for this colony.");
            }
        }

        let owner_faction = colony.owner_faction.clone();
        let resource_cost = Self::colony_building_resource_cost(kind, target_level);
        let element_costs = Self::colony_building_element_costs(kind, target_level);
        let construction_cost = resource_cost.treasury;
        let upfront_treasury_cost =
            ((construction_cost as f32) * Self::CONSTRUCTION_UPFRONT_PAYMENT_RATIO).round() as i64;
        let deferred_treasury_due = construction_cost.saturating_sub(upfront_treasury_cost);
        let annual_construction_upkeep =
            ((construction_cost as f32) * Self::CONSTRUCTION_ANNUAL_UPKEEP_RATIO).round() as i64;
        let construction_duration = Self::colony_building_duration_years(kind, target_level);

        let Some(faction) = self.factions.get_mut(&owner_faction) else {
            return Err("Owning faction could not be found.");
        };
        if faction.treasury < upfront_treasury_cost {
            return Err("Insufficient faction treasury for this construction.");
        }
        if !Self::can_afford_colony_resource_cost(colony, resource_cost, &element_costs) {
            return Err("Insufficient colony stockpiles (elements/food/industry/energy) for this construction.");
        }

        faction.treasury = faction.treasury.saturating_sub(upfront_treasury_cost);
        if let Some(colony) = self.colonies.get_mut(&colony_id) {
            Self::spend_colony_resource_cost(colony, resource_cost, &element_costs);
        }
        self.pending_colony_buildings.push(PendingColonyBuilding {
            colony_id,
            kind,
            site,
            target_level,
            start_year: current_year,
            complete_year: current_year + construction_duration,
            deferred_treasury_due,
            annual_construction_upkeep,
        });

        Ok((construction_duration, construction_cost, target_level))
    }

    pub fn queue_colony_founding(
        &mut self,
        current_year: f32,
        mut pending: PendingColonyFounding,
    ) -> Result<f32, &'static str> {
        if self
            .pending_colony_founding_for_target(pending.system, pending.body_index)
            .is_some()
        {
            return Err("A colony expedition is already en route to this colony site.");
        }
        if self.has_colony_at(pending.system, pending.body_index) {
            return Err("A colony already exists at this location.");
        }

        if let Some(source_colony_id) = pending.source_colony_id {
            if pending.colonists_sent < 100 {
                return Err("At least 100 colonists are required to establish a new colony.");
            }

            let Some(source_colony) = self.colonies.get_mut(&source_colony_id) else {
                return Err("Selected source colony no longer exists.");
            };
            if source_colony.owner_faction != pending.founder_faction {
                return Err("Selected source colony is not owned by the founding faction.");
            }

            let source_population = source_colony.population.max(0.0) as u64;
            let min_remaining = 100_u64;
            let colonists_sent = pending.colonists_sent as u64;
            if source_population <= min_remaining
                || source_population < colonists_sent + min_remaining
            {
                return Err("Source colony does not have enough population to send this expedition.");
            }

            source_colony.population = (source_population - colonists_sent) as f64;
        } else {
            pending.colonists_sent = pending.colonists_sent.max(100);
        }

        pending.start_year = current_year;
        let duration = (pending.complete_year - pending.start_year).max(0.05);
        pending.complete_year = pending.start_year + duration;
        pending.element_resource_profile =
            Self::normalized_element_resource_profile(&pending.element_resource_profile);
        pending.atmosphere_resource_profile =
            Self::normalized_atmosphere_resource_profile(&pending.atmosphere_resource_profile);
        pending.atmosphere_pressure_atm = pending.atmosphere_pressure_atm.max(0.0);
        self.pending_colony_foundings.push(pending);
        Ok(duration)
    }

    /// Minimum colonists that must remain in a colony after a transfer.
    #[allow(dead_code)]
    const TRANSFER_MIN_REMAINING_POP: u64 = 500;
    /// Stability cost applied to the source colony when a transfer departs.
    #[allow(dead_code)]
    const TRANSFER_SOURCE_STABILITY_COST: f32 = 0.12;
    /// Stability cost applied to the destination colony when a transfer arrives.
    #[allow(dead_code)]
    const TRANSFER_DEST_STABILITY_COST: f32 = 0.15;
    /// Base transit duration in years (scaled by distance).
    #[allow(dead_code)]
    const TRANSFER_BASE_DURATION_YEARS: f32 = 0.4;
    /// Treasury cost per colonist transferred.
    #[allow(dead_code)]
    const TRANSFER_COST_PER_COLONIST: f64 = 1.8;

    #[allow(dead_code)]
    pub fn queue_population_transfer(
        &mut self,
        source_colony_id: u64,
        target_colony_id: u64,
        colonists: u32,
        faction_id: &str,
    ) -> Result<f32, &'static str> {
        if source_colony_id == target_colony_id {
            return Err("Source and destination must be different colonies.");
        }
        if colonists < 100 {
            return Err("At least 100 colonists are required for a transfer.");
        }

        // Validate source colony.
        let source = self.colonies.get(&source_colony_id)
            .ok_or("Source colony no longer exists.")?;
        if source.owner_faction != faction_id {
            return Err("Source colony is not owned by your faction.");
        }
        let source_pop = source.population.max(0.0) as u64;
        if source_pop < colonists as u64 + Self::TRANSFER_MIN_REMAINING_POP {
            return Err("Source colony does not have enough population for this transfer.");
        }
        let source_pos = source.system_pos;

        // Validate destination colony.
        let dest = self.colonies.get(&target_colony_id)
            .ok_or("Destination colony no longer exists.")?;
        if dest.owner_faction != faction_id {
            return Err("Destination colony is not owned by your faction.");
        }
        let dest_pos = dest.system_pos;

        // Compute distance-scaled duration.
        let dx = source_pos[0] - dest_pos[0];
        let dy = source_pos[1] - dest_pos[1];
        let dz = source_pos[2] - dest_pos[2];
        let dist = (dx * dx + dy * dy + dz * dz).sqrt();
        let dist_norm = (dist / 50_000.0).clamp(0.0, 1.0);
        let duration = Self::TRANSFER_BASE_DURATION_YEARS * (1.0 + dist_norm * 3.0);

        // Check treasury.
        let cost = (colonists as f64 * Self::TRANSFER_COST_PER_COLONIST).round() as i64;
        let faction = self.factions.get(faction_id)
            .ok_or("Faction not found.")?;
        if faction.treasury < cost {
            return Err("Insufficient treasury to fund the transfer.");
        }

        // --- Commit: deduct population, stability, and treasury ---
        let source = self.colonies.get_mut(&source_colony_id).unwrap();
        source.population -= colonists as f64;
        source.stability = (source.stability - Self::TRANSFER_SOURCE_STABILITY_COST).clamp(0.1, 1.0);

        let faction = self.factions.get_mut(faction_id).unwrap();
        faction.treasury = faction.treasury.saturating_sub(cost);

        self.pending_population_transfers.push(PendingPopulationTransfer {
            source_colony_id,
            target_colony_id,
            colonists,
            start_year: self.current_year,
            complete_year: self.current_year + duration,
        });

        Ok(duration)
    }

    pub fn queue_survey_scan(
        &mut self,
        system: SystemId,
        by_faction: String,
        current_year: f32,
        target_stage: SurveyStage,
        surveyed_body_count: u16,
        habitable_body_count: u16,
        viable_body_index: Option<u16>,
        duration_scale: f32,
    ) -> Result<f32, &'static str> {
        if self.pending_scan_for(system).is_some() {
            return Err("A survey operation is already in progress for this system.");
        }

        let current_stage = self.survey_stage(system);
        if current_stage.next() != Some(target_stage) {
            return Err("Survey transition is invalid for this system.");
        }

        let duration = (Self::survey_duration_years(target_stage) * duration_scale.clamp(0.5, 12.0))
            .max(0.02);
        self.pending_survey_scans.push(PendingSurveyScan {
            system,
            by_faction,
            start_year: current_year,
            complete_year: current_year + duration,
            target_stage,
            surveyed_body_count,
            habitable_body_count,
            viable_body_index,
        });
        Ok(duration)
    }

    pub fn survey_duration_years(target_stage: SurveyStage) -> f32 {
        match target_stage {
            SurveyStage::Unknown => 0.0,
            SurveyStage::Located => 0.60,
            SurveyStage::StellarSurvey => 1.00,
            SurveyStage::PlanetarySurvey => 1.60,
            SurveyStage::ColonyAssessment => 2.20,
        }
    }

    pub fn pending_scan_progress(&self, system: SystemId) -> Option<f32> {
        let scan = self.pending_scan_for(system)?;
        let total = (scan.complete_year - scan.start_year).max(0.0001);
        let elapsed = (self.current_year - scan.start_year).clamp(0.0, total);
        Some((elapsed / total).clamp(0.0, 1.0))
    }

    #[allow(dead_code)]
    pub fn colony_candidate_body(&self, system: SystemId) -> Option<u16> {
        let record = self.survey_record(system)?;
        if record.stage >= SurveyStage::ColonyAssessment {
            record.viable_body_index
        } else {
            None
        }
    }

    fn upsert_survey_record(
        &mut self,
        system: SystemId,
        stage: SurveyStage,
        surveyed_body_count: u16,
        habitable_body_count: u16,
        viable_body_index: Option<u16>,
        at_year: f32,
    ) {
        if let Some(record) = self.survey_records.get_mut(&system) {
            record.stage = record.stage.max(stage);
            if surveyed_body_count > 0 || record.surveyed_body_count == 0 {
                record.surveyed_body_count = record.surveyed_body_count.max(surveyed_body_count);
            }
            if habitable_body_count > 0 || record.habitable_body_count == 0 {
                record.habitable_body_count = record.habitable_body_count.max(habitable_body_count);
            }
            if viable_body_index.is_some() {
                record.viable_body_index = viable_body_index;
            }
            record.last_updated_year = record.last_updated_year.max(at_year);
            return;
        }

        self.survey_records.insert(system, SystemSurveyRecord {
            system,
            stage,
            surveyed_body_count,
            habitable_body_count,
            viable_body_index,
            last_updated_year: at_year,
        });
    }

    pub fn has_colony_at(&self, system: SystemId, body_index: u16) -> bool {
        self.colonies
            .values()
            .any(|c| c.system == system && c.body_index == body_index)
    }

    pub fn reserve_colony_id(&mut self) -> u64 {
        let id = self.next_colony_id;
        self.next_colony_id = self.next_colony_id.saturating_add(1);
        id
    }

    #[allow(dead_code)]
    pub fn nearest_colony_for_faction(
        &self,
        faction_id: &str,
        target_pos: [f32; 3],
    ) -> Option<NearestColonyInfo> {
        self.colonies
            .values()
            .filter(|colony| {
                colony.owner_faction == faction_id
                    && colony.system_pos[0].is_finite()
                    && colony.system_pos[1].is_finite()
                    && colony.system_pos[2].is_finite()
            })
            .map(|colony| {
                let dx = colony.system_pos[0] - target_pos[0];
                let dy = colony.system_pos[1] - target_pos[1];
                let dz = colony.system_pos[2] - target_pos[2];
                let distance = (dx * dx + dy * dy + dz * dz).sqrt();
                NearestColonyInfo {
                    system_pos: colony.system_pos,
                    distance,
                }
            })
            .min_by(|a, b| a.distance.total_cmp(&b.distance))
    }

    pub fn faction_colonization_range_world(&self, faction_id: &str) -> f32 {
        let tech_level = self
            .factions
            .get(faction_id)
            .map(|faction| faction.colonization_tech_level)
            .unwrap_or(0);
        self.base_colonization_range_world
            + tech_level as f32 * Self::COLONIZATION_RANGE_PER_TECH_LEVEL_WORLD
    }

    pub fn set_player_starting_colony(&mut self, colony_id: u64) -> bool {
        let Some(colony) = self.colonies.get(&colony_id) else {
            return false;
        };
        if colony.owner_faction != self.player.faction_id {
            return false;
        }

        self.player.starting_colony_id = Some(colony_id);
        self.player.location = Some(colony.system);
        true
    }

    pub fn set_player_home_system(&mut self, system: SystemId) {
        self.player.home_system = Some(system);
        if self.player.location.is_none() {
            self.player.location = Some(system);
        }
    }

    pub fn apply_event(&mut self, event: &GameEvent) {
        match event {
            GameEvent::DiscoveredSystem {
                at_year,
                system,
                by_faction,
            } => {
                let previous_stage = self.survey_stage(*system);
                self.current_year = self.current_year.max(*at_year);
                self.explored_systems.insert(*system);
                let sim_state = self.ensure_system_sim_state(*system);
                *sim_state
                    .influence_by_faction
                    .entry(by_faction.clone())
                    .or_insert(0.0) += 0.08;
                self.upsert_survey_record(*system, SurveyStage::Located, 0, 0, None, *at_year);
                let updated_stage = self.survey_stage(*system);
                if updated_stage > previous_stage {
                    self.award_exploration_rewards(by_faction, updated_stage);
                }
            }
            GameEvent::HomeSystemSelected { at_year, system } => {
                self.current_year = self.current_year.max(*at_year);
                self.explored_systems.insert(*system);
                self.ensure_system_sim_state(*system);
                self.set_player_home_system(*system);
                self.upsert_survey_record(*system, SurveyStage::Located, 0, 0, None, *at_year);
            }
            GameEvent::SurveyedSystem {
                at_year,
                system,
                stage,
                surveyed_body_count,
                habitable_body_count,
                viable_body_index,
                by_faction,
            } => {
                let previous_stage = self.survey_stage(*system);
                self.current_year = self.current_year.max(*at_year);
                self.explored_systems.insert(*system);
                let sim_state = self.ensure_system_sim_state(*system);
                *sim_state
                    .influence_by_faction
                    .entry(by_faction.clone())
                    .or_insert(0.0) += 0.06;
                self.upsert_survey_record(
                    *system,
                    *stage,
                    *surveyed_body_count,
                    *habitable_body_count,
                    *viable_body_index,
                    *at_year,
                );
                let updated_stage = self.survey_stage(*system);
                if updated_stage > previous_stage {
                    self.award_exploration_rewards(by_faction, updated_stage);
                }
            }
            GameEvent::FoundedColony {
                at_year,
                colony_id,
                colony_name,
                founder_faction,
                system,
                body_index,
                habitable_site,
                earth_like_world,
                system_pos,
                element_resource_profile,
                atmosphere_resource_profile,
                atmosphere_pressure_atm,
                colonists_sent,
                ..
            } => {
                self.current_year = self.current_year.max(*at_year);
                self.explored_systems.insert(*system);
                let sim_state = self.ensure_system_sim_state(*system);
                *sim_state
                    .influence_by_faction
                    .entry(founder_faction.clone())
                    .or_insert(0.0) += 0.35;
                let is_player_starting_colony = self.player.starting_colony_id.is_none()
                    && founder_faction == &self.player.faction_id;
                let minimum_start_population = if is_player_starting_colony {
                    Self::STARTING_COLONY_MIN_POPULATION
                } else {
                    100
                };
                self.upsert_survey_record(
                    *system,
                    SurveyStage::ColonyAssessment,
                    0,
                    0,
                    Some(*body_index),
                    *at_year,
                );
                self.colonies.insert(
                    *colony_id,
                    ColonyState {
                        id: *colony_id,
                        name: colony_name.clone(),
                        owner_faction: founder_faction.clone(),
                        system: *system,
                        body_index: *body_index,
                        habitable_site: *habitable_site,
                        earth_like_world: *earth_like_world,
                        system_pos: *system_pos,
                        policy: ColonyPolicy::Balanced,
                        taxation_policy: TaxationPolicy::Standard,
                        stage: ColonyStage::Outpost,
                        population: (u32::max(*colonists_sent, minimum_start_population)) as f64,
                        stability: if *earth_like_world {
                            0.92
                        } else if *habitable_site {
                            0.82
                        } else {
                            0.68
                        },
                        food_balance: if *earth_like_world {
                            0.20
                        } else if *habitable_site {
                            0.12
                        } else {
                            0.05
                        },
                        industry_balance: if *earth_like_world {
                            0.14
                        } else if *habitable_site {
                            0.08
                        } else {
                            0.03
                        },
                        energy_balance: if *earth_like_world {
                            0.14
                        } else if *habitable_site {
                            0.08
                        } else {
                            0.04
                        },
                        defense_balance: if *earth_like_world { 0.05 } else { 0.02 },
                        stockpile_capacity: if *earth_like_world {
                            160.0
                        } else if *habitable_site {
                            120.0
                        } else {
                            80.0
                        },
                        food_stockpile: if *earth_like_world {
                            96.0
                        } else if *habitable_site {
                            64.0
                        } else {
                            38.0
                        },
                        industry_stockpile: if *earth_like_world {
                            72.0
                        } else if *habitable_site {
                            46.0
                        } else {
                            28.0
                        },
                        energy_stockpile: if *earth_like_world {
                            74.0
                        } else if *habitable_site {
                            46.0
                        } else {
                            30.0
                        },
                        element_stockpiles: Self::default_element_stockpiles_for_site(
                            *earth_like_world,
                            *habitable_site,
                        ),
                        atmosphere_stockpiles: HashMap::new(),
                        element_resource_profile:
                            Self::normalized_element_resource_profile(element_resource_profile),
                        atmosphere_resource_profile:
                            Self::normalized_atmosphere_resource_profile(
                                atmosphere_resource_profile,
                            ),
                        atmosphere_pressure_atm: atmosphere_pressure_atm.max(0.0),
                        buildings: vec![ColonyBuildingState {
                            kind: ColonyBuildingKind::SpaceStation,
                            site: ColonyBuildingSite::Orbital,
                            level: 1,
                        }],
                        last_tax_revenue_annual: 0,
                        last_upkeep_cost_annual: 0,
                        last_net_revenue_annual: 0,
                    },
                );

                // Mark this as the faction's starting colony if it doesn't have one yet.
                let is_faction_starting_colony = self
                    .factions
                    .get(founder_faction)
                    .is_some_and(|f| f.starting_colony_id.is_none());
                if let Some(faction) = self.factions.get_mut(founder_faction) {
                    if faction.starting_colony_id.is_none() {
                        faction.starting_colony_id = Some(*colony_id);
                    }
                }
                // Seed element stockpiles for any faction's starting colony.
                if is_faction_starting_colony {
                    if let Some(colony) = self.colonies.get_mut(colony_id) {
                        Self::seed_starting_colony_element_stockpiles(colony);
                    }
                }
                if is_player_starting_colony {
                    let _ = self.set_player_starting_colony(*colony_id);
                }
            }
            GameEvent::StartingColonySelected { at_year, colony_id } => {
                self.current_year = self.current_year.max(*at_year);
                let _ = self.set_player_starting_colony(*colony_id);
            }
            GameEvent::FactionRelationChanged {
                at_year,
                from_faction,
                to_faction,
                delta,
                ..
            } => {
                self.current_year = self.current_year.max(*at_year);
                let current = self.get_relation(from_faction, to_faction);
                self.set_relation(from_faction, to_faction, current.saturating_add(*delta));
            }
            GameEvent::TreatyEstablished {
                at_year,
                faction_a,
                faction_b,
                treaty,
                expires_year,
                ..
            } => {
                self.current_year = self.current_year.max(*at_year);
                self.diplomacy_treaties.insert(
                    Self::relation_key(faction_a, faction_b),
                    DiplomacyTreatyState {
                        kind: *treaty,
                        started_year: *at_year,
                        expires_year: *expires_year,
                        cohesion: 0.45,
                        strain: 0.0,
                    },
                );
                let relation_boost = match treaty {
                    DiplomaticTreatyKind::Alliance => 10,
                    DiplomaticTreatyKind::NonAggressionPact => 6,
                };
                let current = self.get_relation(faction_a, faction_b);
                self.set_relation(faction_a, faction_b, current.saturating_add(relation_boost));
            }
            GameEvent::TreatyDissolved {
                at_year,
                faction_a,
                faction_b,
                ..
            } => {
                self.current_year = self.current_year.max(*at_year);
                self.diplomacy_treaties.remove(&Self::relation_key(faction_a, faction_b));
                let current = self.get_relation(faction_a, faction_b);
                self.set_relation(faction_a, faction_b, current.saturating_sub(12));
            }
            GameEvent::SanctionImposed {
                at_year,
                by_faction,
                target_faction,
                expires_year,
                ..
            } => {
                self.current_year = self.current_year.max(*at_year);
                self.active_sanctions
                    .insert((by_faction.clone(), target_faction.clone()), *expires_year);
                let current = self.get_relation(by_faction, target_faction);
                self.set_relation(by_faction, target_faction, current.saturating_sub(8));
            }
            GameEvent::SanctionLifted {
                at_year,
                by_faction,
                target_faction,
                ..
            } => {
                self.current_year = self.current_year.max(*at_year);
                self.active_sanctions
                    .remove(&(by_faction.clone(), target_faction.clone()));
                let current = self.get_relation(by_faction, target_faction);
                self.set_relation(by_faction, target_faction, current.saturating_add(4));
            }
            GameEvent::PowerplayOperationResolved {
                at_year,
                actor_faction,
                target_faction,
                system,
                operation,
                success,
                strength,
                ..
            } => {
                self.current_year = self.current_year.max(*at_year);
                let sim = self.ensure_system_sim_state(*system);
                if *success {
                    match operation {
                        PowerplayOperationKind::UndermineInfluence => {
                            if let Some(target) = sim.influence_by_faction.get_mut(target_faction) {
                                *target = (*target - *strength).max(0.0);
                            }
                            *sim
                                .influence_by_faction
                                .entry(actor_faction.clone())
                                .or_insert(0.0) += *strength * 0.55;
                            sim.econ_pressure = (sim.econ_pressure + strength * 0.45).clamp(0.0, 1.2);
                            sim.security = (sim.security - strength * 0.25).clamp(0.05, 1.0);
                        }
                        PowerplayOperationKind::SupportAlly => {
                            *sim
                                .influence_by_faction
                                .entry(target_faction.clone())
                                .or_insert(0.0) += *strength * 0.65;
                            sim.trade_flow = (sim.trade_flow + strength * 0.40).clamp(0.0, 2.0);
                            sim.stability = (sim.stability + strength * 0.20).clamp(0.05, 1.0);
                        }
                        PowerplayOperationKind::EconomicPressure => {
                            sim.scarcity = (sim.scarcity + strength * 0.30).clamp(0.0, 1.0);
                            sim.econ_pressure = (sim.econ_pressure + strength * 0.35).clamp(0.0, 1.2);
                            sim.trade_flow = (sim.trade_flow - strength * 0.28).clamp(0.0, 2.0);
                        }
                    }
                }
                self.recent_powerplay_ops.push(PowerplayOperationRecord {
                    at_year: *at_year,
                    actor_faction: actor_faction.clone(),
                    target_faction: target_faction.clone(),
                    system: *system,
                    operation: *operation,
                    success: *success,
                });
                if self.recent_powerplay_ops.len() > 128 {
                    let drop_n = self.recent_powerplay_ops.len() - 128;
                    self.recent_powerplay_ops.drain(0..drop_n);
                }
            }
            GameEvent::CompletedColonyBuilding {
                at_year,
                colony_id,
                kind,
                site,
                target_level,
            } => {
                self.current_year = self.current_year.max(*at_year);
                if let Some(colony) = self.colonies.get_mut(colony_id) {
                    colony.set_building_level(*kind, *site, *target_level);
                }
            }
        }
    }

    pub fn advance_strategic_tick(&mut self, delta_years: f32) -> Vec<GameEvent> {
        if !delta_years.is_finite() || delta_years <= 0.0 {
            return Vec::new();
        }

        self.current_year += delta_years;
        let mut generated_events = Vec::new();

        let mut colony_counts_by_faction = HashMap::<String, usize>::new();
        for colony in self.colonies.values() {
            *colony_counts_by_faction
                .entry(colony.owner_faction.clone())
                .or_insert(0) += 1;
        }

        for faction in self.factions.values_mut() {
            let colony_count = colony_counts_by_faction
                .get(&faction.id)
                .copied()
                .unwrap_or(0) as f32;
            let research_rate = 0.002 + colony_count * 0.003;
            faction.colonization_tech_progress += research_rate * delta_years;

            while faction.colonization_tech_progress >= 1.0 {
                faction.colonization_tech_progress -= 1.0;
                faction.colonization_tech_level = faction.colonization_tech_level.saturating_add(1);
            }
        }

        let mut treasury_delta_by_faction = HashMap::<String, i64>::new();
        for colony in self.colonies.values_mut() {
            let habitability_bonus = if colony.earth_like_world {
                2.35
            } else if colony.habitable_site {
                1.75
            } else {
                1.0
            };
            let elw_stability_bonus = if colony.earth_like_world { 0.008 } else { 0.0 };
            let elw_population_bonus_factor = if colony.earth_like_world { 1.25 } else { 1.0 };
            let elw_output_multiplier = if colony.earth_like_world { 1.12 } else { 1.0 };
            let elw_base_resource_bonus = if colony.earth_like_world { 0.00075 } else { 0.0 };
            let elw_defense_bonus = if colony.earth_like_world { 0.00030 } else { 0.0 };
            let supply = colony.food_balance + colony.energy_balance + colony.industry_balance;
            let stress = (-supply).max(0.0);

            let policy_def = colony.policy.definition();
            let (mut food_prod, mut industry_prod, mut energy_prod, mut defense_prod) =
                policy_def.production_rates;
            let policy_stability_bonus = policy_def.stability_bonus;
            let policy_migration_bonus = policy_def.migration_bonus;

            let tax_def = colony.taxation_policy.definition();
            let tax_stability_effect = tax_def.stability_effect;
            let tax_growth_effect = tax_def.growth_effect;

            let (
                building_food_prod_bonus,
                building_industry_prod_bonus,
                building_energy_prod_bonus,
                building_food_demand_bonus,
                building_industry_demand_bonus,
                building_energy_demand_bonus,
                building_element_extraction_bonus,
                building_atmosphere_harvest_bonus,
                building_treasury_prod_bonus,
                building_stability_bonus,
                building_growth_bonus,
                building_upkeep_bonus_annual,
            ) =
                Self::colony_building_modifiers(colony);
            food_prod += building_food_prod_bonus + elw_base_resource_bonus;
            industry_prod += building_industry_prod_bonus + elw_base_resource_bonus * 0.9;
            energy_prod += building_energy_prod_bonus + elw_base_resource_bonus * 0.85;
            defense_prod += elw_defense_bonus;

            let stability_gain = (0.006 * habitability_bonus)
                - stress * (0.05 / habitability_bonus)
                + policy_stability_bonus
                + tax_stability_effect
                + elw_stability_bonus
                + building_stability_bonus;
            colony.stability = (colony.stability + stability_gain * delta_years).clamp(0.1, 1.0);

            // Stability-based production efficiency: high stability boosts output,
            // low stability reduces it.  Ranges from 0.70 (at stability 0.1) to
            // 1.10 (at stability 1.0).
            let stability_efficiency = 0.60 + colony.stability * 0.50;

            let startup_population_target = if colony.earth_like_world {
                420_000.0
            } else if colony.habitable_site {
                300_000.0
            } else {
                180_000.0
            };
            let startup_growth_factor = (1.0 - (colony.population / startup_population_target))
                .clamp(0.0, 1.0)
                .powf(Self::STARTUP_GROWTH_CURVE);

            let birth_rate =
                (Self::BASE_BIRTH_RATE_ANNUAL
                    + Self::STARTUP_BIRTH_BOOST_ANNUAL * startup_growth_factor as f64
                    + building_growth_bonus as f64
                    + tax_growth_effect as f64)
                * habitability_bonus as f64
                * elw_population_bonus_factor;
            let death_rate = Self::BASE_DEATH_RATE_ANNUAL
                + (1.0 - colony.stability) as f64 * Self::STABILITY_DEATH_PENALTY_ANNUAL;
            let migration = (supply as f64 * 0.0015 * habitability_bonus as f64)
                + policy_migration_bonus as f64 * 0.35
                - (stress as f64 * 0.0012 / habitability_bonus as f64);
            let carrying_capacity = Self::colony_population_carrying_capacity(colony).max(1.0);
            let capacity_pressure =
                (1.0 - colony.population / carrying_capacity).clamp(-1.5, 1.0);

            // Apply growth as an annualized rate so behavior remains stable across tick sizes.
            let annual_growth_rate_raw = (birth_rate - death_rate + migration) * capacity_pressure;
            let min_annual_growth = if colony.habitable_site {
                Self::MIN_ANNUAL_GROWTH_HABITABLE
            } else {
                Self::MIN_ANNUAL_GROWTH_HOSTILE
            };
            let max_annual_growth = if colony.earth_like_world {
                0.0034
            } else if colony.habitable_site {
                0.0026
            } else {
                0.0018
            };
            let annual_growth_rate = annual_growth_rate_raw.clamp(min_annual_growth, max_annual_growth);
            let growth_factor = (1.0 + annual_growth_rate).max(0.05).powf(delta_years as f64);
            colony.population = (colony.population * growth_factor).max(25.0);

            colony.stage = Self::colony_stage_for_population(colony.population);
            let stage_output_multiplier = Self::stage_output_multiplier(
                colony.population,
                colony.earth_like_world,
            ) * elw_output_multiplier
                * stability_efficiency;
            let population_millions = (colony.population / 1_000_000.0) as f32;
            let food_demand =
                0.0009 + population_millions * 0.0011 + building_food_demand_bonus;
            let industry_demand =
                0.0006 + population_millions * 0.0007 + building_industry_demand_bonus;
            let energy_demand =
                0.0008 + population_millions * 0.0009 + building_energy_demand_bonus;

            colony.food_balance =
                (colony.food_balance + (food_prod * stage_output_multiplier - food_demand) * delta_years)
                    .clamp(-0.35, 0.35);
            colony.industry_balance = (colony.industry_balance
                + (industry_prod * stage_output_multiplier - industry_demand) * delta_years)
                .clamp(-0.35, 0.35);
            colony.energy_balance =
                (colony.energy_balance + (energy_prod * stage_output_multiplier - energy_demand) * delta_years)
                    .clamp(-0.35, 0.35);
            colony.defense_balance =
                (colony.defense_balance + (defense_prod - 0.0002) * delta_years).clamp(-0.20, 0.50);

            let stockpile_capacity = colony.stockpile_capacity.max(20.0);
            let stockpile_delta_scale = stockpile_capacity * 0.12;
            colony.food_stockpile = (colony.food_stockpile
                + colony.food_balance * delta_years * stockpile_delta_scale)
                .clamp(0.0, stockpile_capacity);
            colony.industry_stockpile = (colony.industry_stockpile
                + colony.industry_balance * delta_years * stockpile_delta_scale)
                .clamp(0.0, stockpile_capacity);
            colony.energy_stockpile = (colony.energy_stockpile
                + colony.energy_balance * delta_years * stockpile_delta_scale)
                .clamp(0.0, stockpile_capacity);
            // Keep element capacity meaningfully above starter stockpile seeds so extraction
            // buildings remain productive instead of being silently hard-capped.
            let element_capacity =
                (stockpile_capacity * Self::ELEMENT_STOCKPILE_CAPACITY_MULTIPLIER).max(120.0);
            let element_extraction_rate = (0.018
                + colony.industry_balance.max(0.0) * 0.40
                + if colony.earth_like_world {
                    0.012
                } else if colony.habitable_site {
                    0.008
                } else {
                    0.004
                })
            .max(0.0);
            let element_extraction_amount =
                (element_extraction_rate * delta_years * (stockpile_capacity * 0.10)).max(0.0);
            let total_before = Self::total_element_stockpile(colony);
            let available_capacity = (element_capacity - total_before).max(0.0);
            if available_capacity > 0.0 {
                let addition = element_extraction_amount.min(available_capacity);
                let profile = if colony.element_resource_profile.is_empty() {
                    Self::default_element_resource_profile()
                } else {
                    colony.element_resource_profile.clone()
                };
                Self::add_profiled_stockpile(&mut colony.element_stockpiles, &profile, addition);
            }

            if building_element_extraction_bonus > 0.0 {
                let profile = if colony.element_resource_profile.is_empty() {
                    Self::default_element_resource_profile()
                } else {
                    colony.element_resource_profile.clone()
                };
                let deep_mining_amount = (building_element_extraction_bonus
                    * delta_years
                    * stockpile_capacity
                    * (1.0 + colony.industry_balance.max(0.0) * 0.9))
                    .max(0.0);
                let total_before = Self::total_resource_stockpile(&colony.element_stockpiles);
                let available_capacity = (element_capacity - total_before).max(0.0);
                if available_capacity > 0.0 {
                    Self::add_profiled_stockpile(
                        &mut colony.element_stockpiles,
                        &profile,
                        deep_mining_amount.min(available_capacity),
                    );
                }
            }

            if building_atmosphere_harvest_bonus > 0.0 && colony.atmosphere_pressure_atm > 0.0 {
                let profile = colony.atmosphere_resource_profile.clone();
                let pressure_factor = colony.atmosphere_pressure_atm.clamp(0.0, 12.0).sqrt();
                let atmosphere_capacity = (stockpile_capacity * (0.9 + pressure_factor * 0.8)).max(20.0);
                let atmosphere_harvest_amount = (building_atmosphere_harvest_bonus
                    * delta_years
                    * stockpile_capacity
                    * (0.55 + pressure_factor)
                    * (1.0 + colony.energy_balance.max(0.0) * 0.8))
                    .max(0.0);
                let total_before = Self::total_resource_stockpile(&colony.atmosphere_stockpiles);
                let available_capacity = (atmosphere_capacity - total_before).max(0.0);
                if available_capacity > 0.0 {
                    Self::add_profiled_stockpile(
                        &mut colony.atmosphere_stockpiles,
                        &profile,
                        atmosphere_harvest_amount.min(available_capacity),
                    );
                }
            }

            let tax_revenue_annual = Self::colony_tax_revenue_annual(colony);
            let trading_hub_revenue_annual = if building_treasury_prod_bonus > 0.0 {
                let population_millions = (colony.population / 1_000_000.0) as f32;
                let pop_factor = 1.0 + (population_millions * 0.12).min(1.8);
                (building_treasury_prod_bonus * pop_factor) as i64
            } else {
                0
            };
            let upkeep_cost_annual =
                Self::colony_upkeep_cost_annual(colony).saturating_add(building_upkeep_bonus_annual);
            let net_annual = tax_revenue_annual + trading_hub_revenue_annual - upkeep_cost_annual;

            colony.last_tax_revenue_annual = tax_revenue_annual;
            colony.last_upkeep_cost_annual = upkeep_cost_annual;
            colony.last_net_revenue_annual = net_annual;

            let delta = ((net_annual as f64) * delta_years as f64).round() as i64;
            *treasury_delta_by_faction
                .entry(colony.owner_faction.clone())
                .or_insert(0) += delta;
        }

        for (faction_id, delta) in treasury_delta_by_faction {
            if let Some(faction) = self.factions.get_mut(&faction_id) {
                faction.treasury = faction.treasury.saturating_add(delta);
            }
        }

        let mut colony_metric_input = self.colonies.values().collect::<Vec<_>>();
        colony_metric_input.sort_by_key(|colony| colony.id);
        let colony_metric_rows: Vec<(SystemId, String, f32, f32, f32, f32)> = colony_metric_input
            .par_iter()
            .map(|colony| {
                let supply = colony.food_balance + colony.industry_balance + colony.energy_balance;
                let stress = (-supply).max(0.0);
                let trade_potential =
                    (supply.max(0.0) + colony.last_tax_revenue_annual as f32 / 35_000.0).max(0.0);
                (
                    colony.system,
                    colony.owner_faction.clone(),
                    colony.population as f32 / 1_000_000.0 + colony.stability * 2.2,
                    stress,
                    trade_potential,
                    (1.0 - colony.stability).max(0.0),
                )
            })
            .collect();
        let mut metrics_by_system: HashMap<SystemId, (HashMap<String, f32>, f32, f32, f32)> =
            HashMap::new();
        for (system_id, owner_faction, influence, stress, trade_potential, unrest) in colony_metric_rows {
            let entry = metrics_by_system
                .entry(system_id)
                .or_insert_with(|| (HashMap::new(), 0.0, 0.0, 0.0));
            *entry.0.entry(owner_faction).or_insert(0.0) += influence;
            entry.1 += stress;
            entry.2 += trade_potential;
            entry.3 += unrest;
        }
        let current_year = self.current_year;
        const MAX_RELATION_EVENTS_PER_TICK: usize = 8;
        const MAX_DIPLOMACY_EVENTS_PER_TICK: usize = 5;
        let mut emitted_relation_pairs: HashSet<(String, String)> = HashSet::new();
        let mut pending_diplomacy_events: Vec<GameEvent> = Vec::new();
        for (system_id, (influence_raw, stress, trade, unrest)) in metrics_by_system {
            let sim = self.ensure_system_sim_state(system_id);
            let sum = influence_raw.values().copied().sum::<f32>().max(0.001);
            for (faction_id, value) in influence_raw {
                let target = (value / sum).clamp(0.0, 1.0);
                let current = sim.influence_by_faction.get(&faction_id).copied().unwrap_or(0.0);
                let blended = current * 0.75 + target * 0.25;
                sim.influence_by_faction.insert(faction_id, blended);
            }
            sim.econ_pressure = (sim.econ_pressure * 0.78 + stress * 0.22).clamp(0.0, 1.2);
            sim.trade_flow = (sim.trade_flow * 0.80 + trade * 0.20).clamp(0.0, 2.0);
            sim.scarcity =
                (sim.scarcity * 0.75 + (stress - trade * 0.08).max(0.0) * 0.25).clamp(0.0, 1.0);
            sim.stability =
                (sim.stability + (0.05 - unrest * 0.06 + trade * 0.01) * delta_years).clamp(0.05, 1.0);
            sim.security = (sim.security + (sim.stability - sim.scarcity - 0.4) * 0.16 * delta_years)
                .clamp(0.05, 1.0);

            sim.conflict = if sim.scarcity > 0.75 {
                ConflictState::Embargo
            } else if sim.security < 0.25 {
                ConflictState::ProxyWar
            } else if sim.security < 0.40 {
                ConflictState::PatrolSurge
            } else if sim.econ_pressure > 0.50 {
                ConflictState::Tense
            } else {
                ConflictState::Calm
            };

            let mut top_a: Option<(&String, f32)> = None;
            let mut top_b: Option<(&String, f32)> = None;
            for (faction_id, influence) in &sim.influence_by_faction {
                let value = *influence;
                if top_a.map(|(_, a)| value > a).unwrap_or(true) {
                    top_b = top_a;
                    top_a = Some((faction_id, value));
                } else if top_b.map(|(_, b)| value > b).unwrap_or(true) {
                    top_b = Some((faction_id, value));
                }
            }
            if let (Some(top_a), Some(top_b)) = (top_a, top_b) {
                let conflict_pressure = sim.econ_pressure + sim.scarcity + (1.0 - sim.security);
                let top_a_id = top_a.0.clone();
                let top_b_id = top_b.0.clone();
                let top_a_influence = top_a.1;
                let top_b_influence = top_b.1;
                if top_a.1 > 0.26
                    && top_b.1 > 0.26
                    && conflict_pressure > 1.25
                    && emitted_relation_pairs.len() < MAX_RELATION_EVENTS_PER_TICK
                {
                    let pair = Self::relation_key(top_a.0, top_b.0);
                    if emitted_relation_pairs.insert(pair) {
                        generated_events.push(GameEvent::FactionRelationChanged {
                            at_year: current_year,
                            from_faction: top_a_id.clone(),
                            to_faction: top_b_id.clone(),
                            delta: -1,
                            reason: "Influence contest".to_owned(),
                        });
                    }
                }
                let _ = sim;

                if pending_diplomacy_events.len() < MAX_DIPLOMACY_EVENTS_PER_TICK {
                    let relation = self.get_relation(&top_a_id, &top_b_id);
                    let treaty = self.treaty_between(&top_a_id, &top_b_id).cloned();
                    let sanction_active = self.has_sanction(&top_a_id, &top_b_id)
                        || self.has_sanction(&top_b_id, &top_a_id);

                    if relation >= 48
                        && conflict_pressure < 1.15
                        && treaty.is_none()
                        && top_a_influence > 0.18
                        && top_b_influence > 0.18
                    {
                        pending_diplomacy_events.push(GameEvent::TreatyEstablished {
                            at_year: current_year,
                            faction_a: top_a_id.clone(),
                            faction_b: top_b_id.clone(),
                            treaty: DiplomaticTreatyKind::Alliance,
                            expires_year: current_year + 2.8,
                            reason: "Shared regional interests".to_owned(),
                        });
                    } else if relation >= 25 && relation < 48 && treaty.is_none() && !sanction_active {
                        pending_diplomacy_events.push(GameEvent::TreatyEstablished {
                            at_year: current_year,
                            faction_a: top_a_id.clone(),
                            faction_b: top_b_id.clone(),
                            treaty: DiplomaticTreatyKind::NonAggressionPact,
                            expires_year: current_year + 1.8,
                            reason: "Temporary frontier detente".to_owned(),
                        });
                    } else if relation <= -45
                        && conflict_pressure > 1.20
                        && !sanction_active
                    {
                        pending_diplomacy_events.push(GameEvent::SanctionImposed {
                            at_year: current_year,
                            by_faction: top_a_id.clone(),
                            target_faction: top_b_id.clone(),
                            expires_year: current_year + 1.2,
                            reason: "Escalating influence dispute".to_owned(),
                        });
                    } else if sanction_active && relation > -10 && conflict_pressure < 1.0 {
                        pending_diplomacy_events.push(GameEvent::SanctionLifted {
                            at_year: current_year,
                            by_faction: top_a_id.clone(),
                            target_faction: top_b_id.clone(),
                            reason: "Conflict cooling".to_owned(),
                        });
                    }
                }
            }
        }
        generated_events.extend(pending_diplomacy_events);

        let expired_treaties: Vec<_> = self
            .diplomacy_treaties
            .iter()
            .filter_map(|(pair, treaty)| {
                if treaty.expires_year <= self.current_year {
                    Some((pair.clone(), treaty.kind))
                } else {
                    None
                }
            })
            .collect();
        for ((a, b), treaty_kind) in expired_treaties {
            generated_events.push(GameEvent::TreatyDissolved {
                at_year: self.current_year,
                faction_a: a,
                faction_b: b,
                treaty: treaty_kind,
                reason: "Treaty expired".to_owned(),
            });
        }
        self.active_sanctions.retain(|_, expires_year| *expires_year > self.current_year);
        self.diplomacy_treaties
            .retain(|_, treaty| treaty.expires_year > self.current_year);

        let mut insolvency_penalty_by_faction = HashMap::<String, f32>::new();
        for faction in self.factions.values() {
            if faction.treasury < 0 {
                let debt_millions = (-faction.treasury) as f32 / 1_000_000.0;
                let stability_penalty = (0.003 + debt_millions.min(20.0) * 0.0007) * delta_years;
                insolvency_penalty_by_faction.insert(faction.id.clone(), stability_penalty);
            }
        }
        if !insolvency_penalty_by_faction.is_empty() {
            for colony in self.colonies.values_mut() {
                if let Some(penalty) = insolvency_penalty_by_faction.get(&colony.owner_faction) {
                    colony.stability = (colony.stability - *penalty).clamp(0.05, 1.0);
                }
            }
        }

        let mut completed_scans = Vec::new();
        let mut pending_scans = std::mem::take(&mut self.pending_survey_scans);
        for scan in pending_scans.drain(..) {
            if scan.complete_year <= self.current_year {
                completed_scans.push(scan);
            } else {
                self.pending_survey_scans.push(scan);
            }
        }
        for scan in completed_scans {
            let at_year = scan.complete_year;
            if scan.target_stage == SurveyStage::Located {
                generated_events.push(GameEvent::DiscoveredSystem {
                    at_year,
                    system: scan.system,
                    by_faction: scan.by_faction,
                });
            } else {
                generated_events.push(GameEvent::SurveyedSystem {
                    at_year,
                    system: scan.system,
                    by_faction: scan.by_faction,
                    stage: scan.target_stage,
                    surveyed_body_count: scan.surveyed_body_count,
                    habitable_body_count: scan.habitable_body_count,
                    viable_body_index: scan.viable_body_index,
                });
            }
        }

        let mut completed_foundings = Vec::new();
        let mut pending_foundings = std::mem::take(&mut self.pending_colony_foundings);
        for founding in pending_foundings.drain(..) {
            if founding.complete_year <= self.current_year {
                completed_foundings.push(founding);
            } else {
                self.pending_colony_foundings.push(founding);
            }
        }
        for founding in completed_foundings {
            generated_events.push(GameEvent::FoundedColony {
                at_year: founding.complete_year,
                colony_id: founding.colony_id,
                colony_name: founding.colony_name,
                founder_faction: founding.founder_faction,
                system: founding.system,
                body_index: founding.body_index,
                habitable_site: founding.habitable_site,
                earth_like_world: founding.earth_like_world,
                system_pos: founding.system_pos,
                element_resource_profile: founding.element_resource_profile,
                atmosphere_resource_profile: founding.atmosphere_resource_profile,
                atmosphere_pressure_atm: founding.atmosphere_pressure_atm,
                colonists_sent: founding.colonists_sent,
                source_colony_id: founding.source_colony_id,
            });
        }

        let mut completed_buildings = Vec::new();
        let mut pending_buildings = std::mem::take(&mut self.pending_colony_buildings);
        for mut pending in pending_buildings.drain(..) {
            if let Some(colony) = self.colonies.get(&pending.colony_id) {
                if let Some(faction) = self.factions.get_mut(&colony.owner_faction) {
                    let upkeep_cost = (pending.annual_construction_upkeep as f32 * delta_years)
                        .round() as i64;
                    if upkeep_cost > 0 {
                        faction.treasury = faction.treasury.saturating_sub(upkeep_cost);
                    }
                }
            }
            if pending.complete_year <= self.current_year {
                let mut can_complete = true;
                if pending.deferred_treasury_due > 0 {
                    if let Some(colony) = self.colonies.get(&pending.colony_id) {
                        if let Some(faction) = self.factions.get_mut(&colony.owner_faction) {
                            if faction.treasury >= pending.deferred_treasury_due {
                                faction.treasury =
                                    faction.treasury.saturating_sub(pending.deferred_treasury_due);
                            } else {
                                can_complete = false;
                            }
                        }
                    }
                }
                if can_complete {
                    completed_buildings.push(pending);
                } else {
                    pending.complete_year = (self.current_year + 0.18).max(pending.complete_year + 0.05);
                    self.pending_colony_buildings.push(pending);
                }
            } else {
                self.pending_colony_buildings.push(pending);
            }
        }
        for pending in completed_buildings {
            generated_events.push(GameEvent::CompletedColonyBuilding {
                at_year: pending.complete_year,
                colony_id: pending.colony_id,
                kind: pending.kind,
                site: pending.site,
                target_level: pending.target_level,
            });
        }

        // Complete pending population transfers.
        let mut completed_transfers = Vec::new();
        let mut pending_transfers = std::mem::take(&mut self.pending_population_transfers);
        for transfer in pending_transfers.drain(..) {
            if transfer.complete_year <= self.current_year {
                completed_transfers.push(transfer);
            } else {
                self.pending_population_transfers.push(transfer);
            }
        }
        for transfer in completed_transfers {
            if let Some(dest) = self.colonies.get_mut(&transfer.target_colony_id) {
                dest.population += transfer.colonists as f64;
                dest.stability =
                    (dest.stability - Self::TRANSFER_DEST_STABILITY_COST).clamp(0.1, 1.0);
            }
        }

        self.regenerate_missions();

        generated_events
    }

    pub fn mission_board(&self) -> &[MissionState] {
        &self.missions
    }

    pub fn player_reputation_with(&self, faction_id: &str) -> i16 {
        self.player_reputation.get(faction_id).copied().unwrap_or(0)
    }

    pub fn complete_mission(&mut self, mission_id: u64) -> Result<(), String> {
        let Some(index) = self.missions.iter().position(|mission| mission.id == mission_id) else {
            return Err("Mission not found.".to_owned());
        };
        let mission = self.missions.remove(index);
        if let Some(faction) = self.factions.get_mut(&mission.issuer_faction) {
            faction.treasury = faction.treasury.saturating_add(mission.reward_credits);
            faction.colonization_tech_progress += mission.reward_tech;
        }
        let rep = self
            .player_reputation
            .entry(mission.issuer_faction.clone())
            .or_insert(0);
        *rep = (*rep + mission.reward_reputation).clamp(-100, 100);
        if let Some(sim) = self.system_sim.get_mut(&mission.target_system) {
            match mission.kind {
                MissionKind::SanctionRunning => {
                    sim.econ_pressure = (sim.econ_pressure - 0.18).max(0.0);
                    sim.scarcity = (sim.scarcity - 0.22).max(0.0);
                    sim.trade_flow = (sim.trade_flow + 0.10).clamp(0.0, 2.0);
                    sim.stability = (sim.stability + 0.04).clamp(0.0, 1.0);
                }
                MissionKind::AllianceSupport => {
                    sim.econ_pressure = (sim.econ_pressure - 0.12).max(0.0);
                    sim.scarcity = (sim.scarcity - 0.12).max(0.0);
                    sim.trade_flow = (sim.trade_flow + 0.14).clamp(0.0, 2.0);
                    sim.stability = (sim.stability + 0.06).clamp(0.0, 1.0);
                }
                _ => {
                    sim.econ_pressure = (sim.econ_pressure - 0.15).max(0.0);
                    sim.scarcity = (sim.scarcity - 0.18).max(0.0);
                    sim.stability = (sim.stability + 0.05).clamp(0.0, 1.0);
                }
            }
        }
        Ok(())
    }

    pub fn galactic_hotspots(&self) -> Vec<&SystemSimState> {
        let mut hotspots: Vec<&SystemSimState> = self.system_sim.values().collect();
        hotspots.sort_by(|a, b| {
            (b.econ_pressure + (1.0 - b.security) + b.scarcity)
                .total_cmp(&(a.econ_pressure + (1.0 - a.security) + a.scarcity))
        });
        hotspots
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::procedural_galaxy::{SectorCoord, SystemId};

    fn test_system(local_index: u32) -> SystemId {
        SystemId {
            sector: SectorCoord { x: 0, y: 0 },
            local_index,
        }
    }

    fn test_colony(id: u64, population: f64) -> ColonyState {
        ColonyState {
            id,
            name: format!("Test-{id}"),
            owner_faction: PLAYER_FACTION_ID.to_owned(),
            system: test_system(id as u32),
            body_index: 0,
            habitable_site: false,
            earth_like_world: false,
            system_pos: [0.0, 0.0, 0.0],
            policy: ColonyPolicy::Balanced,
            taxation_policy: TaxationPolicy::Standard,
            stage: ColonyStage::Settlement,
            population,
            stability: 0.5,
            food_balance: 0.0,
            industry_balance: 0.0,
            energy_balance: 0.0,
            defense_balance: 0.0,
            stockpile_capacity: 100.0,
            food_stockpile: 40.0,
            industry_stockpile: 35.0,
            energy_stockpile: 35.0,
            element_stockpiles: GameState::default_element_stockpiles_for_site(false, false),
            atmosphere_stockpiles: HashMap::new(),
            element_resource_profile: GameState::default_element_resource_profile(),
            atmosphere_resource_profile: HashMap::new(),
            atmosphere_pressure_atm: 0.0,
            buildings: Vec::new(),
            last_tax_revenue_annual: 0,
            last_upkeep_cost_annual: 0,
            last_net_revenue_annual: 0,
        }
    }

    #[test]
    fn strategic_tick_can_reduce_population_under_stress() {
        let mut state = GameState::default();
        let mut colony = test_colony(1, 220_000.0);
        colony.stability = 0.1;
        colony.food_balance = -0.35;
        colony.industry_balance = -0.35;
        colony.energy_balance = -0.35;
        state.colonies.insert(colony.id, colony);

        let before = state.colonies.get(&1).unwrap().population;
        state.advance_strategic_tick(0.5);
        let after = state.colonies.get(&1).unwrap().population;

        assert!(after < before, "expected population decline, before={before}, after={after}");
    }

    #[test]
    fn strategic_tick_applies_non_zero_upkeep() {
        let mut state = GameState::default();
        let mut colony = test_colony(2, 5_000_000.0);
        colony.habitable_site = true;
        colony.policy = ColonyPolicy::Fortress;
        colony.defense_balance = 0.4;
        colony.stability = 0.9;
        state.colonies.insert(colony.id, colony);

        state.advance_strategic_tick(0.1);
        let updated = state.colonies.get(&2).unwrap();

        assert!(updated.last_upkeep_cost_annual > 0);
        assert_eq!(
            updated.last_net_revenue_annual,
            updated.last_tax_revenue_annual - updated.last_upkeep_cost_annual
        );
    }

    #[test]
    fn starting_colony_pays_full_upkeep() {
        let mut state = GameState::default();
        let mut colony = test_colony(3, 2_000_000.0);
        colony.owner_faction = state.player.faction_id.clone();
        colony.stage = ColonyStage::City;
        state.player.starting_colony_id = Some(colony.id);
        if let Some(faction) = state.factions.get_mut(&state.player.faction_id) {
            faction.starting_colony_id = Some(colony.id);
        }
        state.colonies.insert(colony.id, colony);

        state.advance_strategic_tick(0.25);
        let updated = state.colonies.get(&3).unwrap();

        assert!(updated.last_upkeep_cost_annual > 0);
        // No discount: upkeep should match the full calculated upkeep (plus building upkeep).
        assert!(updated.last_upkeep_cost_annual >= GameState::colony_upkeep_cost_annual(updated));
    }

    #[test]
    fn colonies_can_run_negative_net_revenue() {
        let mut state = GameState::default();
        let mut colony = test_colony(4, 100.0);
        colony.owner_faction = state.player.faction_id.clone();
        colony.stability = 0.2;
        colony.stage = ColonyStage::Settlement;
        state.colonies.insert(colony.id, colony);

        let treasury_before = state
            .factions
            .get(&state.player.faction_id)
            .map(|f| f.treasury)
            .unwrap_or(0);

        state.advance_strategic_tick(1.0);

        let updated = state.colonies.get(&4).unwrap();
        let treasury_after = state
            .factions
            .get(&state.player.faction_id)
            .map(|f| f.treasury)
            .unwrap_or(0);

        assert!(updated.last_net_revenue_annual < 0);
        assert!(treasury_after < treasury_before);
    }

    #[test]
    fn completed_scan_events_are_ordered_by_completion_year() {
        let mut state = GameState::default();
        state.pending_survey_scans.push(PendingSurveyScan {
            system: test_system(2),
            by_faction: PLAYER_FACTION_ID.to_owned(),
            start_year: 3300.0,
            complete_year: 3301.4,
            target_stage: SurveyStage::StellarSurvey,
            surveyed_body_count: 1,
            habitable_body_count: 0,
            viable_body_index: None,
        });
        state.pending_survey_scans.push(PendingSurveyScan {
            system: test_system(1),
            by_faction: PLAYER_FACTION_ID.to_owned(),
            start_year: 3300.0,
            complete_year: 3300.8,
            target_stage: SurveyStage::Located,
            surveyed_body_count: 0,
            habitable_body_count: 0,
            viable_body_index: None,
        });

        let events = state.advance_strategic_tick(2.0);
        assert_eq!(events.len(), 2);

        let year0 = match &events[0] {
            GameEvent::DiscoveredSystem { at_year, .. }
            | GameEvent::SurveyedSystem { at_year, .. }
            | GameEvent::FoundedColony { at_year, .. }
            | GameEvent::HomeSystemSelected { at_year, .. }
            | GameEvent::StartingColonySelected { at_year, .. }
            | GameEvent::FactionRelationChanged { at_year, .. }
            | GameEvent::CompletedColonyBuilding { at_year, .. }
            | GameEvent::TreatyEstablished { at_year, .. }
            | GameEvent::TreatyDissolved { at_year, .. }
            | GameEvent::SanctionImposed { at_year, .. }
            | GameEvent::SanctionLifted { at_year, .. }
            | GameEvent::PowerplayOperationResolved { at_year, .. } => *at_year,
        };
        let year1 = match &events[1] {
            GameEvent::DiscoveredSystem { at_year, .. }
            | GameEvent::SurveyedSystem { at_year, .. }
            | GameEvent::FoundedColony { at_year, .. }
            | GameEvent::HomeSystemSelected { at_year, .. }
            | GameEvent::StartingColonySelected { at_year, .. }
            | GameEvent::FactionRelationChanged { at_year, .. }
            | GameEvent::CompletedColonyBuilding { at_year, .. }
            | GameEvent::TreatyEstablished { at_year, .. }
            | GameEvent::TreatyDissolved { at_year, .. }
            | GameEvent::SanctionImposed { at_year, .. }
            | GameEvent::SanctionLifted { at_year, .. }
            | GameEvent::PowerplayOperationResolved { at_year, .. } => *at_year,
        };
        assert!(year0 <= year1, "events were not ordered: {year0} then {year1}");
    }

    #[test]
    fn colonization_range_starts_short_and_scales_with_tech() {
        let mut state = GameState::default();
        let faction_id = state.player.faction_id.clone();

        let base_range = state.faction_colonization_range_world(&faction_id);
        assert_eq!(base_range, 100.0);

        if let Some(faction) = state.factions.get_mut(&faction_id) {
            faction.colonization_tech_level = 3;
        }

        let upgraded_range = state.faction_colonization_range_world(&faction_id);
        assert_eq!(upgraded_range, 130.0);
        assert!(upgraded_range > base_range);
    }

    #[test]
    fn queue_colony_founding_allows_parallel_different_bodies() {
        let mut state = GameState::default();
        let system = test_system(42);

        let first = PendingColonyFounding {
            colony_id: 10,
            colony_name: "Alpha".to_owned(),
            founder_faction: state.player.faction_id.clone(),
            system,
            body_index: 0,
            habitable_site: true,
            earth_like_world: false,
            system_pos: [1.0, 2.0, 3.0],
            element_resource_profile: HashMap::new(),
            atmosphere_resource_profile: HashMap::new(),
            atmosphere_pressure_atm: 0.0,
            source_colony_id: None,
            colonists_sent: 200,
            start_year: 3300.0,
            complete_year: 3301.0,
        };
        let second = PendingColonyFounding {
            colony_id: 11,
            colony_name: "Beta".to_owned(),
            founder_faction: state.player.faction_id.clone(),
            system,
            body_index: 1,
            habitable_site: false,
            earth_like_world: false,
            system_pos: [1.0, 2.0, 3.0],
            element_resource_profile: HashMap::new(),
            atmosphere_resource_profile: HashMap::new(),
            atmosphere_pressure_atm: 0.0,
            source_colony_id: None,
            colonists_sent: 200,
            start_year: 3300.0,
            complete_year: 3301.0,
        };

        assert!(state.queue_colony_founding(3300.0, first).is_ok());
        assert!(state.queue_colony_founding(3300.0, second).is_ok());
        assert_eq!(state.pending_colony_foundings.len(), 2);
    }

    #[test]
    fn queue_colony_founding_rejects_duplicate_body_target() {
        let mut state = GameState::default();
        let system = test_system(43);

        let first = PendingColonyFounding {
            colony_id: 12,
            colony_name: "Gamma".to_owned(),
            founder_faction: state.player.faction_id.clone(),
            system,
            body_index: 2,
            habitable_site: true,
            earth_like_world: false,
            system_pos: [4.0, 5.0, 6.0],
            element_resource_profile: HashMap::new(),
            atmosphere_resource_profile: HashMap::new(),
            atmosphere_pressure_atm: 0.0,
            source_colony_id: None,
            colonists_sent: 200,
            start_year: 3300.0,
            complete_year: 3301.0,
        };
        let duplicate = PendingColonyFounding {
            colony_id: 13,
            colony_name: "Delta".to_owned(),
            founder_faction: state.player.faction_id.clone(),
            system,
            body_index: 2,
            habitable_site: false,
            earth_like_world: false,
            system_pos: [4.0, 5.0, 6.0],
            element_resource_profile: HashMap::new(),
            atmosphere_resource_profile: HashMap::new(),
            atmosphere_pressure_atm: 0.0,
            source_colony_id: None,
            colonists_sent: 200,
            start_year: 3300.0,
            complete_year: 3301.0,
        };

        assert!(state.queue_colony_founding(3300.0, first).is_ok());
        let err = state.queue_colony_founding(3300.0, duplicate).unwrap_err();
        assert_eq!(err, "A colony expedition is already en route to this colony site.");
    }

    #[test]
    fn founded_colony_starts_with_space_station() {
        let mut state = GameState::default();
        let event = GameEvent::FoundedColony {
            at_year: 3301.0,
            colony_id: 44,
            colony_name: "Station Test".to_owned(),
            founder_faction: state.player.faction_id.clone(),
            system: test_system(44),
            body_index: 0,
            habitable_site: true,
            earth_like_world: false,
            system_pos: [0.0, 0.0, 0.0],
            element_resource_profile: HashMap::new(),
            atmosphere_resource_profile: HashMap::new(),
            atmosphere_pressure_atm: 0.0,
            colonists_sent: 1_000,
            source_colony_id: None,
        };

        state.apply_event(&event);

        let colony = state.colonies.get(&44).expect("colony should exist");
        assert_eq!(
            colony.building_level_at_site(
                ColonyBuildingKind::SpaceStation,
                ColonyBuildingSite::Orbital
            ),
            1
        );
    }

    #[test]
    fn player_starting_colony_receives_large_bootstrap_element_stockpile() {
        let mut state = GameState::default();
        let event = GameEvent::FoundedColony {
            at_year: 3301.0,
            colony_id: 88,
            colony_name: "Bootstrap Test".to_owned(),
            founder_faction: state.player.faction_id.clone(),
            system: test_system(88),
            body_index: 0,
            habitable_site: true,
            earth_like_world: false,
            system_pos: [0.0, 0.0, 0.0],
            element_resource_profile: HashMap::new(),
            atmosphere_resource_profile: HashMap::new(),
            atmosphere_pressure_atm: 0.0,
            colonists_sent: 1_000,
            source_colony_id: None,
        };

        state.apply_event(&event);

        let colony = state.colonies.get(&88).expect("colony should exist");
        assert_eq!(state.player.starting_colony_id, Some(88));
        for (symbol, min_amount) in GameState::starting_colony_element_stockpile_targets() {
            let amount = colony.element_stockpiles.get(*symbol).copied().unwrap_or(0.0);
            assert!(
                amount + 0.0001 >= *min_amount,
                "expected {} >= {:.1}, got {:.3}",
                symbol,
                min_amount,
                amount
            );
        }
    }

    #[test]
    fn player_starting_colony_has_minimum_population_of_10000() {
        let mut state = GameState::default();
        let event = GameEvent::FoundedColony {
            at_year: 3301.0,
            colony_id: 89,
            colony_name: "Pop Floor Test".to_owned(),
            founder_faction: state.player.faction_id.clone(),
            system: test_system(89),
            body_index: 0,
            habitable_site: true,
            earth_like_world: false,
            system_pos: [0.0, 0.0, 0.0],
            element_resource_profile: HashMap::new(),
            atmosphere_resource_profile: HashMap::new(),
            atmosphere_pressure_atm: 0.0,
            colonists_sent: 100,
            source_colony_id: None,
        };

        state.apply_event(&event);

        let colony = state.colonies.get(&89).expect("colony should exist");
        assert_eq!(state.player.starting_colony_id, Some(89));
        assert!(
            colony.population >= GameState::STARTING_COLONY_MIN_POPULATION as f64,
            "expected population >= {}, got {}",
            GameState::STARTING_COLONY_MIN_POPULATION,
            colony.population,
        );
    }

    #[test]
    fn queue_colony_building_rejects_incompatible_site_type() {
        let mut state = GameState::default();
        let mut colony = test_colony(61, 700_000.0);
        colony.owner_faction = state.player.faction_id.clone();
        state.colonies.insert(colony.id, colony);

        let err = state
            .queue_colony_building(
                state.current_year,
                61,
                ColonyBuildingKind::IndustrialHub,
                ColonyBuildingSite::Star(0),
            )
            .unwrap_err();
        assert_eq!(
            err,
            "That building cannot be constructed at the selected site type."
        );
    }

    #[test]
    fn queue_colony_building_rejects_gas_giant_surface_sites() {
        let mut state = GameState::default();
        let mut colony = test_colony(62, 700_000.0);
        colony.owner_faction = state.player.faction_id.clone();
        state.colonies.insert(colony.id, colony);

        let err = state
            .queue_colony_building_with_profile(
                state.current_year,
                62,
                ColonyBuildingKind::AgriDome,
                ColonyBuildingSite::Planet(0),
                ColonyBuildingSiteProfile {
                    planet_is_gas_giant: Some(true),
                    planet_habitable: Some(false),
                    planet_building_slot_capacity: Some(4),
                    planet_has_atmosphere: Some(true),
                    star_is_scoopable: None,
                },
            )
            .unwrap_err();
        assert_eq!(
            err,
            "This building requires a solid planet surface (not a gas giant)."
        );
    }

    #[test]
    fn queue_colony_building_rejects_when_planet_slots_are_full() {
        let mut state = GameState::default();
        let mut colony = test_colony(66, 900_000.0);
        colony.owner_faction = state.player.faction_id.clone();
        colony.buildings.push(ColonyBuildingState {
            kind: ColonyBuildingKind::IndustrialHub,
            site: ColonyBuildingSite::Planet(0),
            level: 1,
        });
        state.colonies.insert(colony.id, colony);

        let err = state
            .queue_colony_building_with_profile(
                state.current_year,
                66,
                ColonyBuildingKind::AgriDome,
                ColonyBuildingSite::Planet(0),
                ColonyBuildingSiteProfile {
                    planet_is_gas_giant: Some(false),
                    planet_habitable: Some(true),
                    planet_building_slot_capacity: Some(1),
                    planet_has_atmosphere: Some(true),
                    star_is_scoopable: None,
                },
            )
            .unwrap_err();

        assert_eq!(err, "No free building slots remain on this planet.");
    }

    #[test]
    fn queue_colony_building_upgrade_allowed_when_planet_slots_are_full() {
        let mut state = GameState::default();
        let mut colony = test_colony(67, 900_000.0);
        colony.owner_faction = state.player.faction_id.clone();
        colony.food_stockpile = 90.0;
        colony.industry_stockpile = 90.0;
        colony.energy_stockpile = 90.0;
        colony.element_stockpiles = [
            ("Fe".to_owned(), 80.0),
            ("Al".to_owned(), 80.0),
            ("Si".to_owned(), 80.0),
            ("Cu".to_owned(), 80.0),
            ("Ti".to_owned(), 80.0),
            ("Ni".to_owned(), 80.0),
            ("C".to_owned(), 80.0),
            ("N".to_owned(), 80.0),
            ("P".to_owned(), 80.0),
            ("S".to_owned(), 80.0),
        ]
        .into_iter()
        .collect();
        colony.buildings.push(ColonyBuildingState {
            kind: ColonyBuildingKind::AgriDome,
            site: ColonyBuildingSite::Planet(0),
            level: 1,
        });
        state.colonies.insert(colony.id, colony);

        let queued = state.queue_colony_building_with_profile(
            state.current_year,
            67,
            ColonyBuildingKind::AgriDome,
            ColonyBuildingSite::Planet(0),
            ColonyBuildingSiteProfile {
                planet_is_gas_giant: Some(false),
                planet_habitable: Some(true),
                planet_building_slot_capacity: Some(1),
                planet_has_atmosphere: Some(true),
                star_is_scoopable: None,
            },
        );

        assert!(queued.is_ok(), "upgrading existing building should be allowed");
    }

    #[test]
    fn queue_colony_building_rejects_fuel_scoop_on_non_scoopable_star() {
        let mut state = GameState::default();
        let mut colony = test_colony(68, 750_000.0);
        colony.owner_faction = state.player.faction_id.clone();
        state.colonies.insert(colony.id, colony);

        let err = state
            .queue_colony_building_with_profile(
                state.current_year,
                68,
                ColonyBuildingKind::FuelScoopDroneSwarm,
                ColonyBuildingSite::Star(0),
                ColonyBuildingSiteProfile {
                    planet_is_gas_giant: None,
                    planet_habitable: None,
                    planet_building_slot_capacity: None,
                    planet_has_atmosphere: None,
                    star_is_scoopable: Some(false),
                },
            )
            .unwrap_err();

        assert_eq!(err, "This building requires a hydrogen-fusing star (spectral class O, B, A, F, G, K, or M).");
    }

    #[test]
    fn fuel_scoop_drone_swarm_increases_energy_balance_growth() {
        let mut state = GameState::default();
        let mut base_colony = test_colony(72, 1_100_000.0);
        base_colony.owner_faction = state.player.faction_id.clone();

        let mut boosted_colony = test_colony(73, 1_100_000.0);
        boosted_colony.owner_faction = state.player.faction_id.clone();
        boosted_colony.buildings.push(ColonyBuildingState {
            kind: ColonyBuildingKind::FuelScoopDroneSwarm,
            site: ColonyBuildingSite::Star(0),
            level: 1,
        });

        state.colonies.insert(base_colony.id, base_colony);
        state.colonies.insert(boosted_colony.id, boosted_colony);

        state.advance_strategic_tick(0.4);

        let base_energy = state.colonies.get(&72).unwrap().energy_balance;
        let boosted_energy = state.colonies.get(&73).unwrap().energy_balance;
        assert!(
            boosted_energy > base_energy,
            "expected fuel scoop swarm to improve energy balance ({boosted_energy} <= {base_energy})"
        );
    }

    #[test]
    fn earth_like_world_colony_gets_stronger_growth_and_stability() {
        let mut state = GameState::default();

        let mut regular = test_colony(70, 200_000.0);
        regular.habitable_site = true;
        regular.earth_like_world = false;
        regular.stability = 0.70;

        let mut elw = test_colony(71, 200_000.0);
        elw.habitable_site = true;
        elw.earth_like_world = true;
        elw.stability = 0.70;

        state.colonies.insert(regular.id, regular);
        state.colonies.insert(elw.id, elw);

        state.advance_strategic_tick(0.6);

        let regular_after = state.colonies.get(&70).unwrap();
        let elw_after = state.colonies.get(&71).unwrap();

        assert!(
            elw_after.population > regular_after.population,
            "ELW colony should outgrow regular habitable colony"
        );
        assert!(
            elw_after.stability > regular_after.stability,
            "ELW colony should gain more stability"
        );
        assert!(
            elw_after.food_balance > regular_after.food_balance,
            "ELW colony should gain stronger food balance"
        );
    }

    #[test]
    fn queue_colony_building_deducts_treasury_and_completes() {
        let mut state = GameState::default();
        let mut colony = test_colony(55, 900_000.0);
        colony.owner_faction = state.player.faction_id.clone();
        colony.food_stockpile = 65.0;
        colony.industry_stockpile = 65.0;
        colony.energy_stockpile = 65.0;
        colony.element_stockpiles = [
            ("Fe".to_owned(), 80.0),
            ("Al".to_owned(), 80.0),
            ("Si".to_owned(), 80.0),
            ("Cu".to_owned(), 80.0),
            ("Ti".to_owned(), 80.0),
            ("Ni".to_owned(), 80.0),
            ("C".to_owned(), 80.0),
            ("N".to_owned(), 80.0),
            ("P".to_owned(), 80.0),
            ("S".to_owned(), 80.0),
        ]
        .into_iter()
        .collect();
        state.colonies.insert(colony.id, colony);

        let treasury_before = state
            .factions
            .get(&state.player.faction_id)
            .map(|f| f.treasury)
            .unwrap_or(0);
        let colony_before = state.colonies.get(&55).unwrap().clone();

        let (duration, cost, target_level) = state
            .queue_colony_building(
                state.current_year,
                55,
                ColonyBuildingKind::IndustrialHub,
                ColonyBuildingSite::Planet(0),
            )
            .expect("queue should succeed");

        assert!(duration > 0.0);
        assert_eq!(target_level, 1);

        let treasury_after_queue = state
            .factions
            .get(&state.player.faction_id)
            .map(|f| f.treasury)
            .unwrap_or(0);
        assert_eq!(treasury_after_queue, treasury_before - cost);

        let colony_after_queue = state.colonies.get(&55).unwrap();
        assert!(
            colony_after_queue
                .element_stockpiles
                .get("Fe")
                .copied()
                .unwrap_or(0.0)
                < colony_before
                    .element_stockpiles
                    .get("Fe")
                    .copied()
                    .unwrap_or(0.0)
        );
        assert!(colony_after_queue.food_stockpile < colony_before.food_stockpile);
        assert!(colony_after_queue.industry_stockpile < colony_before.industry_stockpile);
        assert!(colony_after_queue.energy_stockpile < colony_before.energy_stockpile);

        let events = state.advance_strategic_tick(duration + 0.05);
        let completion = events
            .into_iter()
            .find(|event| {
                matches!(
                    event,
                    GameEvent::CompletedColonyBuilding {
                        colony_id: 55,
                        kind: ColonyBuildingKind::IndustrialHub,
                        site: ColonyBuildingSite::Planet(0),
                        target_level: 1,
                        ..
                    }
                )
            })
            .expect("completion event should be generated");
        state.apply_event(&completion);

        let colony_after = state.colonies.get(&55).unwrap();
        assert_eq!(
            colony_after.building_level_at_site(
                ColonyBuildingKind::IndustrialHub,
                ColonyBuildingSite::Planet(0)
            ),
            1
        );
    }

    #[test]
    fn queue_colony_building_rejects_second_project_for_same_colony() {
        let mut state = GameState::default();
        let mut colony = test_colony(58, 800_000.0);
        colony.owner_faction = state.player.faction_id.clone();
        state.colonies.insert(colony.id, colony);

        state
            .queue_colony_building(
                state.current_year,
                58,
                ColonyBuildingKind::IndustrialHub,
                ColonyBuildingSite::Planet(0),
            )
            .expect("first queue should succeed");

        let err = state
            .queue_colony_building(
                state.current_year,
                58,
                ColonyBuildingKind::AgriDome,
                ColonyBuildingSite::Planet(0),
            )
            .unwrap_err();
        assert_eq!(
            err,
            "Another building is already under construction for this colony."
        );
    }

    #[test]
    fn queue_colony_building_rejects_when_colony_stockpiles_are_too_low() {
        let mut state = GameState::default();
        let mut colony = test_colony(63, 850_000.0);
        colony.owner_faction = state.player.faction_id.clone();
        colony.food_stockpile = 1.0;
        colony.industry_stockpile = 1.0;
        colony.energy_stockpile = 1.0;
        colony.element_stockpiles = HashMap::new();
        state.colonies.insert(colony.id, colony);

        let err = state
            .queue_colony_building(
                state.current_year,
                63,
                ColonyBuildingKind::IndustrialHub,
                ColonyBuildingSite::Planet(0),
            )
            .unwrap_err();
        assert_eq!(
            err,
            "Insufficient colony stockpiles (elements/food/industry/energy) for this construction."
        );
    }

    #[test]
    fn space_station_boosts_non_element_resource_balances() {
        let mut state = GameState::default();
        let mut no_station = test_colony(64, 1_000_000.0);
        no_station.owner_faction = state.player.faction_id.clone();

        let mut with_station = test_colony(65, 1_000_000.0);
        with_station.owner_faction = state.player.faction_id.clone();
        with_station.buildings.push(ColonyBuildingState {
            kind: ColonyBuildingKind::SpaceStation,
            site: ColonyBuildingSite::Orbital,
            level: 1,
        });

        state.colonies.insert(no_station.id, no_station);
        state.colonies.insert(with_station.id, with_station);

        state.advance_strategic_tick(0.45);

        let base = state.colonies.get(&64).unwrap();
        let boosted = state.colonies.get(&65).unwrap();
        assert!(boosted.food_balance > base.food_balance);
        assert!(boosted.industry_balance > base.industry_balance);
        assert!(boosted.energy_balance > base.energy_balance);
    }

    #[test]
    fn industrial_hub_increases_industry_balance_growth() {
        let mut state = GameState::default();
        let mut base_colony = test_colony(56, 1_200_000.0);
        base_colony.owner_faction = state.player.faction_id.clone();

        let mut boosted_colony = test_colony(57, 1_200_000.0);
        boosted_colony.owner_faction = state.player.faction_id.clone();
        boosted_colony.buildings.push(ColonyBuildingState {
            kind: ColonyBuildingKind::IndustrialHub,
            site: ColonyBuildingSite::Planet(0),
            level: 1,
        });

        state.colonies.insert(base_colony.id, base_colony);
        state.colonies.insert(boosted_colony.id, boosted_colony);

        state.advance_strategic_tick(0.4);

        let base_industry = state.colonies.get(&56).unwrap().industry_balance;
        let boosted_industry = state.colonies.get(&57).unwrap().industry_balance;
        assert!(
            boosted_industry > base_industry,
            "expected industrial hub to improve industry balance ({boosted_industry} <= {base_industry})"
        );
    }

    #[test]
    fn agri_dome_increases_food_balance_growth() {
        let mut state = GameState::default();
        let mut base_colony = test_colony(59, 1_100_000.0);
        base_colony.owner_faction = state.player.faction_id.clone();

        let mut boosted_colony = test_colony(60, 1_100_000.0);
        boosted_colony.owner_faction = state.player.faction_id.clone();
        boosted_colony.buildings.push(ColonyBuildingState {
            kind: ColonyBuildingKind::AgriDome,
            site: ColonyBuildingSite::Planet(0),
            level: 1,
        });

        state.colonies.insert(base_colony.id, base_colony);
        state.colonies.insert(boosted_colony.id, boosted_colony);

        state.advance_strategic_tick(0.4);

        let base_food = state.colonies.get(&59).unwrap().food_balance;
        let boosted_food = state.colonies.get(&60).unwrap().food_balance;
        assert!(
            boosted_food > base_food,
            "expected agri dome to improve food balance ({boosted_food} <= {base_food})"
        );
    }

    #[test]
    fn deep_mantle_mining_still_extracts_when_element_stockpiles_start_high() {
        let mut state = GameState::default();

        let mut base_colony = test_colony(74, 1_100_000.0);
        base_colony.owner_faction = state.player.faction_id.clone();
        base_colony.stockpile_capacity = 100.0;
        base_colony.element_stockpiles = [
            ("Fe".to_owned(), 100.0),
            ("Si".to_owned(), 90.0),
            ("Al".to_owned(), 70.0),
            ("Cu".to_owned(), 50.0),
            ("Ti".to_owned(), 40.0),
            ("Ni".to_owned(), 50.0),
        ]
        .into_iter()
        .collect();
        base_colony.element_resource_profile = GameState::default_element_resource_profile();

        let mut boosted_colony = base_colony.clone();
        boosted_colony.id = 75;
        boosted_colony.name = "Test-75".to_owned();
        boosted_colony.buildings.push(ColonyBuildingState {
            kind: ColonyBuildingKind::DeepMantleMiningStation,
            site: ColonyBuildingSite::Planet(0),
            level: 1,
        });

        state.colonies.insert(base_colony.id, base_colony);
        state.colonies.insert(boosted_colony.id, boosted_colony);

        let base_before = GameState::total_element_stockpile(state.colonies.get(&74).unwrap());
        let boosted_before = GameState::total_element_stockpile(state.colonies.get(&75).unwrap());

        state.advance_strategic_tick(0.5);

        let base_after = GameState::total_element_stockpile(state.colonies.get(&74).unwrap());
        let boosted_after = GameState::total_element_stockpile(state.colonies.get(&75).unwrap());

        assert!(base_after > base_before, "expected baseline extraction to increase element stockpiles");
        assert!(
            boosted_after > base_after,
            "expected deep mantle mining station to add extra extraction ({boosted_after} <= {base_after})"
        );
        assert!(boosted_after > boosted_before);
    }

    #[test]
    fn tax_revenue_scales_sublinearly_for_large_populations() {
        let mut colony_mid = test_colony(90, 8_000_000.0);
        colony_mid.stability = 0.95;
        colony_mid.taxation_policy = TaxationPolicy::Standard;

        let mut colony_high = colony_mid.clone();
        colony_high.id = 91;
        colony_high.population = 16_000_000.0;

        let mid_revenue = GameState::colony_tax_revenue_annual(&colony_mid);
        let high_revenue = GameState::colony_tax_revenue_annual(&colony_high);

        assert!(high_revenue > mid_revenue);
        assert!(
            high_revenue < mid_revenue.saturating_mul(2),
            "expected diminishing tax returns at high population"
        );
    }

    #[test]
    fn over_capacity_colony_population_contracts() {
        let mut state = GameState::default();
        let mut colony = test_colony(92, 9_000_000.0);
        colony.stability = 0.95;
        colony.food_balance = 0.35;
        colony.industry_balance = 0.35;
        colony.energy_balance = 0.35;
        colony.habitable_site = false;
        colony.earth_like_world = false;
        state.colonies.insert(colony.id, colony);

        let before = state.colonies.get(&92).unwrap().population;
        state.advance_strategic_tick(0.5);
        let after = state.colonies.get(&92).unwrap().population;

        assert!(
            after < before,
            "expected over-cap population to contract, before={before}, after={after}"
        );
    }
}
