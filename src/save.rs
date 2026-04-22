use std::fs;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::events::GameEvent;
use crate::game_state::{ColonyBuildingKind, ColonyBuildingSite, ColonyBuildingState, ColonyState, GameState};

const SAVE_VERSION_V1: u32 = 1;
const SAVE_VERSION_V2: u32 = 2;
const SAVE_VERSION_V3: u32 = 3;
const SAVE_VERSION_V4: u32 = 4;
const SAVE_VERSION_V5: u32 = 5;
const SAVE_VERSION_V6: u32 = 6;
const SAVE_VERSION_V7: u32 = 7;
const CURRENT_SAVE_VERSION: u32 = SAVE_VERSION_V7;
const V1_TO_V2_COLONIZATION_RANGE_SCALE: f32 = 0.05;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GameSaveFile {
    version: u32,
    state: GameState,
    events: Vec<GameEvent>,
}

#[derive(Clone, Debug, Deserialize)]
struct GameSaveFileV0 {
    state: GameState,
    #[serde(default)]
    events: Vec<GameEvent>,
}

fn parse_game_save_file(content: &str) -> io::Result<GameSaveFile> {
    let raw: Value = serde_json::from_str(content).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse game save JSON: {err}"),
        )
    })?;

    migrate_game_save(raw)
}

fn migrate_game_save(raw: Value) -> io::Result<GameSaveFile> {
    let version = raw
        .get("version")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(0);

    match version {
        CURRENT_SAVE_VERSION => serde_json::from_value::<GameSaveFile>(raw).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse current save format: {err}"),
            )
        }),
        SAVE_VERSION_V6 => migrate_v6_to_v7(raw),
        SAVE_VERSION_V5 => migrate_v5_to_v6(raw).and_then(migrate_v6_to_v7_file),
        SAVE_VERSION_V4 => migrate_v4_to_v5(raw)
            .and_then(migrate_v5_to_v6_file)
            .and_then(migrate_v6_to_v7_file),
        SAVE_VERSION_V3 => migrate_v3_to_v4(raw)
            .and_then(migrate_v4_to_v5_file)
            .and_then(migrate_v5_to_v6_file)
            .and_then(migrate_v6_to_v7_file),
        SAVE_VERSION_V2 => migrate_v2_to_v3(raw)
            .and_then(migrate_v3_to_v4_file)
            .and_then(migrate_v4_to_v5_file)
            .and_then(migrate_v5_to_v6_file)
            .and_then(migrate_v6_to_v7_file),
        SAVE_VERSION_V1 => migrate_v1_to_v2(raw)
            .and_then(migrate_v2_to_v3_file)
            .and_then(migrate_v3_to_v4_file)
            .and_then(migrate_v4_to_v5_file)
            .and_then(migrate_v5_to_v6_file)
            .and_then(migrate_v6_to_v7_file),
        0 => migrate_legacy_v0(raw),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported save version {} (expected {} or legacy unversioned format)",
                other, CURRENT_SAVE_VERSION
            ),
        )),
    }
}

fn migrate_v1_to_v2(raw: Value) -> io::Result<GameSaveFile> {
    let mut parsed_v1: GameSaveFile = serde_json::from_value(raw).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse v1 save format: {err}"),
        )
    })?;

    parsed_v1.version = SAVE_VERSION_V2;
    parsed_v1.state.base_colonization_range_world =
        (parsed_v1.state.base_colonization_range_world * V1_TO_V2_COLONIZATION_RANGE_SCALE)
            .clamp(20.0, 5_000.0);
    Ok(parsed_v1)
}

fn migrate_v2_to_v3(raw: Value) -> io::Result<GameSaveFile> {
    let parsed_v2: GameSaveFile = serde_json::from_value(raw).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse v2 save format: {err}"),
        )
    })?;

    migrate_v2_to_v3_file(parsed_v2)
}

fn migrate_v2_to_v3_file(mut parsed_v2: GameSaveFile) -> io::Result<GameSaveFile> {
    parsed_v2.version = SAVE_VERSION_V3;
    for colony in parsed_v2.state.colonies.values_mut() {
        seed_colony_stockpiles(colony);
        seed_element_stockpiles(colony);
    }
    Ok(parsed_v2)
}

fn migrate_v3_to_v4(raw: Value) -> io::Result<GameSaveFile> {
    let parsed_v3: GameSaveFile = serde_json::from_value(raw).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse v3 save format: {err}"),
        )
    })?;

    migrate_v3_to_v4_file(parsed_v3)
}

fn migrate_v3_to_v4_file(mut parsed_v3: GameSaveFile) -> io::Result<GameSaveFile> {
    parsed_v3.version = SAVE_VERSION_V4;
    for colony in parsed_v3.state.colonies.values_mut() {
        seed_space_station(colony);
        seed_element_stockpiles(colony);
    }
    Ok(parsed_v3)
}

fn migrate_v4_to_v5(raw: Value) -> io::Result<GameSaveFile> {
    let parsed_v4: GameSaveFile = serde_json::from_value(raw).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse v4 save format: {err}"),
        )
    })?;
    migrate_v4_to_v5_file(parsed_v4)
}

fn migrate_v4_to_v5_file(mut parsed_v4: GameSaveFile) -> io::Result<GameSaveFile> {
    parsed_v4.version = SAVE_VERSION_V5;
    for faction_id in parsed_v4.state.factions.keys() {
        parsed_v4
            .state
            .player_reputation
            .entry(faction_id.clone())
            .or_insert(0);
    }
    Ok(parsed_v4)
}

fn migrate_v5_to_v6(raw: Value) -> io::Result<GameSaveFile> {
    let parsed_v5: GameSaveFile = serde_json::from_value(raw).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse v5 save format: {err}"),
        )
    })?;
    migrate_v5_to_v6_file(parsed_v5)
}

fn migrate_v5_to_v6_file(mut parsed_v5: GameSaveFile) -> io::Result<GameSaveFile> {
    parsed_v5.version = SAVE_VERSION_V6;
    for pending in parsed_v5.state.pending_colony_buildings.iter_mut() {
        pending.deferred_treasury_due = pending.deferred_treasury_due.max(0);
        pending.annual_construction_upkeep = pending.annual_construction_upkeep.max(0);
    }
    Ok(parsed_v5)
}

fn migrate_v6_to_v7(raw: Value) -> io::Result<GameSaveFile> {
    let parsed_v6: GameSaveFile = serde_json::from_value(raw).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse v6 save format: {err}"),
        )
    })?;
    migrate_v6_to_v7_file(parsed_v6)
}

fn migrate_v6_to_v7_file(mut parsed_v6: GameSaveFile) -> io::Result<GameSaveFile> {
    parsed_v6.version = SAVE_VERSION_V7;
    parsed_v6
        .state
        .recent_powerplay_ops
        .retain(|op| op.at_year.is_finite());
    parsed_v6
        .state
        .active_sanctions
        .retain(|_, expires| expires.is_finite() && *expires >= parsed_v6.state.current_year - 10.0);
    Ok(parsed_v6)
}

fn seed_colony_stockpiles(colony: &mut ColonyState) {
    let capacity = colony.stockpile_capacity.max(20.0);
    colony.stockpile_capacity = capacity;

    if colony.food_stockpile <= 0.0 || !colony.food_stockpile.is_finite() {
        colony.food_stockpile =
            ((colony.food_balance + 0.35) / 0.70 * capacity).clamp(0.0, capacity);
    }
    if colony.industry_stockpile <= 0.0 || !colony.industry_stockpile.is_finite() {
        colony.industry_stockpile =
            ((colony.industry_balance + 0.35) / 0.70 * capacity).clamp(0.0, capacity);
    }
    if colony.energy_stockpile <= 0.0 || !colony.energy_stockpile.is_finite() {
        colony.energy_stockpile =
            ((colony.energy_balance + 0.35) / 0.70 * capacity).clamp(0.0, capacity);
    }
}

fn seed_element_stockpiles(colony: &mut ColonyState) {
    let already_seeded = colony
        .element_stockpiles
        .values()
        .any(|value| *value > 0.0 && value.is_finite());
    if already_seeded {
        return;
    }

    let base_total = if colony.earth_like_world {
        140.0
    } else if colony.habitable_site {
        95.0
    } else {
        60.0
    };
    colony.element_stockpiles = [
        ("Fe".to_owned(), base_total * 0.22),
        ("Si".to_owned(), base_total * 0.16),
        ("Al".to_owned(), base_total * 0.10),
        ("Cu".to_owned(), base_total * 0.06),
        ("Ti".to_owned(), base_total * 0.04),
        ("Ni".to_owned(), base_total * 0.05),
        ("C".to_owned(), base_total * 0.08),
        ("N".to_owned(), base_total * 0.08),
        ("O".to_owned(), base_total * 0.10),
        ("Mg".to_owned(), base_total * 0.06),
        ("S".to_owned(), base_total * 0.03),
        ("P".to_owned(), base_total * 0.02),
    ]
    .into_iter()
    .collect();
}

fn seed_space_station(colony: &mut ColonyState) {
    let has_space_station = colony
        .buildings
        .iter()
        .any(|building| {
            building.kind == ColonyBuildingKind::SpaceStation
                && building.site == ColonyBuildingSite::Orbital
                && building.level >= 1
        });
    if !has_space_station {
        colony.buildings.push(ColonyBuildingState {
            kind: ColonyBuildingKind::SpaceStation,
            site: ColonyBuildingSite::Orbital,
            level: 1,
        });
    }
}

fn migrate_legacy_v0(raw: Value) -> io::Result<GameSaveFile> {
    if let Ok(v0) = serde_json::from_value::<GameSaveFileV0>(raw.clone()) {
        let file = GameSaveFile {
            version: SAVE_VERSION_V2,
            state: v0.state,
            events: v0.events,
        };
        return migrate_v2_to_v3_file(file)
            .and_then(migrate_v3_to_v4_file)
            .and_then(migrate_v4_to_v5_file)
            .and_then(migrate_v5_to_v6_file)
            .and_then(migrate_v6_to_v7_file);
    }

    if let Ok(state_only) = serde_json::from_value::<GameState>(raw) {
        let file = GameSaveFile {
            version: SAVE_VERSION_V2,
            state: state_only,
            events: Vec::new(),
        };
        return migrate_v2_to_v3_file(file)
            .and_then(migrate_v3_to_v4_file)
            .and_then(migrate_v4_to_v5_file)
            .and_then(migrate_v5_to_v6_file)
            .and_then(migrate_v6_to_v7_file);
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "failed to migrate legacy save: expected {state, events?} or raw GameState shape",
    ))
}

pub fn load_game_save(path: impl AsRef<Path>) -> io::Result<(GameState, Vec<GameEvent>)> {
    let path = path.as_ref();
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Ok((GameState::default(), Vec::new()));
        }
        Err(err) => return Err(err),
    };

    let mut parsed = parse_game_save_file(&content)?;
    for colony in parsed.state.colonies.values_mut() {
        seed_element_stockpiles(colony);
    }

    Ok((parsed.state, parsed.events))
}

#[allow(dead_code)]
pub fn save_game_save(
    path: impl AsRef<Path>,
    state: &GameState,
    events: &[GameEvent],
) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = GameSaveFile {
        version: CURRENT_SAVE_VERSION,
        state: state.clone(),
        events: events.to_vec(),
    };

    let json = serde_json::to_string_pretty(&file).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to serialize game save JSON: {err}"),
        )
    })?;

    fs::write(path, json)
}

pub fn save_game_save_compact(
    path: impl AsRef<Path>,
    state: &GameState,
    events: &[GameEvent],
) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = GameSaveFile {
        version: CURRENT_SAVE_VERSION,
        state: state.clone(),
        events: events.to_vec(),
    };

    let json = serde_json::to_string(&file).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to serialize compact game save JSON: {err}"),
        )
    })?;

    fs::write(path, json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_current_version_save() {
        let state = GameState::default();
        let raw = json!({
            "version": CURRENT_SAVE_VERSION,
            "state": state,
            "events": [],
        });

        let parsed = migrate_game_save(raw).expect("current save should parse");
        assert_eq!(parsed.version, CURRENT_SAVE_VERSION);
    }

    #[test]
    fn migrates_v1_save_and_rebalances_colonization_range() {
        let mut state = GameState::default();
        state.base_colonization_range_world = 8_000.0;
        let raw = json!({
            "version": SAVE_VERSION_V1,
            "state": state,
            "events": [],
        });

        let parsed = migrate_game_save(raw).expect("v1 save should migrate");
        assert_eq!(parsed.version, CURRENT_SAVE_VERSION);
        assert_eq!(parsed.state.base_colonization_range_world, 400.0);
    }

    #[test]
    fn migrates_v2_save_and_seeds_stockpiles() {
        let mut state = GameState::default();
        let colony_id = 1_u64;
        state.colonies.insert(
            colony_id,
            crate::game_state::ColonyState {
                id: colony_id,
                name: "Migration Test Colony".to_owned(),
                owner_faction: state.player.faction_id.clone(),
                system: crate::procedural_galaxy::SystemId {
                    sector: crate::procedural_galaxy::SectorCoord { x: 0, y: 0 },
                    local_index: 0,
                },
                body_index: 0,
                habitable_site: false,
                earth_like_world: false,
                system_pos: [0.0, 0.0, 0.0],
                policy: crate::game_state::ColonyPolicy::Balanced,
                taxation_policy: crate::game_state::TaxationPolicy::Standard,
                stage: crate::game_state::ColonyStage::Outpost,
                population: 1_000.0,
                stability: 0.5,
                food_balance: 0.10,
                industry_balance: 0.06,
                energy_balance: 0.08,
                defense_balance: 0.0,
                stockpile_capacity: 100.0,
                food_stockpile: 0.0,
                industry_stockpile: 0.0,
                energy_stockpile: 0.0,
                element_stockpiles: std::collections::HashMap::new(),
                atmosphere_stockpiles: std::collections::HashMap::new(),
                element_resource_profile: std::collections::HashMap::new(),
                atmosphere_resource_profile: std::collections::HashMap::new(),
                atmosphere_pressure_atm: 0.0,
                buildings: Vec::new(),
                last_tax_revenue_annual: 0,
                last_upkeep_cost_annual: 0,
                last_net_revenue_annual: 0,
            },
        );

        let raw = json!({
            "version": SAVE_VERSION_V2,
            "state": state,
            "events": [],
        });

        let parsed = migrate_game_save(raw).expect("v2 save should migrate");
        assert_eq!(parsed.version, CURRENT_SAVE_VERSION);

        let migrated = parsed
            .state
            .colonies
            .get(&colony_id)
            .expect("colony should exist");
        assert!(migrated.food_stockpile > 0.0);
        assert!(migrated.industry_stockpile > 0.0);
        assert!(migrated.energy_stockpile > 0.0);
        assert!(
            migrated
                .element_stockpiles
                .values()
                .copied()
                .sum::<f32>()
                > 0.0
        );
    }

    #[test]
    fn migrates_v3_save_and_seeds_space_station() {
        let mut state = GameState::default();
        let colony_id = 2_u64;
        state.colonies.insert(
            colony_id,
            crate::game_state::ColonyState {
                id: colony_id,
                name: "Station Migration Colony".to_owned(),
                owner_faction: state.player.faction_id.clone(),
                system: crate::procedural_galaxy::SystemId {
                    sector: crate::procedural_galaxy::SectorCoord { x: 0, y: 0 },
                    local_index: 1,
                },
                body_index: 0,
                habitable_site: true,
                earth_like_world: false,
                system_pos: [0.0, 0.0, 0.0],
                policy: crate::game_state::ColonyPolicy::Balanced,
                taxation_policy: crate::game_state::TaxationPolicy::Standard,
                stage: crate::game_state::ColonyStage::Settlement,
                population: 10_000.0,
                stability: 0.8,
                food_balance: 0.1,
                industry_balance: 0.1,
                energy_balance: 0.1,
                defense_balance: 0.02,
                stockpile_capacity: 120.0,
                food_stockpile: 60.0,
                industry_stockpile: 60.0,
                energy_stockpile: 60.0,
                element_stockpiles: std::collections::HashMap::new(),
                atmosphere_stockpiles: std::collections::HashMap::new(),
                element_resource_profile: std::collections::HashMap::new(),
                atmosphere_resource_profile: std::collections::HashMap::new(),
                atmosphere_pressure_atm: 0.0,
                buildings: Vec::new(),
                last_tax_revenue_annual: 0,
                last_upkeep_cost_annual: 0,
                last_net_revenue_annual: 0,
            },
        );

        let raw = json!({
            "version": SAVE_VERSION_V3,
            "state": state,
            "events": [],
        });

        let parsed = migrate_game_save(raw).expect("v3 save should migrate");
        assert_eq!(parsed.version, CURRENT_SAVE_VERSION);
        let colony = parsed
            .state
            .colonies
            .get(&colony_id)
            .expect("colony should exist");
        assert!(colony.buildings.iter().any(|building| {
            building.kind == ColonyBuildingKind::SpaceStation
                && building.site == ColonyBuildingSite::Orbital
                && building.level >= 1
        }));
    }

    #[test]
    fn migrates_legacy_wrapped_save_without_version() {
        let state = GameState::default();
        let raw = json!({
            "state": state,
            "events": [],
        });

        let parsed = migrate_game_save(raw).expect("legacy wrapped save should migrate");
        assert_eq!(parsed.version, CURRENT_SAVE_VERSION);
        assert!(parsed.events.is_empty());
    }

    #[test]
    fn migrates_legacy_state_only_save() {
        let raw = serde_json::to_value(GameState::default()).expect("state should serialize");

        let parsed = migrate_game_save(raw).expect("legacy state-only save should migrate");
        assert_eq!(parsed.version, CURRENT_SAVE_VERSION);
        assert!(parsed.events.is_empty());
    }

    #[test]
    fn rejects_unsupported_future_version() {
        let raw = json!({
            "version": CURRENT_SAVE_VERSION + 5,
            "state": GameState::default(),
            "events": [],
        });

        let err = migrate_game_save(raw).expect_err("future version should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("unsupported save version"),
            "unexpected error message: {}",
            err
        );
    }
}
