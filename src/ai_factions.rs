use std::collections::{HashMap, HashSet};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::events::GameEvent;
use crate::game_state::{
    ColonyBuildingKind, ColonyBuildingSite, ColonyBuildingSiteProfile, GameState,
    PendingColonyFounding, SurveyStage,
};
use crate::procedural_galaxy::{GalaxyGenerator, PlanetKind, SectorCoord, SystemId, SystemSummary};

/// How frequently (in game-years) each AI decision category fires.
const SURVEY_INTERVAL_YEARS: f32 = 2.0;
const COLONIZE_INTERVAL_YEARS: f32 = 3.0;
const BUILD_INTERVAL_YEARS: f32 = 2.0;

/// Concurrency caps per faction.
const MAX_CONCURRENT_SURVEYS: usize = 4;
const MAX_CONCURRENT_COLONY_FOUNDINGS: usize = 2;

/// How many sectors outward from each colony the AI scans for survey targets.
const SURVEY_SEARCH_RADIUS_SECTORS: i32 = 2;
/// Maximum candidate systems to evaluate per decision cycle.
const MAX_SURVEY_CANDIDATES_PER_CYCLE: usize = 8;

/// Maximum number of sector generations allowed per AI faction per decision cycle.
/// This is the primary knob to prevent frame-time spikes.
const MAX_SECTOR_GENS_PER_CYCLE: usize = 6;

/// Colony founding parameters.
const COLONY_FOUNDING_DURATION_YEARS: f32 = 1.80;
const COLONISTS_SENT: u32 = 5_000;
/// Minimum total faction net annual income required before the AI will found new colonies.
/// This prevents the AI from expanding into debt before existing colonies are profitable.
const MIN_FACTION_INCOME_FOR_COLONY: i64 = 4_000;
const MIN_TREASURY_FOR_BUILDING: i64 = 25_000;
const MIN_SOURCE_POPULATION_FOR_COLONY: f64 = 2_000.0;

/// AI faction identifiers.
const AI_FACTION_IDS: &[&str] = &["raccoon-flood", "helios-league", "orion-syndicate"];

/// Minimum distance from galaxy center for AI home positions (avoids the dense core).
const HOME_MIN_RADIUS: f32 = 8_000.0;
/// Maximum distance from galaxy center for AI home positions.
const HOME_MAX_RADIUS: f32 = 35_000.0;
/// Minimum angular separation (radians) between AI home positions.
const HOME_MIN_ANGLE_SEP: f32 = std::f32::consts::FRAC_PI_3; // 60°

/// Generate randomized home positions for AI factions using the galaxy seed.
fn generate_home_positions(generator: &GalaxyGenerator) -> Vec<[f32; 3]> {
    let cfg = generator.config();
    let mut rng = StdRng::seed_from_u64(cfg.galaxy_seed.wrapping_add(0xA1_FA_C7_10));
    let center = cfg.center;
    let z_mid = (cfg.z_min + cfg.z_max) * 0.5;

    let mut positions = Vec::with_capacity(AI_FACTION_IDS.len());
    for i in 0..AI_FACTION_IDS.len() {
        // Try to find a position that is well-separated from previously placed factions.
        let mut best = [0.0f32; 3];
        let mut best_min_sep = -1.0f32;
        for _ in 0..64 {
            let angle: f32 = rng.gen_range(0.0..std::f32::consts::TAU);
            let radius: f32 = rng.gen_range(HOME_MIN_RADIUS..HOME_MAX_RADIUS);
            let x = center[0] + angle.cos() * radius;
            let y = center[1] + angle.sin() * radius;
            let z = z_mid + rng.gen_range(-2000.0..2000.0);
            let candidate = [x, y, z];

            // Check angular separation from all previously placed factions.
            let min_sep = positions.iter().map(|prev: &[f32; 3]| {
                let dx1 = prev[0] - center[0];
                let dy1 = prev[1] - center[1];
                let dx2 = candidate[0] - center[0];
                let dy2 = candidate[1] - center[1];
                let dot = dx1 * dx2 + dy1 * dy2;
                let mag = (dx1 * dx1 + dy1 * dy1).sqrt() * (dx2 * dx2 + dy2 * dy2).sqrt();
                if mag < 1.0 { 0.0 } else { (dot / mag).clamp(-1.0, 1.0).acos() }
            }).min_by(|a, b| a.total_cmp(b)).unwrap_or(std::f32::consts::TAU);

            if min_sep > best_min_sep {
                best = candidate;
                best_min_sep = min_sep;
            }
            // Good enough if well separated.
            if min_sep >= HOME_MIN_ANGLE_SEP {
                best = candidate;
                break;
            }
        }
        positions.push(best);
        let _ = i; // suppress unused warning
    }
    positions
}

/// Per-faction persistent AI state.
struct FactionAiState {
    faction_id: String,
    home_pos: [f32; 3],
    bootstrapped: bool,
    last_survey_year: f32,
    last_colonize_year: f32,
    last_build_year: f32,
    /// Rotating index into the discovery sector ring, so we only generate
    /// a few sectors per cycle instead of the full grid at once.
    discovery_sector_cursor: usize,
}

pub struct AiFactionController {
    factions: Vec<FactionAiState>,
}

impl AiFactionController {
    pub fn new(generator: &GalaxyGenerator) -> Self {
        let home_positions = generate_home_positions(generator);
        let cfg = generator.config();
        let mut rng = StdRng::seed_from_u64(cfg.galaxy_seed.wrapping_add(0xA1_FA_C7_20));
        let factions = AI_FACTION_IDS
            .iter()
            .zip(home_positions)
            .map(|(faction_id, home_pos)| {
                // Stagger initial timers so factions don't all act on the same tick.
                let survey_offset: f32 = rng.gen_range(0.0..SURVEY_INTERVAL_YEARS);
                let colonize_offset: f32 = rng.gen_range(0.0..COLONIZE_INTERVAL_YEARS);
                let build_offset: f32 = rng.gen_range(0.0..BUILD_INTERVAL_YEARS);
                FactionAiState {
                    faction_id: faction_id.to_string(),
                    home_pos,
                    bootstrapped: false,
                    last_survey_year: -survey_offset,
                    last_colonize_year: -colonize_offset,
                    last_build_year: -build_offset,
                    discovery_sector_cursor: 0,
                }
            })
            .collect();
        Self { factions }
    }

    /// Run one AI decision pass. Call once per strategic tick from the main loop.
    /// Returns any events that should be applied immediately (e.g. bootstrap colonies).
    pub fn tick(
        &mut self,
        game_state: &mut GameState,
        generator: &GalaxyGenerator,
    ) -> Vec<GameEvent> {
        let mut events = Vec::new();
        let current_year = game_state.current_year;
        let sector_size = generator.config().sector_size;
        let center = generator.config().center;

        for ai in &mut self.factions {
            // Skip factions that no longer exist in the game state.
            if !game_state.factions.contains_key(&ai.faction_id) {
                continue;
            }

            // ── Bootstrap: give each AI a starting colony if it has none ──
            if !ai.bootstrapped {
                let has_colony = game_state
                    .colonies
                    .values()
                    .any(|c| c.owner_faction == ai.faction_id);
                if !has_colony {
                    if let Some(event) = bootstrap_colony(ai, game_state, generator) {
                        events.push(event);
                    }
                }
                ai.bootstrapped = true;
            }

            // ── Survey: discover and scan systems near colonies ──
            if current_year - ai.last_survey_year >= SURVEY_INTERVAL_YEARS {
                ai.last_survey_year = current_year;
                run_survey_decisions(ai, game_state, generator, sector_size, center);
            }

            // ── Colonize: found new colonies on assessed systems ──
            if current_year - ai.last_colonize_year >= COLONIZE_INTERVAL_YEARS {
                ai.last_colonize_year = current_year;
                run_colonize_decisions(ai, game_state, generator);
            }

            // ── Build: queue buildings at existing colonies ──
            if current_year - ai.last_build_year >= BUILD_INTERVAL_YEARS {
                ai.last_build_year = current_year;
                run_build_decisions(ai, game_state);
            }
        }

        events
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bootstrap
// ─────────────────────────────────────────────────────────────────────────────

/// Create a starting colony event for an AI faction by finding the nearest
/// habitable system to its home position.
fn bootstrap_colony(
    ai: &FactionAiState,
    game_state: &mut GameState,
    generator: &GalaxyGenerator,
) -> Option<GameEvent> {
    let cfg = generator.config();
    let sx = ((ai.home_pos[0] - cfg.center[0]) / cfg.sector_size).floor() as i32;
    let sy = ((ai.home_pos[1] - cfg.center[1]) / cfg.sector_size).floor() as i32;

    // Search expanding rings of sectors around home_pos for a habitable system.
    for radius in 0i32..=6 {
        for dx in -radius..=radius {
            for dy in -radius..=radius {
                if dx.abs() != radius && dy.abs() != radius {
                    continue; // only the ring edge
                }
                let coord = SectorCoord {
                    x: sx + dx,
                    y: sy + dy,
                };
                let systems = generator.generate_sector(coord);
                for summary in &systems {
                    // Skip systems already colonized by anyone.
                    if game_state
                        .colonies
                        .values()
                        .any(|c| c.system == summary.id)
                    {
                        continue;
                    }

                    let detail = generator.generate_system_detail(summary);
                    let candidate = detail
                        .planets
                        .iter()
                        .enumerate()
                        .filter(|(_, p)| p.habitable && p.host_planet_index.is_none())
                        .max_by_key(|(_, p)| {
                            // Prefer ELW, then habitable rocky, then water worlds.
                            match p.kind {
                                PlanetKind::EarthLikeWorld => 100,
                                PlanetKind::WaterWorld => 60,
                                _ => 30,
                            }
                        });

                    let Some((body_index, planet)) = candidate else {
                        continue;
                    };

                    let colony_id = game_state.reserve_colony_id();
                    let colony_name = format!(
                        "{} Prime",
                        game_state
                            .factions
                            .get(&ai.faction_id)
                            .map(|f| f.display_name.as_str())
                            .unwrap_or("Colony")
                    );

                    let element_profile =
                        normalized_composition_profile(&planet.composition);
                    let atmosphere_profile =
                        normalized_atmosphere_profile(&planet.atmosphere);

                    return Some(GameEvent::FoundedColony {
                        at_year: game_state.current_year,
                        colony_id,
                        colony_name,
                        founder_faction: ai.faction_id.clone(),
                        system: summary.id,
                        body_index: body_index as u16,
                        habitable_site: true,
                        earth_like_world: planet.kind == PlanetKind::EarthLikeWorld,
                        system_pos: summary.pos,
                        element_resource_profile: element_profile,
                        atmosphere_resource_profile: atmosphere_profile,
                        atmosphere_pressure_atm: planet.atmosphere_pressure_atm,
                        colonists_sent: 10_000,
                        source_colony_id: None,
                    });
                }
            }
        }
    }
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// Surveys
// ─────────────────────────────────────────────────────────────────────────────

/// Queue survey scans on nearby systems for the given AI faction.
/// Sector generation is budgeted to avoid frame spikes.
fn run_survey_decisions(
    ai: &mut FactionAiState,
    game_state: &mut GameState,
    generator: &GalaxyGenerator,
    sector_size: f32,
    center: [f32; 3],
) {
    let active_scans = game_state
        .pending_survey_scans
        .iter()
        .filter(|s| s.by_faction == ai.faction_id)
        .count();
    if active_scans >= MAX_CONCURRENT_SURVEYS {
        return;
    }
    let slots = MAX_CONCURRENT_SURVEYS - active_scans;
    let mut sector_gen_budget = MAX_SECTOR_GENS_PER_CYCLE;

    // Collect colony sector coordinates for cheap proximity checks.
    let colony_sectors: HashSet<SectorCoord> = game_state
        .colonies
        .values()
        .filter(|c| c.owner_faction == ai.faction_id)
        .map(|c| {
            SectorCoord {
                x: ((c.system_pos[0] - center[0]) / sector_size).floor() as i32,
                y: ((c.system_pos[1] - center[1]) / sector_size).floor() as i32,
            }
        })
        .collect();

    if colony_sectors.is_empty() {
        return;
    }

    // First: advance already-known systems through survey stages.
    // Use sector proximity (cheap) instead of find_system_summary (expensive).
    let known_systems: Vec<(SystemId, SurveyStage)> = game_state
        .survey_records
        .values()
        .filter(|r| {
            r.stage.next().is_some()
                && game_state.pending_scan_for(r.system).is_none()
                && is_sector_near_colony_sectors(&colony_sectors, r.system.sector, SURVEY_SEARCH_RADIUS_SECTORS)
        })
        .map(|r| (r.system, r.stage))
        .take(slots * 4)
        .collect();

    let mut queued = 0;
    for (system_id, stage) in &known_systems {
        if queued >= slots || sector_gen_budget == 0 {
            break;
        }
        let Some(next_stage) = stage.next() else {
            continue;
        };
        // For the higher stages, generate detail to get planet info (costs 2 sector gens).
        let (body_count, hab_count, viable_idx) = if next_stage >= SurveyStage::PlanetarySurvey {
            if sector_gen_budget < 2 {
                break;
            }
            sector_gen_budget -= 2;
            survey_body_info(generator, *system_id)
        } else {
            (0, 0, None)
        };

        let _ = game_state.queue_survey_scan(
            *system_id,
            ai.faction_id.clone(),
            game_state.current_year,
            next_stage,
            body_count,
            hab_count,
            viable_idx,
            1.0,
        );
        queued += 1;
    }

    // Second: discover new systems from nearby sectors.
    // Use a rotating cursor to only generate a few sectors per cycle.
    if queued >= slots || sector_gen_budget == 0 {
        return;
    }

    // Build a deterministic list of unique sector coords around all colonies.
    let mut discovery_sectors: Vec<SectorCoord> = Vec::new();
    let mut seen = HashSet::new();
    // Sort colony sectors for deterministic order.
    let mut sorted_colony_sectors: Vec<SectorCoord> = colony_sectors.iter().copied().collect();
    sorted_colony_sectors.sort_by_key(|s| (s.x, s.y));

    for cs in &sorted_colony_sectors {
        for dx in -SURVEY_SEARCH_RADIUS_SECTORS..=SURVEY_SEARCH_RADIUS_SECTORS {
            for dy in -SURVEY_SEARCH_RADIUS_SECTORS..=SURVEY_SEARCH_RADIUS_SECTORS {
                let coord = SectorCoord {
                    x: cs.x + dx,
                    y: cs.y + dy,
                };
                if seen.insert(coord) {
                    discovery_sectors.push(coord);
                }
            }
        }
    }

    if discovery_sectors.is_empty() {
        return;
    }

    // Advance the cursor, wrapping around the sector list.
    let total = discovery_sectors.len();
    let start = ai.discovery_sector_cursor % total;
    let sectors_to_scan = sector_gen_budget.min(total);

    let colony_positions: Vec<[f32; 3]> = game_state
        .colonies
        .values()
        .filter(|c| c.owner_faction == ai.faction_id)
        .map(|c| c.system_pos)
        .collect();

    let mut candidates: Vec<SystemSummary> = Vec::new();
    for i in 0..sectors_to_scan {
        let idx = (start + i) % total;
        let coord = discovery_sectors[idx];
        let systems = generator.generate_sector(coord);
        for sys in systems {
            if game_state.survey_stage(sys.id) == SurveyStage::Unknown
                && game_state.pending_scan_for(sys.id).is_none()
            {
                candidates.push(sys);
            }
        }
    }
    ai.discovery_sector_cursor = (start + sectors_to_scan) % total;

    // Sort by distance to the nearest colony, closest first.
    candidates.sort_by(|a, b| {
        let da = min_distance_to_positions(&colony_positions, a.pos);
        let db = min_distance_to_positions(&colony_positions, b.pos);
        da.total_cmp(&db)
    });
    candidates.truncate(MAX_SURVEY_CANDIDATES_PER_CYCLE);

    for sys in candidates {
        if queued >= slots {
            break;
        }
        let _ = game_state.queue_survey_scan(
            sys.id,
            ai.faction_id.clone(),
            game_state.current_year,
            SurveyStage::Located,
            0,
            0,
            None,
            1.0,
        );
        queued += 1;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Colonization
// ─────────────────────────────────────────────────────────────────────────────

/// Attempt to found new colonies on fully-assessed habitable systems.
/// Uses a points-based scoring system to pick the best target.
fn run_colonize_decisions(
    ai: &FactionAiState,
    game_state: &mut GameState,
    generator: &GalaxyGenerator,
) {
    // Only expand when the faction's existing colonies are generating positive income.
    let total_faction_income: i64 = game_state
        .colonies
        .values()
        .filter(|c| c.owner_faction == ai.faction_id)
        .map(|c| c.last_net_revenue_annual)
        .sum();
    if total_faction_income < MIN_FACTION_INCOME_FOR_COLONY {
        return;
    }

    let active_foundings = game_state
        .pending_colony_foundings
        .iter()
        .filter(|f| f.founder_faction == ai.faction_id)
        .count();
    if active_foundings >= MAX_CONCURRENT_COLONY_FOUNDINGS {
        return;
    }

    let colonization_range = game_state.faction_colonization_range_world(&ai.faction_id);
    let sector_size = generator.config().sector_size;
    let center = generator.config().center;

    // Pre-compute colony sectors for cheap proximity filtering.
    let colony_sectors: HashSet<SectorCoord> = game_state
        .colonies
        .values()
        .filter(|c| c.owner_faction == ai.faction_id)
        .map(|c| {
            SectorCoord {
                x: ((c.system_pos[0] - center[0]) / sector_size).floor() as i32,
                y: ((c.system_pos[1] - center[1]) / sector_size).floor() as i32,
            }
        })
        .collect();

    // Max sector distance that could still be within colonization range.
    let max_sector_dist = (colonization_range / sector_size).ceil() as i32 + 1;

    // Gather candidate system IDs (cheap pre-filter, no sector generation).
    let candidate_ids: Vec<SystemId> = game_state
        .survey_records
        .values()
        .filter_map(|record| {
            if record.stage < SurveyStage::ColonyAssessment {
                return None;
            }
            let system_id = record.system;

            if !is_sector_near_colony_sectors(&colony_sectors, system_id.sector, max_sector_dist) {
                return None;
            }
            if game_state.colonies.values().any(|c| c.system == system_id) {
                return None;
            }
            if game_state
                .pending_colony_foundings
                .iter()
                .any(|f| f.system == system_id)
            {
                return None;
            }
            Some(system_id)
        })
        .take(20)
        .collect();

    if candidate_ids.is_empty() {
        return;
    }

    // Score each candidate by generating its detail (budgeted: one sector gen each).
    // Only evaluate up to MAX_COLONIZE_EVALUATIONS to cap cost.
    const MAX_COLONIZE_EVALUATIONS: usize = 6;

    let mut best_score = f32::NEG_INFINITY;
    let mut best_pick: Option<ColonizationPick> = None;

    for system_id in candidate_ids.into_iter().take(MAX_COLONIZE_EVALUATIONS) {
        let Some(summary) = generator.find_system_summary(system_id) else {
            continue;
        };
        let detail = generator.generate_system_detail(&summary);
        let (score, body_index) = score_system_for_colonization(&detail);
        if score > best_score {
            best_score = score;
            best_pick = Some(ColonizationPick {
                system_id,
                summary,
                detail,
                body_index,
            });
        }
    }

    let Some(pick) = best_pick else {
        return;
    };

    let planet = pick.detail.planets.get(pick.body_index as usize);
    let habitable = planet.map_or(false, |p| p.habitable);
    let earth_like = planet.map_or(false, |p| p.kind == PlanetKind::EarthLikeWorld);

    let element_profile = planet
        .map(|p| normalized_composition_profile(&p.composition))
        .unwrap_or_default();
    let atmosphere_profile = planet
        .map(|p| normalized_atmosphere_profile(&p.atmosphere))
        .unwrap_or_default();
    let atmosphere_pressure = planet.map_or(0.0, |p| p.atmosphere_pressure_atm);

    // Pick a source colony: the largest population colony of this faction.
    let source_colony = game_state
        .colonies
        .values()
        .filter(|c| {
            c.owner_faction == ai.faction_id
                && c.population >= MIN_SOURCE_POPULATION_FOR_COLONY
        })
        .max_by(|a, b| a.population.total_cmp(&b.population));

    let source_colony_id = source_colony.map(|c| c.id);

    let colony_id = game_state.reserve_colony_id();
    let colony_name = format!(
        "{}-{}",
        game_state
            .factions
            .get(&ai.faction_id)
            .map(|f| f.display_name.as_str())
            .unwrap_or("AI"),
        colony_id
    );

    let pending = PendingColonyFounding {
        colony_id,
        colony_name,
        founder_faction: ai.faction_id.clone(),
        system: pick.system_id,
        body_index: pick.body_index,
        habitable_site: habitable,
        earth_like_world: earth_like,
        system_pos: pick.summary.pos,
        element_resource_profile: element_profile,
        atmosphere_resource_profile: atmosphere_profile,
        atmosphere_pressure_atm: atmosphere_pressure,
        source_colony_id,
        colonists_sent: COLONISTS_SENT,
        start_year: game_state.current_year,
        complete_year: game_state.current_year + COLONY_FOUNDING_DURATION_YEARS,
    };

    let _ = game_state.queue_colony_founding(game_state.current_year, pending);
}

struct ColonizationPick {
    system_id: SystemId,
    summary: SystemSummary,
    detail: crate::procedural_galaxy::SystemDetail,
    body_index: u16,
}

/// Score a star system for colonization potential and return (score, best_body_index).
/// Higher scores are more desirable.
fn score_system_for_colonization(
    detail: &crate::procedural_galaxy::SystemDetail,
) -> (f32, u16) {
    let mut system_score: f32 = 0.0;
    let mut best_body_score: f32 = f32::NEG_INFINITY;
    let mut best_body_index: u16 = 0;

    // ── Star bonuses ──
    for star in &detail.stars {
        if star.class.spectral.is_scoopable() {
            system_score += 15.0; // Scoopable stars provide fuel infrastructure.
        }
        // Brighter stars tend to have wider habitable zones.
        system_score += (star.luminosity_solar.ln() * 3.0).clamp(-5.0, 10.0);
    }

    // ── Planet scoring ──
    for (idx, planet) in detail.planets.iter().enumerate() {
        // Skip moons for primary colony target (they contribute system score only).
        let is_moon = planet.host_planet_index.is_some();

        let mut body_score: f32 = 0.0;

        // Planet type bonus.
        match planet.kind {
            PlanetKind::EarthLikeWorld => body_score += 200.0,
            PlanetKind::WaterWorld => body_score += 80.0,
            PlanetKind::AmmoniaWorld => body_score += 25.0,
            PlanetKind::Rocky | PlanetKind::MetalRich | PlanetKind::Metal => {
                body_score += if planet.habitable { 50.0 } else { 5.0 };
            }
            PlanetKind::RockyIceWorld | PlanetKind::Icy => {
                body_score += if planet.habitable { 30.0 } else { 2.0 };
            }
            // Gas giants: not colonizable surfaces but contribute system value.
            _ => body_score += 1.0,
        }

        // Habitability is the single most important factor.
        if planet.habitable {
            body_score += 100.0;
        }

        // Size bonus: larger habitable worlds support bigger populations.
        // Earth-radius = 1.0; scale diminishing returns.
        if planet.habitable {
            let size_factor = planet.radius_earth.clamp(0.3, 4.0);
            body_score += size_factor * 25.0;
        }

        // Temperature: prefer temperate range (250-310K), penalty for extremes.
        if planet.habitable {
            let temp_ideal = 280.0f32;
            let temp_deviation = (planet.temperature_k - temp_ideal).abs();
            body_score += (30.0 - temp_deviation * 0.5).max(-10.0);
        }

        // Atmosphere: presence with reasonable pressure is good for habitable worlds.
        if planet.habitable && planet.atmosphere_pressure_atm > 0.1 {
            let pressure_score = if (0.5..=2.0).contains(&planet.atmosphere_pressure_atm) {
                20.0 // Earth-like pressure, ideal
            } else if planet.atmosphere_pressure_atm < 0.5 {
                8.0 // Thin but present
            } else {
                5.0 // Dense, harder to work with
            };
            body_score += pressure_score;
        }

        // Composition richness: more diverse elements = better resource base.
        let element_diversity = planet.composition.len() as f32;
        body_score += (element_diversity * 2.0).min(16.0);

        // Add body contribution to system total (all bodies count).
        system_score += body_score * if is_moon { 0.3 } else { 1.0 };

        // Track best colonizable body (non-moon, habitable).
        if !is_moon && planet.habitable && body_score > best_body_score {
            best_body_score = body_score;
            best_body_index = idx as u16;
        }
    }

    // Planet count: more bodies means more potential resources.
    let planet_count = detail.planets.len() as f32;
    system_score += (planet_count * 2.0).min(20.0);

    (system_score, best_body_index)
}

// ─────────────────────────────────────────────────────────────────────────────
// Construction
// ─────────────────────────────────────────────────────────────────────────────

/// Queue buildings for AI-owned colonies that lack active construction.
fn run_build_decisions(ai: &FactionAiState, game_state: &mut GameState) {
    let colony_ids: Vec<u64> = game_state
        .colonies
        .values()
        .filter(|c| c.owner_faction == ai.faction_id)
        .map(|c| c.id)
        .collect();

    for colony_id in colony_ids {
        // Re-read treasury each iteration so earlier builds are accounted for.
        let treasury = game_state
            .factions
            .get(&ai.faction_id)
            .map(|f| f.treasury)
            .unwrap_or(0);
        if treasury < MIN_TREASURY_FOR_BUILDING {
            return;
        }

        // Skip if already constructing.
        if game_state
            .pending_colony_building_for_colony(colony_id)
            .is_some()
        {
            continue;
        }

        let Some(colony) = game_state.colonies.get(&colony_id) else {
            continue;
        };

        // Determine best building to construct.
        let body_site = ColonyBuildingSite::Planet(colony.body_index);
        let site_profile = ColonyBuildingSiteProfile {
            planet_is_gas_giant: Some(false),
            planet_habitable: Some(colony.habitable_site),
            planet_building_slot_capacity: Some(8),
            planet_has_atmosphere: Some(colony.atmosphere_pressure_atm > 0.01),
            star_is_scoopable: Some(true),
        };

        let Some((kind, site)) = pick_building(colony, body_site, treasury) else {
            continue;
        };

        let profile = if matches!(site, ColonyBuildingSite::Planet(_)) {
            site_profile
        } else {
            ColonyBuildingSiteProfile::default()
        };

        let _ = game_state.queue_colony_building_with_profile(
            game_state.current_year,
            colony_id,
            kind,
            site,
            profile,
        );
    }
}

/// Check if the colony can afford a building (treasury + stockpile costs with substitution).
fn can_ai_afford_building(
    colony: &crate::game_state::ColonyState,
    kind: ColonyBuildingKind,
    site: ColonyBuildingSite,
    treasury: i64,
) -> bool {
    let current_level = colony.building_level_at_site(kind, site);
    let target_level = current_level.saturating_add(1);
    if target_level > kind.max_level() {
        return false;
    }
    let cost = GameState::colony_building_cost_preview(kind, target_level);
    if treasury < cost.treasury {
        return false;
    }
    if colony.food_stockpile + 0.0001 < cost.food
        || colony.industry_stockpile + 0.0001 < cost.industry
        || colony.energy_stockpile + 0.0001 < cost.energy
    {
        return false;
    }
    // Check element costs with substitution.
    let element_costs: Vec<_> = cost
        .element_costs
        .iter()
        .map(|(sym, amt)| (sym.as_str(), *amt))
        .collect();
    can_afford_elements_with_substitution(&colony.element_stockpiles, &element_costs)
}

/// Lightweight element affordability check using substitution groups.
fn can_afford_elements_with_substitution(
    stockpiles: &HashMap<String, f32>,
    costs: &[(&str, f32)],
) -> bool {
    use crate::game_state::ELEMENT_SUBSTITUTION_PENALTY;
    use crate::game_state::element_substitutes;

    let mut remaining: HashMap<&str, f32> = HashMap::new();
    for (sym, amt) in stockpiles.iter() {
        remaining.insert(sym.as_str(), *amt);
    }

    for &(symbol, amount) in costs {
        let available = remaining.get(symbol).copied().unwrap_or(0.0);
        if available + 0.0001 >= amount {
            *remaining.entry(symbol).or_insert(0.0) -= amount;
            continue;
        }
        let mut still_needed = amount - available.max(0.0);
        if available > 0.0 {
            *remaining.entry(symbol).or_insert(0.0) = 0.0;
        }
        for &sub in element_substitutes(symbol) {
            if still_needed <= 0.0001 {
                break;
            }
            let sub_available = remaining.get(sub).copied().unwrap_or(0.0);
            if sub_available < 0.01 {
                continue;
            }
            let sub_needed = still_needed * ELEMENT_SUBSTITUTION_PENALTY;
            let sub_used = sub_available.min(sub_needed);
            *remaining.entry(sub).or_insert(0.0) -= sub_used;
            still_needed -= sub_used / ELEMENT_SUBSTITUTION_PENALTY;
        }
        if still_needed > 0.0001 {
            return false;
        }
    }
    true
}

/// Compute total element stockpile as a simple sum of all element amounts.
fn total_element_stockpile(colony: &crate::game_state::ColonyState) -> f32 {
    colony.element_stockpiles.values().copied().sum()
}

/// Check how many elements the colony has vs. the cheapest affordable building's costs.
/// Returns true if the colony should prioritize resource extraction.
fn needs_element_production(colony: &crate::game_state::ColonyState) -> bool {
    let total = total_element_stockpile(colony);
    // If total element stockpile is below this threshold, the colony needs mining.
    // A single L1 building typically needs 15-35 total element units.
    let mining_level = colony.building_level_at_site(
        ColonyBuildingKind::DeepMantleMiningStation,
        ColonyBuildingSite::Planet(colony.body_index),
    );
    // More lenient threshold if mining exists (production is incoming).
    let threshold = if mining_level > 0 { 20.0 } else { 50.0 };
    total < threshold
}

/// Heuristic: pick the most beneficial building for a colony's current state.
/// Considers element stockpiles, affordability, and balanced growth.
/// Returns None if no building can be afforded or is worth building.
fn pick_building(
    colony: &crate::game_state::ColonyState,
    planet_site: ColonyBuildingSite,
    treasury: i64,
) -> Option<(ColonyBuildingKind, ColonyBuildingSite)> {
    let star_site = ColonyBuildingSite::Star(0);
    let has_atmosphere = colony.atmosphere_pressure_atm > 0.01;

    // Helper: check if a building is upgradeable and affordable.
    let can_build = |kind: ColonyBuildingKind, site: ColonyBuildingSite| -> bool {
        let current = colony.building_level_at_site(kind, site);
        current < kind.max_level() && can_ai_afford_building(colony, kind, site, treasury)
    };

    let mining_level = colony.building_level_at_site(ColonyBuildingKind::DeepMantleMiningStation, planet_site);
    let harvester_level = if has_atmosphere {
        colony.building_level_at_site(ColonyBuildingKind::AtmosphereHarvester, planet_site)
    } else {
        u16::MAX // Mark as unavailable
    };
    let wants_elements = needs_element_production(colony);

    // ── Priority 0: Resource extraction when element stockpiles are critically low ──
    // Without elements, no other buildings can be constructed.
    if wants_elements {
        if can_build(ColonyBuildingKind::DeepMantleMiningStation, planet_site) {
            return Some((ColonyBuildingKind::DeepMantleMiningStation, planet_site));
        }
        if has_atmosphere && can_build(ColonyBuildingKind::AtmosphereHarvester, planet_site) {
            return Some((ColonyBuildingKind::AtmosphereHarvester, planet_site));
        }
        // If we can't afford mining either, try the cheapest possible building to
        // avoid getting permanently stuck.
    }

    // ── Priority 1: Critical deficits (food, energy) ──
    if colony.food_balance < -0.02 {
        if can_build(ColonyBuildingKind::AgriDome, planet_site) {
            return Some((ColonyBuildingKind::AgriDome, planet_site));
        }
    }

    if colony.energy_balance < -0.02 {
        if can_build(ColonyBuildingKind::FuelScoopDroneSwarm, star_site) {
            return Some((ColonyBuildingKind::FuelScoopDroneSwarm, star_site));
        }
    }

    // ── Priority 2: Low stability ──
    if colony.stability < 0.55 {
        if can_build(ColonyBuildingKind::EntertainmentPlaza, planet_site) {
            return Some((ColonyBuildingKind::EntertainmentPlaza, planet_site));
        }
    }

    // ── Priority 3: Industry deficit ──
    if colony.industry_balance < -0.02 {
        if can_build(ColonyBuildingKind::IndustrialHub, planet_site) {
            return Some((ColonyBuildingKind::IndustrialHub, planet_site));
        }
    }

    // ── Priority 4: Ensure foundational buildings are built (L1 of each) ──
    let trading_level = colony.building_level_at_site(ColonyBuildingKind::TradingHub, planet_site);
    let agri_level = colony.building_level_at_site(ColonyBuildingKind::AgriDome, planet_site);
    let entertainment_level = colony.building_level_at_site(ColonyBuildingKind::EntertainmentPlaza, planet_site);
    let industrial_level = colony.building_level_at_site(ColonyBuildingKind::IndustrialHub, planet_site);

    // Mining L1 is essential — ensures element production for future buildings.
    if mining_level < 1 && can_build(ColonyBuildingKind::DeepMantleMiningStation, planet_site) {
        return Some((ColonyBuildingKind::DeepMantleMiningStation, planet_site));
    }

    // Revenue: TradingHub for income.
    if trading_level < 1 && can_build(ColonyBuildingKind::TradingHub, planet_site) {
        return Some((ColonyBuildingKind::TradingHub, planet_site));
    }

    // Food for growth.
    if agri_level < 1 && can_build(ColonyBuildingKind::AgriDome, planet_site) {
        return Some((ColonyBuildingKind::AgriDome, planet_site));
    }

    // Stability for growth.
    if entertainment_level < 1 && can_build(ColonyBuildingKind::EntertainmentPlaza, planet_site) {
        return Some((ColonyBuildingKind::EntertainmentPlaza, planet_site));
    }

    // Industry for production.
    if industrial_level < 1 && can_build(ColonyBuildingKind::IndustrialHub, planet_site) {
        return Some((ColonyBuildingKind::IndustrialHub, planet_site));
    }

    // Atmosphere harvester L1 if available.
    if has_atmosphere && harvester_level < 1 && can_build(ColonyBuildingKind::AtmosphereHarvester, planet_site) {
        return Some((ColonyBuildingKind::AtmosphereHarvester, planet_site));
    }

    // ── Priority 5: Upgrade to L2 — revenue and growth focus ──
    if trading_level < 2 && can_build(ColonyBuildingKind::TradingHub, planet_site) {
        return Some((ColonyBuildingKind::TradingHub, planet_site));
    }
    if mining_level < 2 && can_build(ColonyBuildingKind::DeepMantleMiningStation, planet_site) {
        return Some((ColonyBuildingKind::DeepMantleMiningStation, planet_site));
    }
    if agri_level < 2 && can_build(ColonyBuildingKind::AgriDome, planet_site) {
        return Some((ColonyBuildingKind::AgriDome, planet_site));
    }

    // ── Priority 6: Upgrade existing buildings to higher levels ──
    // Prefer upgrading the lowest-level building for balanced development.
    let mut upgrade_candidates: Vec<(ColonyBuildingKind, ColonyBuildingSite, u16)> = vec![
        (ColonyBuildingKind::TradingHub, planet_site, trading_level),
        (ColonyBuildingKind::AgriDome, planet_site, agri_level),
        (ColonyBuildingKind::EntertainmentPlaza, planet_site, entertainment_level),
        (ColonyBuildingKind::IndustrialHub, planet_site, industrial_level),
        (ColonyBuildingKind::DeepMantleMiningStation, planet_site, mining_level),
    ];

    if has_atmosphere {
        upgrade_candidates.push((
            ColonyBuildingKind::AtmosphereHarvester,
            planet_site,
            harvester_level,
        ));
    }

    let fuel_level = colony.building_level_at_site(
        ColonyBuildingKind::FuelScoopDroneSwarm,
        star_site,
    );
    upgrade_candidates.push((
        ColonyBuildingKind::FuelScoopDroneSwarm,
        star_site,
        fuel_level,
    ));

    // Sort by level ascending (upgrade lowest first), then filter to affordable.
    upgrade_candidates.sort_by_key(|&(_, _, level)| level);

    for (kind, site, _) in upgrade_candidates {
        if can_build(kind, site) {
            return Some((kind, site));
        }
    }

    None
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn min_distance_to_positions(positions: &[[f32; 3]], target: [f32; 3]) -> f32 {
    positions
        .iter()
        .map(|pos| {
            let dx = pos[0] - target[0];
            let dy = pos[1] - target[1];
            let dz = pos[2] - target[2];
            (dx * dx + dy * dy + dz * dz).sqrt()
        })
        .min_by(|a, b| a.total_cmp(b))
        .unwrap_or(f32::MAX)
}

fn is_sector_near_colony_sectors(
    colony_sectors: &HashSet<SectorCoord>,
    system_sector: SectorCoord,
    radius: i32,
) -> bool {
    colony_sectors.iter().any(|cs| {
        (cs.x - system_sector.x).abs() <= radius
            && (cs.y - system_sector.y).abs() <= radius
    })
}

fn survey_body_info(
    generator: &GalaxyGenerator,
    system_id: SystemId,
) -> (u16, u16, Option<u16>) {
    let Some(summary) = generator.find_system_summary(system_id) else {
        return (0, 0, None);
    };
    let detail = generator.generate_system_detail(&summary);
    let body_count = detail.planets.len() as u16;
    let mut hab_count = 0u16;
    let mut viable_idx: Option<u16> = None;
    for (idx, planet) in detail.planets.iter().enumerate() {
        if planet.habitable && planet.host_planet_index.is_none() {
            hab_count += 1;
            if viable_idx.is_none() || planet.kind == PlanetKind::EarthLikeWorld {
                viable_idx = Some(idx as u16);
            }
        }
    }
    (body_count, hab_count, viable_idx)
}

fn normalized_composition_profile(
    composition: &[crate::procedural_galaxy::PlanetElementComponent],
) -> HashMap<String, f32> {
    let total: f32 = composition.iter().map(|c| c.percent).sum::<f32>().max(0.01);
    composition
        .iter()
        .map(|c| (c.symbol.clone(), c.percent / total))
        .collect()
}

fn normalized_atmosphere_profile(
    atmosphere: &[crate::procedural_galaxy::PlanetAtmosphereComponent],
) -> HashMap<String, f32> {
    let total: f32 = atmosphere.iter().map(|a| a.percent).sum::<f32>().max(0.01);
    atmosphere
        .iter()
        .map(|a| (a.formula.clone(), a.percent / total))
        .collect()
}
