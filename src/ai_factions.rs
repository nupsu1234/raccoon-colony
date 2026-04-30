use std::collections::{HashMap, HashSet};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;

use crate::events::GameEvent;
use crate::game_state::{
    AiBuildTelemetry, ColonyBuildingKind, ColonyBuildingSite, ColonyBuildingSiteProfile, GameState,
    DiplomaticTreatyKind, MilitaryCampaignOutcome, PowerplayOperationKind,
    PendingColonyFounding, SurveyStage,
};
use crate::procedural_galaxy::{GalaxyGenerator, PlanetKind, SectorCoord, SystemId, SystemSummary};

/// How frequently (in game-years) each AI decision category fires.
const SURVEY_INTERVAL_YEARS: f32 = 2.0;
const COLONIZE_INTERVAL_YEARS: f32 = 3.0;
const BUILD_INTERVAL_YEARS: f32 = 2.0;
const POWERPLAY_INTERVAL_YEARS: f32 = 1.0;
const MILITARY_INTERVAL_YEARS: f32 = 1.4;

/// Concurrency caps per faction.
const MAX_CONCURRENT_SURVEYS: usize = 4;
const MAX_CONCURRENT_COLONY_FOUNDINGS: usize = 2;

/// How many sectors outward from each colony the AI scans for survey targets.
const SURVEY_SEARCH_RADIUS_SECTORS: i32 = 2;
/// Maximum candidate systems to evaluate per decision cycle.
const MAX_SURVEY_CANDIDATES_PER_CYCLE: usize = 4;

/// Maximum number of sector generations allowed per AI faction per decision cycle.
/// This is the primary knob to prevent frame-time spikes.
const MAX_SECTOR_GENS_PER_CYCLE: usize = 2;
/// Per-tick faction decision budgets to avoid synchronized multi-second AI bursts.
const MAX_SURVEY_FACTIONS_PER_TICK: usize = 1;
const MAX_BUILD_FACTIONS_PER_TICK: usize = 1;
const MAX_COLONIZE_FACTIONS_PER_TICK: usize = 1;
const MAX_POWERPLAY_FACTIONS_PER_TICK: usize = 4;
const FORCED_DIPLOMACY_IDLE_YEARS: f32 = 1.5;
const FORCED_POWERPLAY_IDLE_YEARS: f32 = 1.0;
const MAX_MILITARY_FACTIONS_PER_TICK: usize = 2;
const AI_MAX_CONCURRENT_MILITARY_CAMPAIGNS: usize = 2;
const AI_MAX_MILITARY_DEFICIT_TOLERANCE: i64 = 120_000;

#[derive(Clone, Copy, Debug)]
struct AiFactionTuning {
    expansion_drive: f32,
    militarism: f32,
    build_tall_bias: f32,
    exploration_bias: f32,
    powerplay_bias: f32,
}

fn tuning_for_faction(faction_id: &str) -> AiFactionTuning {
    match faction_id {
        "raccoon-flood" => AiFactionTuning {
            expansion_drive: 1.30,
            militarism: 1.05,
            build_tall_bias: 0.90,
            exploration_bias: 1.00,
            powerplay_bias: 1.00,
        },
        "drifters" => AiFactionTuning {
            expansion_drive: 0.72,
            militarism: 0.55,
            build_tall_bias: 1.35,
            exploration_bias: 0.95,
            powerplay_bias: 0.85,
        },
        "new-providence" => AiFactionTuning {
            expansion_drive: 1.00,
            militarism: 1.45,
            build_tall_bias: 0.95,
            exploration_bias: 0.85,
            powerplay_bias: 1.00,
        },
        "battle-pilgrims" => AiFactionTuning {
            expansion_drive: 1.00,
            militarism: 1.50,
            build_tall_bias: 0.92,
            exploration_bias: 0.86,
            powerplay_bias: 0.95,
        },
        "wanderers-library" => AiFactionTuning {
            expansion_drive: 0.90,
            militarism: 0.35,
            build_tall_bias: 1.00,
            exploration_bias: 1.55,
            powerplay_bias: 0.75,
        },
        "greater-armenia" => AiFactionTuning {
            expansion_drive: 1.00,
            militarism: 1.00,
            build_tall_bias: 1.00,
            exploration_bias: 1.00,
            powerplay_bias: 1.00,
        },
        "hypercapitalist-foundation" => AiFactionTuning {
            expansion_drive: 1.12,
            militarism: 0.62,
            build_tall_bias: 1.30,
            exploration_bias: 0.95,
            powerplay_bias: 1.35,
        },
        "brewer-corporation" => AiFactionTuning {
            expansion_drive: 1.25,
            militarism: 0.75,
            build_tall_bias: 0.80,
            exploration_bias: 1.3,
            powerplay_bias: 1.45,
        },
        _ => AiFactionTuning {
            expansion_drive: 1.0,
            militarism: 1.0,
            build_tall_bias: 1.0,
            exploration_bias: 1.0,
            powerplay_bias: 1.0,
        },
    }
}

/// Colony founding parameters.
const COLONY_FOUNDING_DURATION_YEARS: f32 = 1.80;
const COLONISTS_SENT: u32 = 5_000;
/// Minimum total faction net annual income required before the AI will found new colonies.
/// This prevents the AI from expanding into debt before existing colonies are profitable.
const MIN_FACTION_INCOME_FOR_COLONY: i64 = 8_500;
const MIN_TREASURY_FOR_BUILDING: i64 = 35_000;
const MIN_TREASURY_RESERVE_FOR_COLONY: i64 = 220_000;
const MIN_RESERVE_DEPTH_AFTER_BUILD: f32 = 0.10;
const MAX_SUBSTITUTION_STRESS: f32 = 0.55;
const MIN_SOURCE_POPULATION_FOR_COLONY: f64 = 2_000.0;
const BOOTSTRAP_MIN_NON_MOON_PLANETS: usize = 3;
const BOOTSTRAP_MIN_HABITABLE_PLANETS: usize = 1;

/// AI faction identifiers.
const AI_FACTION_IDS: &[&str] = &[
    "raccoon-flood",
    "brewer-corporation",
    "wanderers-library",
    "drifters",
    "new-providence",
    "hypercapitalist-foundation",
    "greater-armenia",
    "battle-pilgrims",
];

/// Soft minimum separation between faction starts as a fraction of cluster radius.
/// This avoids overlapping starts while preserving randomness (no corner patterns).
const HOME_MIN_SEPARATION_FACTOR: f32 = 0.24;

#[derive(Clone, Copy, Debug)]
pub struct AiHomeSpawnConfig {
    /// Radius of the randomized population bubble where AI homes are placed.
    pub bubble_radius_world: f32,
}

impl Default for AiHomeSpawnConfig {
    fn default() -> Self {
        Self {
            bubble_radius_world: 5_200.0,
        }
    }
}

/// Generate randomized home positions for AI factions using the galaxy seed.
fn generate_home_positions(
    generator: &GalaxyGenerator,
    spawn_config: AiHomeSpawnConfig,
) -> Vec<[f32; 3]> {
    let cfg = generator.config();
    let mut rng = StdRng::seed_from_u64(cfg.galaxy_seed.wrapping_add(0xA1_FA_C7_10));
    let galaxy_center = cfg.center;
    let z_mid = (cfg.z_min + cfg.z_max) * 0.5;
    let max_radius = spawn_config.bubble_radius_world.clamp(900.0, 30_000.0);
    let min_radius = (max_radius * 0.20).max(220.0);
    let min_separation = (max_radius * HOME_MIN_SEPARATION_FACTOR).max(520.0);
    let target_count = AI_FACTION_IDS.len();
    let bubble_center_max_radius = (cfg.playfield_radius - max_radius - cfg.sector_size * 1.5).max(0.0);
    let bubble_angle = rng.gen_range(0.0..std::f32::consts::TAU);
    let bubble_radius_t: f32 = rng.gen_range(0.0..1.0);
    let bubble_center_radius = bubble_radius_t.sqrt() * bubble_center_max_radius;
    let bubble_center = [
        galaxy_center[0] + bubble_angle.cos() * bubble_center_radius,
        galaxy_center[1] + bubble_angle.sin() * bubble_center_radius,
        z_mid,
    ];

    let sample_point = |rng: &mut StdRng| -> [f32; 3] {
        let angle: f32 = rng.gen_range(0.0..std::f32::consts::TAU);
        let radius_t: f32 = rng.gen_range(0.0..1.0);
        let radius = min_radius + radius_t.sqrt() * (max_radius - min_radius);
        [
            bubble_center[0] + angle.cos() * radius,
            bubble_center[1] + angle.sin() * radius,
            z_mid + rng.gen_range(-1800.0..1800.0),
        ]
    };

    let mut positions = Vec::with_capacity(target_count);
    for _ in 0..target_count {
        let mut candidates = Vec::with_capacity(96);
        for _ in 0..96 {
            let candidate = sample_point(&mut rng);
            let min_dist = positions
                .iter()
                .map(|prev: &[f32; 3]| {
                    let dx = prev[0] - candidate[0];
                    let dy = prev[1] - candidate[1];
                    let dz = prev[2] - candidate[2];
                    (dx * dx + dy * dy + dz * dz).sqrt()
                })
                .min_by(|a: &f32, b: &f32| a.total_cmp(b))
                .unwrap_or(f32::MAX);
            // Prefer separation, but keep stochasticity to avoid geometric patterns.
            let jitter = rng.gen_range(0.0..(min_separation * 0.35));
            let score = min_dist + jitter;
            candidates.push((score, candidate));
        }
        candidates.sort_by(|a, b| b.0.total_cmp(&a.0));
        let pick_band = candidates.len().min(6);
        let pick_idx = rng.gen_range(0..pick_band);
        let mut picked = candidates[pick_idx].1;
        if !positions.is_empty() {
            // Nudge away from too-close neighbors without forcing rigid symmetry.
            for prev in &positions {
                let dx = picked[0] - prev[0];
                let dy = picked[1] - prev[1];
                let dz = picked[2] - prev[2];
                let dist = (dx * dx + dy * dy + dz * dz).sqrt().max(0.001);
                if dist < min_separation * 0.86 {
                    let push = (min_separation * 0.86 - dist) * 0.45;
                    picked[0] += dx / dist * push;
                    picked[1] += dy / dist * push;
                    picked[2] += dz / dist * push * 0.45;
                }
            }
            // Clamp back into spawn annulus.
            let ox = picked[0] - bubble_center[0];
            let oy = picked[1] - bubble_center[1];
            let radial = (ox * ox + oy * oy).sqrt().max(0.001);
            let clamped_r = radial.clamp(min_radius, max_radius);
            picked[0] = bubble_center[0] + ox / radial * clamped_r;
            picked[1] = bubble_center[1] + oy / radial * clamped_r;
        }
        // Keep homes inside the playable galaxy radius.
        let gx = picked[0] - galaxy_center[0];
        let gy = picked[1] - galaxy_center[1];
        let g_radial = (gx * gx + gy * gy).sqrt().max(0.001);
        let max_allowed = (cfg.playfield_radius - cfg.sector_size * 0.5).max(1.0);
        if g_radial > max_allowed {
            picked[0] = galaxy_center[0] + gx / g_radial * max_allowed;
            picked[1] = galaxy_center[1] + gy / g_radial * max_allowed;
        }
        positions.push(picked);
    }
    positions
}

/// Per-faction persistent AI state.
#[derive(Clone)]
struct FactionAiState {
    faction_id: String,
    home_pos: [f32; 3],
    bootstrapped: bool,
    last_survey_year: f32,
    last_colonize_year: f32,
    last_build_year: f32,
    last_powerplay_year: f32,
    last_military_year: f32,
    /// Rotating index into the discovery sector ring, so we only generate
    /// a few sectors per cycle instead of the full grid at once.
    discovery_sector_cursor: usize,
}

#[derive(Clone)]
pub struct AiFactionController {
    factions: Vec<FactionAiState>,
    survey_cursor: usize,
    colonize_cursor: usize,
    build_cursor: usize,
    powerplay_cursor: usize,
    military_cursor: usize,
}

impl AiFactionController {
    pub fn new_with_spawn_config(
        generator: &GalaxyGenerator,
        spawn_config: AiHomeSpawnConfig,
    ) -> Self {
        let home_positions = generate_home_positions(generator, spawn_config);
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
                    last_powerplay_year: -rng.gen_range(0.0..POWERPLAY_INTERVAL_YEARS),
                    last_military_year: -rng.gen_range(0.0..MILITARY_INTERVAL_YEARS),
                    discovery_sector_cursor: 0,
                }
            })
            .collect();
        Self {
            factions,
            survey_cursor: 0,
            colonize_cursor: 0,
            build_cursor: 0,
            powerplay_cursor: 0,
            military_cursor: 0,
        }
    }

    pub fn debug_home_positions(&self) -> Vec<(String, [f32; 3])> {
        self.factions
            .iter()
            .map(|ai| (ai.faction_id.clone(), ai.home_pos))
            .collect()
    }

    /// Run one AI decision pass. Call once per strategic tick from the main loop.
    /// Returns any events that should be applied immediately (e.g. bootstrap colonies).
    pub fn tick(
        &mut self,
        game_state: &mut GameState,
        generator: &GalaxyGenerator,
    ) -> Vec<GameEvent> {
        fn pick_due_indices(
            due: &[bool],
            cursor: &mut usize,
            max_per_tick: usize,
        ) -> HashSet<usize> {
            let len = due.len();
            if len == 0 || max_per_tick == 0 {
                return HashSet::new();
            }
            let mut picked = HashSet::new();
            let start = (*cursor).min(len.saturating_sub(1));
            let mut scanned = 0usize;
            let mut idx = start;
            while scanned < len && picked.len() < max_per_tick {
                if due[idx] {
                    picked.insert(idx);
                }
                idx = (idx + 1) % len;
                scanned += 1;
            }
            *cursor = idx;
            picked
        }

        #[derive(Clone, Copy)]
        struct FactionDecisionPlan {
            run_survey: bool,
            run_colonize: bool,
            run_build: bool,
            run_powerplay: bool,
            run_military: bool,
        }
        let mut events = Vec::new();
        let mut reserved_bootstrap_systems: HashSet<SystemId> = game_state
            .colonies
            .values()
            .map(|colony| colony.system)
            .collect();
        reserved_bootstrap_systems.extend(
            game_state
                .pending_colony_foundings
                .iter()
                .map(|founding| founding.system),
        );
        let current_year = game_state.current_year;
        let sector_size = generator.config().sector_size;
        let center = generator.config().center;
        let decision_plans: Vec<FactionDecisionPlan> = self
            .factions
            .par_iter()
            .map(|ai| FactionDecisionPlan {
                run_survey: current_year - ai.last_survey_year >= SURVEY_INTERVAL_YEARS,
                run_colonize: current_year - ai.last_colonize_year >= COLONIZE_INTERVAL_YEARS,
                run_build: current_year - ai.last_build_year >= BUILD_INTERVAL_YEARS,
                run_powerplay: current_year - ai.last_powerplay_year >= POWERPLAY_INTERVAL_YEARS,
                run_military: current_year - ai.last_military_year >= MILITARY_INTERVAL_YEARS,
            })
            .collect();
        let survey_due = decision_plans
            .iter()
            .map(|p| p.run_survey)
            .collect::<Vec<_>>();
        let colonize_due = decision_plans
            .iter()
            .map(|p| p.run_colonize)
            .collect::<Vec<_>>();
        let build_due = decision_plans
            .iter()
            .map(|p| p.run_build)
            .collect::<Vec<_>>();
        let powerplay_due = decision_plans
            .iter()
            .map(|p| p.run_powerplay)
            .collect::<Vec<_>>();
        let military_due = decision_plans
            .iter()
            .map(|p| p.run_military)
            .collect::<Vec<_>>();
        let survey_selected = pick_due_indices(
            &survey_due,
            &mut self.survey_cursor,
            MAX_SURVEY_FACTIONS_PER_TICK,
        );
        let colonize_selected = pick_due_indices(
            &colonize_due,
            &mut self.colonize_cursor,
            MAX_COLONIZE_FACTIONS_PER_TICK,
        );
        let build_selected = pick_due_indices(
            &build_due,
            &mut self.build_cursor,
            MAX_BUILD_FACTIONS_PER_TICK,
        );
        let powerplay_selected = pick_due_indices(
            &powerplay_due,
            &mut self.powerplay_cursor,
            MAX_POWERPLAY_FACTIONS_PER_TICK,
        );
        let military_selected = pick_due_indices(
            &military_due,
            &mut self.military_cursor,
            MAX_MILITARY_FACTIONS_PER_TICK,
        );

        for (idx, ai) in self.factions.iter_mut().enumerate() {
            let plan = decision_plans
                .get(idx)
                .copied()
                .unwrap_or(FactionDecisionPlan {
                    run_survey: false,
                    run_colonize: false,
                    run_build: false,
                    run_powerplay: false,
                    run_military: false,
                });
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
                    if let Some(event) = bootstrap_colony(
                        ai,
                        game_state,
                        generator,
                        &mut reserved_bootstrap_systems,
                    ) {
                        events.push(event);
                    }
                }
                ai.bootstrapped = true;
            }

            // ── Survey: discover and scan systems near colonies ──
            if plan.run_survey && survey_selected.contains(&idx) {
                ai.last_survey_year = current_year;
                run_survey_decisions(ai, game_state, generator, sector_size, center);
            }

            // ── Colonize: found new colonies on assessed systems ──
            if plan.run_colonize && colonize_selected.contains(&idx) {
                ai.last_colonize_year = current_year;
                run_colonize_decisions(ai, game_state, generator);
            }

            // ── Build: queue buildings at existing colonies ──
            if plan.run_build && build_selected.contains(&idx) {
                ai.last_build_year = current_year;
                run_build_decisions(ai, game_state, generator);
            }
            if plan.run_powerplay && powerplay_selected.contains(&idx) {
                ai.last_powerplay_year = current_year;
                events.extend(run_powerplay_decisions(ai, game_state));
            }
            if plan.run_military && military_selected.contains(&idx) {
                ai.last_military_year = current_year;
                events.extend(run_military_decisions(ai, game_state));
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
    reserved_systems: &mut HashSet<SystemId>,
) -> Option<GameEvent> {
    const MAX_BOOTSTRAP_EVALUATIONS: usize = 180;
    #[derive(Clone)]
    struct BootstrapPick {
        score: f32,
        summary: SystemSummary,
        body_index: usize,
        earth_like_world: bool,
        atmosphere_pressure_atm: f32,
        element_resource_profile: HashMap<String, f32>,
        atmosphere_resource_profile: HashMap<String, f32>,
    }

    let cfg = generator.config();
    let sx = ((ai.home_pos[0] - cfg.center[0]) / cfg.sector_size).floor() as i32;
    let sy = ((ai.home_pos[1] - cfg.center[1]) / cfg.sector_size).floor() as i32;

    let mut strict_best: Option<BootstrapPick> = None;
    let mut relaxed_best: Option<BootstrapPick> = None;
    let mut candidate_summaries: Vec<SystemSummary> = Vec::new();
    let mut evaluated = 0usize;
    // Search expanding rings of sectors around home_pos.
    'search: for radius in 0i32..=6 {
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
                for summary in systems {
                    if evaluated >= MAX_BOOTSTRAP_EVALUATIONS {
                        break 'search;
                    }
                    // Skip systems already colonized by anyone.
                    if reserved_systems.contains(&summary.id) {
                        continue;
                    }
                    candidate_summaries.push(summary);
                    evaluated += 1;
                }
            }
        }
    }
    let evaluated_picks: Vec<(BootstrapPick, bool)> = candidate_summaries
        .into_par_iter()
        .filter_map(|summary| {
            let detail = generator.generate_system_detail(&summary);
            let (best_body_idx, score, habitable_count, non_moon_count) =
                score_bootstrap_system(&detail)?;
            let dx = ai.home_pos[0] - summary.pos[0];
            let dy = ai.home_pos[1] - summary.pos[1];
            let dz = ai.home_pos[2] - summary.pos[2];
            let distance_penalty = (dx * dx + dy * dy + dz * dz).sqrt() / 20_000.0;
            let adjusted_score = score - distance_penalty;
            let planet = &detail.planets[best_body_idx];
            let pick = BootstrapPick {
                score: adjusted_score,
                summary,
                body_index: best_body_idx,
                earth_like_world: planet.kind == PlanetKind::EarthLikeWorld,
                atmosphere_pressure_atm: planet.atmosphere_pressure_atm,
                element_resource_profile: normalized_composition_profile(&planet.composition),
                atmosphere_resource_profile: normalized_atmosphere_profile(&planet.atmosphere),
            };
            let is_strict = habitable_count >= BOOTSTRAP_MIN_HABITABLE_PLANETS
                && non_moon_count >= BOOTSTRAP_MIN_NON_MOON_PLANETS;
            Some((pick, is_strict))
        })
        .collect();
    for (pick, is_strict) in evaluated_picks {
        if is_strict {
            if strict_best
                .as_ref()
                .map(|best| pick.score > best.score)
                .unwrap_or(true)
            {
                strict_best = Some(pick);
            }
        } else if relaxed_best
            .as_ref()
            .map(|best| pick.score > best.score)
            .unwrap_or(true)
        {
            relaxed_best = Some(pick);
        }
    }
    let picked = strict_best.or(relaxed_best);
    let Some(pick) = picked else {
        return None;
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
    reserved_systems.insert(pick.summary.id);
    Some(GameEvent::FoundedColony {
        at_year: game_state.current_year,
        colony_id,
        colony_name,
        founder_faction: ai.faction_id.clone(),
        system: pick.summary.id,
        body_index: pick.body_index as u16,
        habitable_site: true,
        earth_like_world: pick.earth_like_world,
        system_pos: pick.summary.pos,
        element_resource_profile: pick.element_resource_profile,
        atmosphere_resource_profile: pick.atmosphere_resource_profile,
        atmosphere_pressure_atm: pick.atmosphere_pressure_atm,
        colonists_sent: 10_000,
        source_colony_id: None,
    })
}

fn score_bootstrap_system(
    detail: &crate::procedural_galaxy::SystemDetail,
) -> Option<(usize, f32, usize, usize)> {
    let mut best_idx: Option<usize> = None;
    let mut best_score = f32::NEG_INFINITY;
    let mut habitable_count = 0usize;
    let mut non_moon_count = 0usize;
    for (idx, planet) in detail.planets.iter().enumerate() {
        if planet.host_planet_index.is_some() {
            continue;
        }
        non_moon_count += 1;
        if !planet.habitable {
            continue;
        }
        habitable_count += 1;
        let kind_score = match planet.kind {
            PlanetKind::EarthLikeWorld => 120.0,
            PlanetKind::WaterWorld => 78.0,
            PlanetKind::CrystalWorld => 66.0,
            PlanetKind::ChthonianWorld => 22.0,
            PlanetKind::RogueWorld => 16.0,
            PlanetKind::Rocky | PlanetKind::MetalRich | PlanetKind::Metal => 54.0,
            PlanetKind::AmmoniaWorld => 36.0,
            _ => 24.0,
        };
        let atmosphere_score = if planet.atmosphere_pressure_atm > 0.1 {
            if (0.4..=2.2).contains(&planet.atmosphere_pressure_atm) {
                18.0
            } else {
                9.0
            }
        } else {
            2.0
        };
        let composition_score = (planet.composition.len() as f32 * 2.0).min(16.0);
        let body_score = kind_score
            + atmosphere_score
            + composition_score
            + (planet.radius_earth.clamp(0.4, 2.6) * 8.0);
        if body_score > best_score {
            best_score = body_score;
            best_idx = Some(idx);
        }
    }
    let best_idx = best_idx?;
    // richness bonus to favor systems that can expand after bootstrap
    let richness_bonus = (non_moon_count as f32 * 2.5).min(15.0)
        + (habitable_count as f32 * 8.0).min(24.0);
    Some((best_idx, best_score + richness_bonus, habitable_count, non_moon_count))
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
    let tuning = tuning_for_faction(&ai.faction_id);
    let mut pending_scan_systems: HashSet<SystemId> = game_state
        .pending_survey_scans
        .iter()
        .map(|scan| scan.system)
        .collect();
    let active_scans = game_state
        .pending_survey_scans
        .iter()
        .filter(|s| s.by_faction == ai.faction_id)
        .count();
    let max_concurrent_surveys =
        ((MAX_CONCURRENT_SURVEYS as f32) * tuning.exploration_bias).round().clamp(1.0, 8.0) as usize;
    if active_scans >= max_concurrent_surveys {
        return;
    }
    let slots = max_concurrent_surveys - active_scans;
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
    let survey_radius =
        ((SURVEY_SEARCH_RADIUS_SECTORS as f32) * tuning.exploration_bias).round().clamp(1.0, 5.0) as i32;
    let nearby_colony_sectors = expand_sector_neighborhood(&colony_sectors, survey_radius);
    let known_systems: Vec<(SystemId, SurveyStage)> = game_state
        .survey_records
        .values()
        .filter(|r| {
            r.stage.next().is_some()
                && !pending_scan_systems.contains(&r.system)
                && nearby_colony_sectors.contains(&r.system.sector)
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

        if game_state
            .queue_survey_scan(
            *system_id,
            ai.faction_id.clone(),
            game_state.current_year,
            next_stage,
            body_count,
            hab_count,
            viable_idx,
            1.0,
            )
            .is_ok()
        {
            pending_scan_systems.insert(*system_id);
            queued += 1;
        }
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

    let sector_batch: Vec<SectorCoord> = (0..sectors_to_scan)
        .map(|i| discovery_sectors[(start + i) % total])
        .collect();
    let mut candidates: Vec<(f32, SystemSummary)> = sector_batch
        .into_par_iter()
        .flat_map_iter(|coord| generator.generate_sector(coord))
        .filter_map(|sys| {
            if game_state.survey_stage(sys.id) == SurveyStage::Unknown
                && !pending_scan_systems.contains(&sys.id)
            {
                let distance_key = min_distance_to_positions(&colony_positions, sys.pos);
                Some((distance_key, sys))
            } else {
                None
            }
        })
        .collect();
    ai.discovery_sector_cursor = (start + sectors_to_scan) % total;

    // Sort by distance to the nearest colony, closest first.
    candidates.sort_by(|a, b| {
        a.0.total_cmp(&b.0).then_with(|| {
            (a.1.id.sector.x, a.1.id.sector.y, a.1.id.local_index)
                .cmp(&(b.1.id.sector.x, b.1.id.sector.y, b.1.id.local_index))
        })
    });
    candidates.truncate(MAX_SURVEY_CANDIDATES_PER_CYCLE);

    for (_, sys) in candidates {
        if queued >= slots {
            break;
        }
        if game_state
            .queue_survey_scan(
            sys.id,
            ai.faction_id.clone(),
            game_state.current_year,
            SurveyStage::Located,
            0,
            0,
            None,
            1.0,
            )
            .is_ok()
        {
            pending_scan_systems.insert(sys.id);
            queued += 1;
        }
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
    let tuning = tuning_for_faction(&ai.faction_id);
    let colonization_cost_mod = game_state.faction_colonization_cost_modifier(&ai.faction_id);
    let occupied_or_claimed_systems: HashSet<SystemId> = game_state
        .colonies
        .values()
        .map(|colony| colony.system)
        .chain(
            game_state
                .pending_colony_foundings
                .iter()
                .map(|founding| founding.system),
        )
        .collect();
    // Only expand when the faction's existing colonies are generating positive income.
    let total_faction_income: i64 = game_state
        .colonies
        .values()
        .filter(|c| c.owner_faction == ai.faction_id)
        .map(|c| c.last_net_revenue_annual)
        .sum();
    let min_income = (MIN_FACTION_INCOME_FOR_COLONY as f32
        * (1.15 + tuning.build_tall_bias * 0.35)
        * colonization_cost_mod
        / tuning.expansion_drive.max(0.5))
        .round() as i64;
    if total_faction_income < min_income {
        return;
    }
    let treasury = game_state
        .factions
        .get(&ai.faction_id)
        .map(|f| f.treasury)
        .unwrap_or(0);
    let min_treasury = (MIN_TREASURY_RESERVE_FOR_COLONY as f32
        * (1.10 + tuning.build_tall_bias * 0.40)
        * colonization_cost_mod
        / tuning.expansion_drive.max(0.5))
        .round() as i64;
    if treasury < min_treasury {
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
    let nearby_colony_sectors = expand_sector_neighborhood(&colony_sectors, max_sector_dist);

    // Gather candidate system IDs (cheap pre-filter, no sector generation).
    let candidate_ids: Vec<SystemId> = game_state
        .survey_records
        .values()
        .filter_map(|record| {
            if record.stage < SurveyStage::ColonyAssessment {
                return None;
            }
            let system_id = record.system;

            if !nearby_colony_sectors.contains(&system_id.sector) {
                return None;
            }
            if occupied_or_claimed_systems.contains(&system_id) {
                return None;
            }
            Some(system_id)
        })
        .take(20)
        .collect();

    if candidate_ids.is_empty() {
        return;
    }

    // Score each candidate by generating its detail (budgeted and parallelized).
    // Only evaluate up to MAX_COLONIZE_EVALUATIONS to cap cost.
    const MAX_COLONIZE_EVALUATIONS: usize = 6;
    let scored_picks: Vec<(f32, ColonizationPick)> = candidate_ids
        .into_iter()
        .take(MAX_COLONIZE_EVALUATIONS)
        .collect::<Vec<_>>()
        .into_par_iter()
        .filter_map(|system_id| {
            let summary = generator.find_system_summary(system_id)?;
            let detail = generator.generate_system_detail(&summary);
            let (score, body_index) =
                score_system_for_colonization(&ai.faction_id, game_state, system_id, &detail);
            Some((
                score,
                ColonizationPick {
                    system_id,
                    summary,
                    detail,
                    body_index,
                },
            ))
        })
        .collect();
    let best_pick = scored_picks
        .into_iter()
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, pick)| pick);

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
    faction_id: &str,
    game_state: &GameState,
    system_id: SystemId,
    detail: &crate::procedural_galaxy::SystemDetail,
) -> (f32, u16) {
    let mut system_score: f32 = 0.0;
    let mut best_body_score: f32 = f32::NEG_INFINITY;
    let mut best_body_index: u16 = 0;

    // ── Star bonuses ──
    for star in &detail.stars {
        if star.class.spectral.is_scoopable() {
            system_score += 12.0; // Scoopable stars provide fuel infrastructure.
        }
        // Brighter stars tend to have wider habitable zones.
        system_score += (star.luminosity_solar.ln() * 3.0).clamp(-5.0, 10.0);
    }
    // Favor chemically rich, younger systems for faster industrial ramp.
    system_score += (detail.metallicity - 1.0).clamp(-0.8, 1.2) * 24.0;
    system_score += ((6.0 - detail.stellar_age_gyr) / 6.0).clamp(-0.6, 1.0) * 12.0;
    system_score += match detail.population_band {
        crate::procedural_galaxy::StellarPopulationBand::ThinDisk => 8.0,
        crate::procedural_galaxy::StellarPopulationBand::Spur => 6.0,
        crate::procedural_galaxy::StellarPopulationBand::BulgeBar => 4.0,
        crate::procedural_galaxy::StellarPopulationBand::ThickDisk => 1.0,
        crate::procedural_galaxy::StellarPopulationBand::Halo => -10.0,
    };
    if detail.architecture == crate::procedural_galaxy::SystemArchitecture::BinaryClose {
        system_score -= 18.0;
    }

    // ── Planet scoring ──
    for (idx, planet) in detail.planets.iter().enumerate() {
        // Skip moons for primary colony target (they contribute system score only).
        let is_moon = planet.host_planet_index.is_some();

        let mut body_score: f32 = 0.0;

        // Planet type bonus.
        match planet.kind {
            PlanetKind::EarthLikeWorld => body_score += 110.0,
            PlanetKind::WaterWorld => body_score += 68.0,
            PlanetKind::AmmoniaWorld => body_score += 34.0,
            PlanetKind::CrystalWorld => body_score += 56.0,
            PlanetKind::ChthonianWorld => body_score += 12.0,
            PlanetKind::RogueWorld => body_score += 8.0,
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
            body_score += 70.0;
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
        body_score += (element_diversity * 2.4).min(18.0);
        let atmosphere_diversity = planet.atmosphere.len() as f32;
        body_score += (atmosphere_diversity * 1.5).min(9.0);
        let pressure = planet.atmosphere_pressure_atm;
        if (0.2..=3.0).contains(&pressure) {
            body_score += 10.0;
        }

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

    if let Some(sim) = game_state.system_sim.get(&system_id) {
        if let Some((top_faction, top_influence)) = sim
            .influence_by_faction
            .iter()
            .max_by(|a, b| a.1.total_cmp(b.1))
        {
            if top_faction != faction_id {
                let hostility = game_state.hostility_score_between(faction_id, top_faction);
                let allied = hostility < -0.30;
                if allied {
                    system_score -= (*top_influence * 65.0).clamp(0.0, 55.0);
                } else {
                    system_score += (hostility.max(0.0) * *top_influence * 42.0).clamp(0.0, 38.0);
                }
            }
        }
        let pressure = sim.econ_pressure + sim.scarcity + (1.0 - sim.security);
        system_score += (pressure * 8.0).clamp(-6.0, 9.0);
    }

    (system_score, best_body_index)
}

// ─────────────────────────────────────────────────────────────────────────────
// Construction
// ─────────────────────────────────────────────────────────────────────────────

/// Queue buildings for AI-owned colonies that lack active construction.
#[derive(Clone, Copy, Debug)]
enum BuildIntent {
    Recovery,
    Extraction,
    Throughput,
    Growth,
}

#[derive(Clone, Copy, Debug)]
enum BuildRejectReason {
    Reserve,
    SubstitutionStress,
    SiteInvalid,
}

#[derive(Debug)]
struct BuildChoice {
    kind: ColonyBuildingKind,
    site: ColonyBuildingSite,
    profile: ColonyBuildingSiteProfile,
    intent: BuildIntent,
    score: f32,
}

fn run_build_decisions(ai: &FactionAiState, game_state: &mut GameState, generator: &GalaxyGenerator) {
    let tuning = tuning_for_faction(&ai.faction_id);
    let econ_eff = game_state.faction_econ_efficiency(&ai.faction_id);
    let mut telemetry = AiBuildTelemetry::default();
    let colonies_with_pending_build: HashSet<u64> = game_state
        .pending_colony_buildings
        .iter()
        .map(|pending| pending.colony_id)
        .collect();
    let mut colony_ids: Vec<u64> = game_state
        .colonies
        .values()
        .filter(|c| c.owner_faction == ai.faction_id)
        .map(|c| c.id)
        .collect();
    colony_ids.sort_by(|a, b| {
        let a_need = game_state
            .colonies
            .get(a)
            .map(colony_need_score)
            .unwrap_or(0.0);
        let b_need = game_state
            .colonies
            .get(b)
            .map(colony_need_score)
            .unwrap_or(0.0);
        b_need.total_cmp(&a_need)
    });
    let mut reserve_depth_total = 0.0f32;
    let mut reserve_depth_count = 0usize;

    for colony_id in colony_ids {
        // Re-read treasury each iteration so earlier builds are accounted for.
        let treasury = game_state
            .factions
            .get(&ai.faction_id)
            .map(|f| f.treasury)
            .unwrap_or(0);
        let min_build_treasury = ((MIN_TREASURY_FOR_BUILDING as f32
            * (0.92 + tuning.build_tall_bias * 0.32))
            / econ_eff.max(1.0))
            .round() as i64;
        if treasury < min_build_treasury {
            break;
        }

        // Skip if already constructing.
        if colonies_with_pending_build.contains(&colony_id) {
            continue;
        }

        let Some(colony) = game_state.colonies.get(&colony_id) else {
            continue;
        };

        reserve_depth_total += colony_reserve_depth(colony);
        reserve_depth_count += 1;
        let Some(choice) = select_build_choice(ai, colony, treasury, game_state, generator, &mut telemetry) else {
            continue;
        };

        let _ = game_state.queue_colony_building_with_profile(
            game_state.current_year,
            colony_id,
            choice.kind,
            choice.site,
            choice.profile,
        );
        match choice.intent {
            BuildIntent::Recovery => telemetry.intent_recovery = telemetry.intent_recovery.saturating_add(1),
            BuildIntent::Extraction => telemetry.intent_extraction = telemetry.intent_extraction.saturating_add(1),
            BuildIntent::Throughput => telemetry.intent_throughput = telemetry.intent_throughput.saturating_add(1),
            BuildIntent::Growth => telemetry.intent_growth = telemetry.intent_growth.saturating_add(1),
        }
    }
    telemetry.avg_reserve_depth = if reserve_depth_count > 0 {
        reserve_depth_total / reserve_depth_count as f32
    } else {
        0.0
    };
    game_state.ai_build_telemetry = telemetry;
}

fn colony_need_score(colony: &crate::game_state::ColonyState) -> f32 {
    let reserve_penalty = (0.45 - colony_reserve_depth(colony)).max(0.0) * 2.0;
    let deficit = (-colony.food_balance).max(0.0)
        + (-colony.industry_balance).max(0.0)
        + (-colony.energy_balance).max(0.0);
    let economic_penalty = if colony.last_net_revenue_annual < 0 { 0.9 } else { 0.0 };
    reserve_penalty + deficit * 1.6 + economic_penalty
}

fn colony_reserve_depth(colony: &crate::game_state::ColonyState) -> f32 {
    let capacity = colony.stockpile_capacity.max(1.0);
    let food = (colony.food_stockpile / capacity).clamp(0.0, 1.0);
    let industry = (colony.industry_stockpile / capacity).clamp(0.0, 1.0);
    let energy = (colony.energy_stockpile / capacity).clamp(0.0, 1.0);
    food.min(industry).min(energy)
}

fn select_build_choice(
    ai: &FactionAiState,
    colony: &crate::game_state::ColonyState,
    treasury: i64,
    game_state: &GameState,
    generator: &GalaxyGenerator,
    telemetry: &mut AiBuildTelemetry,
) -> Option<BuildChoice> {
    let site_candidates = candidate_sites_for_colony(colony, generator);
    let is_sanctioned = game_state
        .active_sanctions
        .iter()
        .any(|((_, target), expires)| target == &ai.faction_id && *expires > game_state.current_year);
    let in_recovery = is_sanctioned
        || colony.last_net_revenue_annual < 0
        || colony_reserve_depth(colony) < 0.18
        || ((-colony.food_balance).max(0.0)
            + (-colony.industry_balance).max(0.0)
            + (-colony.energy_balance).max(0.0))
            > 0.045;

    let mut best: Option<BuildChoice> = None;
    let mut rejected_site = false;
    for (site, profile) in site_candidates {
        for kind in ColonyBuildingKind::all().into_iter().filter(|k| *k != ColonyBuildingKind::SpaceStation) {
            if !kind.is_player_queueable() {
                continue;
            }
            if let Some(reason) =
                evaluate_build_candidate(colony, kind, site, profile, treasury, in_recovery)
            {
                match reason {
                    Ok(choice) => {
                        if best.as_ref().map(|b| choice.score > b.score).unwrap_or(true) {
                            best = Some(choice);
                        }
                    }
                    Err(BuildRejectReason::Reserve) => {
                        telemetry.reject_reserve = telemetry.reject_reserve.saturating_add(1);
                    }
                    Err(BuildRejectReason::SubstitutionStress) => {
                        telemetry.reject_substitution_stress =
                            telemetry.reject_substitution_stress.saturating_add(1);
                    }
                    Err(BuildRejectReason::SiteInvalid) => {
                        rejected_site = true;
                    }
                }
            }
        }
    }
    if rejected_site {
        telemetry.reject_site_invalid = telemetry.reject_site_invalid.saturating_add(1);
    }
    best
}

fn candidate_sites_for_colony(
    colony: &crate::game_state::ColonyState,
    generator: &GalaxyGenerator,
) -> Vec<(ColonyBuildingSite, ColonyBuildingSiteProfile)> {
    let slot_capacity = match colony.stage {
        crate::game_state::ColonyStage::Outpost => 3,
        crate::game_state::ColonyStage::Settlement => 5,
        crate::game_state::ColonyStage::City => 7,
        crate::game_state::ColonyStage::CoreWorld => 9,
    };
    let mut candidates = Vec::new();
    if let Some(summary) = generator.find_system_summary(colony.system) {
        let detail = generator.generate_system_detail(&summary);
        for (idx, planet) in detail.planets.iter().enumerate().take(6) {
            if planet.host_planet_index.is_some() {
                continue;
            }
            candidates.push((
                ColonyBuildingSite::Planet(idx as u16),
                ColonyBuildingSiteProfile {
                    planet_is_gas_giant: Some(planet.kind.is_gas_giant()),
                    planet_habitable: Some(planet.habitable),
                    planet_building_slot_capacity: Some(slot_capacity),
                    planet_has_atmosphere: Some(planet.atmosphere_pressure_atm > 0.01),
                    star_is_scoopable: None,
                },
            ));
        }
        for (idx, star) in detail.stars.iter().enumerate().take(2) {
            candidates.push((
                ColonyBuildingSite::Star(idx as u16),
                ColonyBuildingSiteProfile {
                    star_is_scoopable: Some(star.class.spectral.is_scoopable()),
                    ..ColonyBuildingSiteProfile::default()
                },
            ));
        }
    }
    // Always include home body + orbital fallback.
    candidates.push((
        ColonyBuildingSite::Planet(colony.body_index),
        ColonyBuildingSiteProfile {
            planet_is_gas_giant: Some(false),
            planet_habitable: Some(colony.habitable_site),
            planet_building_slot_capacity: Some(slot_capacity),
            planet_has_atmosphere: Some(colony.atmosphere_pressure_atm > 0.01),
            star_is_scoopable: None,
        },
    ));
    candidates.push((
        ColonyBuildingSite::Orbital,
        ColonyBuildingSiteProfile::default(),
    ));
    candidates.sort_by_key(|(site, _)| *site);
    candidates.dedup_by_key(|(site, _)| *site);
    candidates
}

fn evaluate_build_candidate(
    colony: &crate::game_state::ColonyState,
    kind: ColonyBuildingKind,
    site: ColonyBuildingSite,
    site_profile: ColonyBuildingSiteProfile,
    treasury: i64,
    in_recovery: bool,
) -> Option<Result<BuildChoice, BuildRejectReason>> {
    if GameState::building_site_support_error(kind, site, site_profile).is_some() {
        return Some(Err(BuildRejectReason::SiteInvalid));
    }
    let current = colony.building_level_at_site(kind, site);
    let target = current.saturating_add(1);
    if target > kind.max_level() {
        return None;
    }
    let cost = GameState::colony_building_cost_preview(kind, target);
    if treasury < cost.treasury {
        return None;
    }
    if colony.food_stockpile + 0.0001 < cost.food
        || colony.industry_stockpile + 0.0001 < cost.industry
        || colony.energy_stockpile + 0.0001 < cost.energy
    {
        return None;
    }
    let (elements_ok, substitution_stress) =
        affordability_and_substitution_stress(&colony.element_stockpiles, &cost.element_costs);
    if !elements_ok {
        return None;
    }
    if substitution_stress > MAX_SUBSTITUTION_STRESS {
        return Some(Err(BuildRejectReason::SubstitutionStress));
    }
    let reserve_after = estimate_reserve_depth_after_build(colony, &cost);
    if reserve_after < MIN_RESERVE_DEPTH_AFTER_BUILD {
        return Some(Err(BuildRejectReason::Reserve));
    }
    let intent = classify_build_intent(kind, in_recovery);
    let mut score = score_build_candidate(colony, kind, intent, substitution_stress, reserve_after);
    let load = site_building_load(colony, site);
    if load == 0 {
        score += 0.85;
    } else {
        score -= (load as f32 * 0.38).min(1.25);
    }
    if matches!(site, ColonyBuildingSite::Planet(idx) if idx == colony.body_index) {
        score += 0.05;
    }
    Some(Ok(BuildChoice {
        kind,
        site,
        profile: site_profile,
        intent,
        score,
    }))
}

fn site_building_load(colony: &crate::game_state::ColonyState, site: ColonyBuildingSite) -> usize {
    colony
        .buildings
        .iter()
        .filter(|b| b.level > 0 && b.site == site && b.kind.consumes_site_slot())
        .count()
}

fn affordability_and_substitution_stress(
    stockpiles: &HashMap<String, f32>,
    costs: &[(String, f32)],
) -> (bool, f32) {
    use crate::game_state::{element_substitutes, element_substitution_penalty};
    let mut remaining: HashMap<&str, f32> =
        stockpiles.iter().map(|(sym, amt)| (sym.as_str(), *amt)).collect();
    let mut total_required = 0.0f32;
    let mut substituted_effective = 0.0f32;
    for (symbol, amount) in costs {
        total_required += *amount;
        let available = remaining.get(symbol.as_str()).copied().unwrap_or(0.0);
        if available + 0.0001 >= *amount {
            *remaining.entry(symbol.as_str()).or_insert(0.0) -= *amount;
            continue;
        }
        let mut still_needed = *amount - available.max(0.0);
        if available > 0.0 {
            *remaining.entry(symbol.as_str()).or_insert(0.0) = 0.0;
        }
        for sub in element_substitutes(symbol.as_str()) {
            if still_needed <= 0.0001 {
                break;
            }
            let sub_available = remaining.get(sub).copied().unwrap_or(0.0);
            if sub_available <= 0.01 {
                continue;
            }
            let penalty = element_substitution_penalty(symbol.as_str());
            let needed_sub = still_needed * penalty;
            let used = sub_available.min(needed_sub);
            *remaining.entry(sub).or_insert(0.0) -= used;
            let covered_original = used / penalty;
            substituted_effective += covered_original * penalty.min(2.0);
            still_needed -= covered_original;
        }
        if still_needed > 0.0001 {
            return (false, 1.0);
        }
    }
    let stress = if total_required > 0.001 {
        (substituted_effective / total_required).clamp(0.0, 1.5)
    } else {
        0.0
    };
    (true, stress)
}

fn estimate_reserve_depth_after_build(
    colony: &crate::game_state::ColonyState,
    cost: &crate::game_state::ColonyBuildingCostPreview,
) -> f32 {
    let capacity = colony.stockpile_capacity.max(1.0);
    let food = ((colony.food_stockpile - cost.food).max(0.0) / capacity).clamp(0.0, 1.0);
    let industry =
        ((colony.industry_stockpile - cost.industry).max(0.0) / capacity).clamp(0.0, 1.0);
    let energy = ((colony.energy_stockpile - cost.energy).max(0.0) / capacity).clamp(0.0, 1.0);
    food.min(industry).min(energy)
}

fn classify_build_intent(kind: ColonyBuildingKind, in_recovery: bool) -> BuildIntent {
    if in_recovery {
        return BuildIntent::Recovery;
    }
    match kind {
        ColonyBuildingKind::DeepMantleMiningStation
        | ColonyBuildingKind::AtmosphereHarvester
        | ColonyBuildingKind::CatalyticRefinery
        | ColonyBuildingKind::OrePurifierComplex
        | ColonyBuildingKind::StellarIsotopeCondenser => BuildIntent::Extraction,
        ColonyBuildingKind::LogisticsExchange
        | ColonyBuildingKind::SystemsAdministration
        | ColonyBuildingKind::TradingHub
        | ColonyBuildingKind::IndustrialHub
        | ColonyBuildingKind::FuelScoopDroneSwarm => BuildIntent::Throughput,
        _ => BuildIntent::Growth,
    }
}

fn score_build_candidate(
    colony: &crate::game_state::ColonyState,
    kind: ColonyBuildingKind,
    intent: BuildIntent,
    substitution_stress: f32,
    reserve_after: f32,
) -> f32 {
    let mut score = 1.0 + reserve_after * 2.0 - substitution_stress * 1.8;
    match intent {
        BuildIntent::Recovery => score += 3.2,
        BuildIntent::Extraction => score += 2.6,
        BuildIntent::Throughput => score += 2.0,
        BuildIntent::Growth => score += 1.2,
    }
    if colony.food_balance < -0.02 && kind == ColonyBuildingKind::AgriDome {
        score += 2.3;
    }
    if colony.industry_balance < -0.02 && kind == ColonyBuildingKind::IndustrialHub {
        score += 2.1;
    }
    if colony.energy_balance < -0.02 && kind == ColonyBuildingKind::FuelScoopDroneSwarm {
        score += 2.1;
    }
    if total_element_stockpile(colony) < 64.0
        && matches!(
            kind,
            ColonyBuildingKind::DeepMantleMiningStation
                | ColonyBuildingKind::AtmosphereHarvester
                | ColonyBuildingKind::CatalyticRefinery
                | ColonyBuildingKind::OrePurifierComplex
                | ColonyBuildingKind::StellarIsotopeCondenser
        )
    {
        score += 2.8;
    }
    score
}

fn run_powerplay_decisions(ai: &FactionAiState, game_state: &mut GameState) -> Vec<GameEvent> {
    const MAX_POWERPLAY_EVENTS_PER_PASS: usize = 6;
    let mut events = Vec::new();
    let tuning = tuning_for_faction(&ai.faction_id);
    let powerplay_eff = game_state.faction_powerplay_efficiency(&ai.faction_id);
    let diplomacy_mod = game_state.faction_diplomacy_modifier(&ai.faction_id);
    let mut owner_by_system: HashMap<SystemId, HashMap<String, usize>> = HashMap::new();
    for colony in game_state.colonies.values() {
        *owner_by_system
            .entry(colony.system)
            .or_default()
            .entry(colony.owner_faction.clone())
            .or_insert(0) += 1;
    }
    let mut internal_candidates: Vec<(SystemId, f32)> = Vec::new();
    let mut foreign_candidates: Vec<(SystemId, String, f32, f32)> = Vec::new();
    for (system_id, sim) in &game_state.system_sim {
        let Some(owner_counts) = owner_by_system.get(system_id) else {
            continue;
        };
        let my_influence = sim
            .influence_by_faction
            .get(&ai.faction_id)
            .copied()
            .unwrap_or(0.0);
        let own_colony_count = owner_counts.get(&ai.faction_id).copied().unwrap_or(0);
        if own_colony_count > 0 {
            let total_colonies = owner_counts.values().copied().sum::<usize>().max(1) as f32;
            let owner_share = own_colony_count as f32 / total_colonies;
            let fragility = (0.78 - my_influence).max(0.0) + (0.55 - owner_share).max(0.0) * 0.8;
            if fragility > 0.05 {
                internal_candidates.push((*system_id, fragility));
            }
        } else {
            if my_influence < 0.08 {
                continue;
            }
            let Some((target_faction, target_influence)) = sim
                .influence_by_faction
                .iter()
                .filter(|(faction_id, _)| *faction_id != &ai.faction_id)
                .max_by(|a, b| a.1.total_cmp(b.1))
            else {
                continue;
            };
            let hostility = game_state.hostility_score_between(&ai.faction_id, target_faction);
            if target_influence > &0.12 {
                foreign_candidates.push((*system_id, target_faction.clone(), *target_influence, hostility));
            }
        }
    }
    internal_candidates.sort_by(|a, b| b.1.total_cmp(&a.1));
    foreign_candidates.sort_by(|a, b| {
        let score_a = a.2 + a.3.max(0.0);
        let score_b = b.2 + b.3.max(0.0);
        score_b.total_cmp(&score_a)
    });

    let treasury = game_state
        .factions
        .get(&ai.faction_id)
        .map(|f| f.treasury)
        .unwrap_or(0);
    let force_diplomacy = game_state
        .factions
        .get(&ai.faction_id)
        .map(|f| game_state.current_year - f.last_diplomacy_action_year >= FORCED_DIPLOMACY_IDLE_YEARS)
        .unwrap_or(false);
    let force_powerplay = game_state
        .factions
        .get(&ai.faction_id)
        .map(|f| game_state.current_year - f.last_powerplay_action_year >= FORCED_POWERPLAY_IDLE_YEARS)
        .unwrap_or(false);
    let treasury_gate = (15_000.0 / (tuning.powerplay_bias.max(0.5) * powerplay_eff.max(1.0)))
        .round() as i64;
    if treasury < treasury_gate && !force_powerplay {
        return events;
    }
    let mut diplomatic_trade_targets: Vec<(String, f32)> = game_state
        .factions
        .keys()
        .filter(|id| *id != &ai.faction_id)
        .map(|id| {
            let relation = game_state.relation_between(&ai.faction_id, id) as f32;
            let hostility = game_state.hostility_score_between(&ai.faction_id, id);
            let my_threat = game_state.faction_threatenedness_score(&ai.faction_id);
            let their_threat = game_state.faction_threatenedness_score(id);
            let their_dominance = game_state.faction_dominance_score(id);
            let shared_pressure = my_threat.min(their_threat) * 30.0;
            let anti_dominance = (1.0 - their_dominance).max(0.0) * 20.0;
            (
                id.clone(),
                relation - hostility * 60.0 + shared_pressure + anti_dominance + diplomacy_mod,
            )
        })
        .collect();
    diplomatic_trade_targets.sort_by(|a, b| b.1.total_cmp(&a.1));
    if let Some((target, score)) = diplomatic_trade_targets.into_iter().next() {
        let has_treaty = game_state.treaty_between(&ai.faction_id, &target).is_some();
        if !has_treaty {
            let treaty_kind = if score > 54.0 {
                DiplomaticTreatyKind::Alliance
            } else if score > 33.0 {
                DiplomaticTreatyKind::TradePact
            } else if score > 20.0 {
                DiplomaticTreatyKind::NonAggressionPact
            } else {
                DiplomaticTreatyKind::TradePact
            };
            if score > 20.0 || force_diplomacy {
                events.push(GameEvent::TreatyEstablished {
                    at_year: game_state.current_year,
                    faction_a: ai.faction_id.clone(),
                    faction_b: target,
                    treaty: treaty_kind,
                    expires_year: game_state.current_year + 4.8,
                    reason: if force_diplomacy {
                        "Forced fallback diplomacy window".to_owned()
                    } else {
                        "AI diplomatic initiative".to_owned()
                    },
                });
            }
        }
    }

    for &(system, fragility) in &internal_candidates {
        if events.len() >= MAX_POWERPLAY_EVENTS_PER_PASS {
            break;
        }
        let target_faction = ai.faction_id.clone();
        let strength = (0.03 + fragility * 0.045).clamp(0.03, 0.10);
        let reason = if force_powerplay {
            "Forced fallback powerplay window".to_owned()
        } else {
            "Internal command consolidation".to_owned()
        };
        events.push(GameEvent::PowerplayOperationResolved {
            at_year: game_state.current_year,
            actor_faction: ai.faction_id.clone(),
            target_faction,
            system,
            operation: PowerplayOperationKind::SupportAlly,
            success: true,
            strength,
            internal_operation: true,
            treasury_cost: 0,
            reason,
        });
    }

    let projected_foreign_budget_floor = if force_powerplay {
        GameState::POWERPLAY_FOREIGN_OP_MIN_TREASURY_RESERVE / 2
    } else {
        GameState::POWERPLAY_FOREIGN_OP_MIN_TREASURY_RESERVE
    };
    let mut remaining_treasury = treasury;
    for (system, target_faction, target_influence, hostility) in foreign_candidates {
        if events.len() >= MAX_POWERPLAY_EVENTS_PER_PASS {
            break;
        }
        if remaining_treasury.saturating_sub(GameState::POWERPLAY_FOREIGN_OP_COST)
            < -projected_foreign_budget_floor
        {
            break;
        }
        let hostile_threshold = 0.26 / tuning.powerplay_bias.max(0.6);
        let pressure_threshold = 0.08 / tuning.powerplay_bias.max(0.6);
        let (operation, success, strength, reason) =
            if hostility > hostile_threshold && target_influence > 0.22 {
            (
                PowerplayOperationKind::UndermineInfluence,
                true,
                (0.04 + hostility * 0.05).clamp(0.03, 0.10),
                "Rival destabilization push".to_owned(),
            )
        } else if hostility > pressure_threshold && target_influence > 0.14 {
            (
                PowerplayOperationKind::EconomicPressure,
                true,
                0.05,
                "Trade pressure campaign".to_owned(),
            )
        } else {
            continue;
        };
        events.push(GameEvent::PowerplayOperationResolved {
            at_year: game_state.current_year,
            actor_faction: ai.faction_id.clone(),
            target_faction,
            system,
            operation,
            success,
            strength,
            internal_operation: false,
            treasury_cost: GameState::POWERPLAY_FOREIGN_OP_COST,
            reason,
        });
        remaining_treasury =
            remaining_treasury.saturating_sub(GameState::POWERPLAY_FOREIGN_OP_COST);
    }

    if force_powerplay && !events.iter().any(|event| matches!(event, GameEvent::PowerplayOperationResolved { .. })) {
        if let Some((system, fragility)) = internal_candidates.first().copied() {
            events.push(GameEvent::PowerplayOperationResolved {
                at_year: game_state.current_year,
                actor_faction: ai.faction_id.clone(),
                target_faction: ai.faction_id.clone(),
                system,
                operation: PowerplayOperationKind::SupportAlly,
                success: true,
                strength: (0.05 + fragility * 0.035).clamp(0.05, 0.11),
                internal_operation: true,
                treasury_cost: 0,
                reason: "Forced fallback powerplay window".to_owned(),
            });
        }
    }

    events
}

fn run_military_decisions(ai: &FactionAiState, game_state: &mut GameState) -> Vec<GameEvent> {
    let mut events = Vec::new();
    let tuning = tuning_for_faction(&ai.faction_id);
    let military_eff = game_state.faction_military_effectiveness(&ai.faction_id);
    let Some(faction) = game_state.factions.get(&ai.faction_id) else {
        return events;
    };
    let faction_treasury = faction.treasury;
    let total_population = game_state.faction_total_population(&ai.faction_id);
    let standing_units = faction.military.standing_army_units;
    let active_campaigns = game_state.active_campaign_count_for_faction(&ai.faction_id);

    let recruit_treasury_threshold =
        (450_000.0 / (tuning.militarism.max(0.35) * military_eff.max(1.0))).round() as i64;
    let recruit_population_threshold =
        (220_000.0 / (tuning.militarism.max(0.35) * military_eff.max(1.0))) as f64;
    let projected_upkeep_after_recruit = ((standing_units + GameState::MILITARY_RECRUIT_UNITS_PER_BATCH)
        as i64)
        .saturating_mul(GameState::MILITARY_UPKEEP_TREASURY_COST_PER_UNIT);
    let deficit_floor = -AI_MAX_MILITARY_DEFICIT_TOLERANCE;
    let can_recruit = faction_treasury > recruit_treasury_threshold
        && faction_treasury.saturating_sub(projected_upkeep_after_recruit) > deficit_floor
        && total_population > recruit_population_threshold
        && active_campaigns <= AI_MAX_CONCURRENT_MILITARY_CAMPAIGNS;
    let target_army = (600.0 * tuning.militarism.max(0.35) * military_eff.max(1.0)).round() as u32;
    if standing_units < target_army && can_recruit {
        if let Some(event) = game_state.try_recruit_army_batch(&ai.faction_id) {
            events.push(event);
        }
    }
    let projected_upkeep_now =
        (standing_units as i64).saturating_mul(GameState::MILITARY_UPKEEP_TREASURY_COST_PER_UNIT);
    if active_campaigns >= AI_MAX_CONCURRENT_MILITARY_CAMPAIGNS
        || standing_units < (250.0 * tuning.militarism.max(0.35)).round() as u32
        || faction_treasury.saturating_sub(projected_upkeep_now) <= deficit_floor
    {
        return events;
    }

    let mut targets: Vec<(u64, String, f32, f32, f32)> = game_state
        .colonies
        .values()
        .filter(|c| c.owner_faction != ai.faction_id)
        .map(|colony| {
            let hostility = game_state.hostility_score_between(&ai.faction_id, &colony.owner_faction);
            let value = colony.population as f32 * 0.0000012 + (1.0 - colony.stability) * 0.5;
            let score = value + hostility.max(0.0) * 0.8 - colony.defense_balance.max(0.0) * 0.5;
            (
                colony.id,
                colony.owner_faction.clone(),
                score,
                hostility,
                colony.defense_balance,
            )
        })
        .collect();
    targets.sort_by(|a, b| b.2.total_cmp(&a.2));
    let offense_gate = 0.55 / tuning.militarism.max(0.35);
    let launch_budget = (tuning.militarism * 1.55).floor() as usize + 1;
    let max_launches = launch_budget
        .min(AI_MAX_CONCURRENT_MILITARY_CAMPAIGNS.saturating_sub(active_campaigns))
        .max(1);
    for (target_colony_id, defender, score, hostility, defense_balance) in targets.into_iter().take(4) {
        if events.len() >= max_launches {
            break;
        }
        let defensive_only = ai.faction_id == "wanderers-library";
        if defensive_only && score < 1.15 {
            continue;
        }
        if score <= offense_gate {
            continue;
        }
        let treasury_pressure = if faction_treasury < 200_000 { 1.0 } else { 0.0 };
        let outcome = choose_campaign_outcome(
            tuning.expansion_drive,
            hostility,
            defense_balance,
            treasury_pressure,
            target_colony_id,
        );
        if let Some(event) = game_state.try_start_military_campaign(
            &ai.faction_id,
            &defender,
            target_colony_id,
            outcome,
        ) {
            events.push(event);
        }
    }
    events
}

fn choose_campaign_outcome(
    expansion_drive: f32,
    hostility: f32,
    defense_balance: f32,
    treasury_pressure: f32,
    seed_hint: u64,
) -> MilitaryCampaignOutcome {
    let takeover_bias = (expansion_drive - 1.0) * 1.25
        + hostility.max(0.0) * 0.35
        - defense_balance.max(0.0) * 0.25
        - treasury_pressure * 0.55;
    let noise = (((seed_hint.wrapping_mul(1103515245).wrapping_add(12345)) % 1000) as f32 / 1000.0)
        - 0.5;
    if takeover_bias + noise * 0.18 >= 0.0 {
        MilitaryCampaignOutcome::Takeover
    } else {
        MilitaryCampaignOutcome::Sack
    }
}

/// Compute total element stockpile as a simple sum of all element amounts.
fn total_element_stockpile(colony: &crate::game_state::ColonyState) -> f32 {
    colony.element_stockpiles.values().copied().sum()
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

fn expand_sector_neighborhood(
    colony_sectors: &HashSet<SectorCoord>,
    radius: i32,
) -> HashSet<SectorCoord> {
    let mut nearby = HashSet::with_capacity(
        colony_sectors
            .len()
            .saturating_mul(((radius * 2 + 1) * (radius * 2 + 1)) as usize),
    );
    for cs in colony_sectors {
        for dx in -radius..=radius {
            for dy in -radius..=radius {
                nearby.insert(SectorCoord {
                    x: cs.x + dx,
                    y: cs.y + dy,
                });
            }
        }
    }
    nearby
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::game_state::{
        ColonyPolicy, ColonyStage, ConflictState, DiplomaticTreatyKind, SystemSimState, TaxationPolicy,
        PLAYER_FACTION_ID,
    };
    use crate::procedural_galaxy::{GalaxyGenerator, GeneratorConfig, SectorCoord, SystemId};

    fn test_colony(system: SystemId) -> crate::game_state::ColonyState {
        crate::game_state::ColonyState {
            id: 1,
            name: "Test Colony".to_owned(),
            owner_faction: PLAYER_FACTION_ID.to_owned(),
            system,
            body_index: 0,
            habitable_site: true,
            earth_like_world: false,
            system_pos: [0.0, 0.0, 0.0],
            policy: ColonyPolicy::Balanced,
            taxation_policy: TaxationPolicy::Standard,
            stage: ColonyStage::Settlement,
            population: 220_000.0,
            stability: 0.65,
            food_balance: -0.01,
            industry_balance: -0.01,
            energy_balance: -0.01,
            defense_balance: 0.0,
            stockpile_capacity: 100.0,
            food_stockpile: 60.0,
            industry_stockpile: 60.0,
            energy_stockpile: 60.0,
            element_stockpiles: [
                ("Fe".to_owned(), 100.0),
                ("Si".to_owned(), 85.0),
                ("Al".to_owned(), 65.0),
                ("Cu".to_owned(), 50.0),
                ("Ti".to_owned(), 35.0),
                ("Ni".to_owned(), 35.0),
            ]
            .into_iter()
            .collect(),
            atmosphere_stockpiles: HashMap::new(),
            element_resource_profile: HashMap::new(),
            atmosphere_resource_profile: HashMap::new(),
            atmosphere_pressure_atm: 1.0,
            buildings: Vec::new(),
            last_tax_revenue_annual: 0,
            last_upkeep_cost_annual: 0,
            last_net_revenue_annual: 2_000,
        }
    }

    fn test_system(local_index: u32) -> SystemId {
        SystemId {
            sector: SectorCoord { x: 0, y: 0 },
            local_index,
        }
    }

    #[test]
    fn reserve_gate_rejects_low_buffer_build() {
        let mut colony = test_colony(test_system(1));
        let preview = GameState::colony_building_cost_preview(ColonyBuildingKind::IndustrialHub, 1);
        colony.food_stockpile = preview.food + 0.2;
        colony.industry_stockpile = preview.industry + 0.2;
        colony.energy_stockpile = preview.energy + 0.2;
        let result = evaluate_build_candidate(
            &colony,
            ColonyBuildingKind::IndustrialHub,
            ColonyBuildingSite::Planet(0),
            ColonyBuildingSiteProfile {
                planet_is_gas_giant: Some(false),
                planet_habitable: Some(true),
                planet_building_slot_capacity: Some(6),
                planet_has_atmosphere: Some(true),
                star_is_scoopable: Some(true),
            },
            5_000_000,
            false,
        );
        assert!(
            matches!(result, Some(Err(BuildRejectReason::Reserve))),
            "unexpected candidate result: {result:?}"
        );
    }

    #[test]
    fn stressed_colony_has_higher_need_score() {
        let mut healthy = test_colony(test_system(2));
        let mut stressed = test_colony(test_system(3));
        healthy.food_balance = 0.08;
        healthy.industry_balance = 0.07;
        healthy.energy_balance = 0.06;
        healthy.food_stockpile = 88.0;
        healthy.industry_stockpile = 82.0;
        healthy.energy_stockpile = 84.0;
        stressed.food_balance = -0.18;
        stressed.industry_balance = -0.14;
        stressed.energy_balance = -0.16;
        stressed.food_stockpile = 12.0;
        stressed.industry_stockpile = 10.0;
        stressed.energy_stockpile = 11.0;
        stressed.last_net_revenue_annual = -3_000;
        assert!(colony_need_score(&stressed) > colony_need_score(&healthy));
    }

    #[test]
    fn candidate_sites_include_multiple_planets_when_available() {
        let generator = GalaxyGenerator::new(GeneratorConfig {
            galaxy_seed: 42,
            ..Default::default()
        });
        let mut picked: Option<SystemId> = None;
        'search: for x in -2..=2 {
            for y in -2..=2 {
                let coord = SectorCoord { x, y };
                let systems = generator.generate_sector(coord);
                for summary in systems {
                    let detail = generator.generate_system_detail(&summary);
                    let non_moon_planets = detail
                        .planets
                        .iter()
                        .filter(|p| p.host_planet_index.is_none())
                        .count();
                    if non_moon_planets >= 2 {
                        picked = Some(summary.id);
                        break 'search;
                    }
                }
            }
        }
        let system = picked.expect("expected to find system with multiple planets");
        let colony = test_colony(system);
        let candidates = candidate_sites_for_colony(&colony, &generator);
        let mut unique_planet_sites = candidates
            .iter()
            .filter(|(site, _)| matches!(site, ColonyBuildingSite::Planet(_)))
            .map(|(site, _)| *site)
            .collect::<Vec<_>>();
        unique_planet_sites.sort();
        unique_planet_sites.dedup();
        assert!(unique_planet_sites.len() >= 2);
    }

    #[test]
    fn expansion_drive_shapes_campaign_outcome_tendency() {
        let low = choose_campaign_outcome(0.68, 0.2, 0.25, 0.3, 1001);
        let high = choose_campaign_outcome(1.38, 0.2, 0.25, 0.0, 1001);
        assert_eq!(low, MilitaryCampaignOutcome::Sack);
        assert_eq!(high, MilitaryCampaignOutcome::Takeover);
    }

    #[test]
    fn faction_personality_biases_are_directionally_correct() {
        let drifters = tuning_for_faction("drifters");
        let raccoons = tuning_for_faction("raccoon-flood");
        let hyper = tuning_for_faction("hypercapitalist-foundation");
        let wanderers = tuning_for_faction("wanderers-library");
        assert!(drifters.build_tall_bias > raccoons.build_tall_bias);
        assert!(raccoons.expansion_drive > drifters.expansion_drive);
        assert!(hyper.powerplay_bias > 1.0);
        assert!(wanderers.militarism < 0.5);
    }

    #[test]
    fn powerplay_can_open_trade_pact_when_relations_are_strong() {
        let mut state = GameState::default();
        let ai = FactionAiState {
            faction_id: "hypercapitalist-foundation".to_owned(),
            home_pos: [0.0, 0.0, 0.0],
            bootstrapped: true,
            last_survey_year: 0.0,
            last_colonize_year: 0.0,
            last_build_year: 0.0,
            last_powerplay_year: 0.0,
            last_military_year: 0.0,
            discovery_sector_cursor: 0,
        };
        state.apply_event(&GameEvent::FactionRelationChanged {
            at_year: state.current_year,
            from_faction: "hypercapitalist-foundation".to_owned(),
            to_faction: "greater-armenia".to_owned(),
            delta: 65,
            reason: "Test setup".to_owned(),
        });
        state.system_sim.insert(
            test_system(99),
            SystemSimState {
                system: test_system(99),
                influence_by_faction: [
                    ("hypercapitalist-foundation".to_owned(), 0.35),
                    ("greater-armenia".to_owned(), 0.33),
                ]
                .into_iter()
                .collect(),
                security: 0.6,
                stability: 0.7,
                econ_pressure: 0.2,
                trade_flow: 0.8,
                scarcity: 0.1,
                conflict: ConflictState::Calm,
            },
        );
        let mut colony = test_colony(test_system(99));
        colony.id = 9900;
        colony.owner_faction = "greater-armenia".to_owned();
        state.colonies.insert(colony.id, colony);
        let events = run_powerplay_decisions(&ai, &mut state);
        assert!(events.iter().any(|e| matches!(
            e,
            GameEvent::TreatyEstablished {
                treaty: DiplomaticTreatyKind::TradePact,
                ..
            }
        )));
    }

    #[test]
    fn powerplay_prefers_internal_consolidation_on_own_colony_system() {
        let mut state = GameState::default();
        let ai = FactionAiState {
            faction_id: "hypercapitalist-foundation".to_owned(),
            home_pos: [0.0, 0.0, 0.0],
            bootstrapped: true,
            last_survey_year: 0.0,
            last_colonize_year: 0.0,
            last_build_year: 0.0,
            last_powerplay_year: 0.0,
            last_military_year: 0.0,
            discovery_sector_cursor: 0,
        };
        let system = test_system(123);
        let mut colony = test_colony(system);
        colony.id = 12300;
        colony.owner_faction = "hypercapitalist-foundation".to_owned();
        state.colonies.insert(colony.id, colony);
        state.system_sim.insert(
            system,
            SystemSimState {
                system,
                influence_by_faction: [
                    ("hypercapitalist-foundation".to_owned(), 0.22),
                    ("drifters".to_owned(), 0.52),
                ]
                .into_iter()
                .collect(),
                security: 0.45,
                stability: 0.55,
                econ_pressure: 0.3,
                trade_flow: 0.4,
                scarcity: 0.2,
                conflict: ConflictState::Tense,
            },
        );

        let events = run_powerplay_decisions(&ai, &mut state);
        assert!(events.iter().any(|event| matches!(
            event,
            GameEvent::PowerplayOperationResolved {
                internal_operation: true,
                ..
            }
        )));
    }

    #[test]
    fn forced_powerplay_window_emits_internal_operation_when_idle() {
        let mut state = GameState::default();
        let ai = FactionAiState {
            faction_id: "hypercapitalist-foundation".to_owned(),
            home_pos: [0.0, 0.0, 0.0],
            bootstrapped: true,
            last_survey_year: 0.0,
            last_colonize_year: 0.0,
            last_build_year: 0.0,
            last_powerplay_year: 0.0,
            last_military_year: 0.0,
            discovery_sector_cursor: 0,
        };
        state.current_year += 4.0;
        if let Some(faction) = state.factions.get_mut("hypercapitalist-foundation") {
            faction.last_powerplay_action_year = state.current_year - 3.0;
            faction.treasury = 2_000;
        }
        let system = test_system(124);
        let mut colony = test_colony(system);
        colony.id = 12400;
        colony.owner_faction = "hypercapitalist-foundation".to_owned();
        state.colonies.insert(colony.id, colony);
        state.system_sim.insert(
            system,
            SystemSimState {
                system,
                influence_by_faction: [
                    ("hypercapitalist-foundation".to_owned(), 0.20),
                    ("drifters".to_owned(), 0.55),
                ]
                .into_iter()
                .collect(),
                security: 0.45,
                stability: 0.50,
                econ_pressure: 0.30,
                trade_flow: 0.35,
                scarcity: 0.25,
                conflict: ConflictState::Tense,
            },
        );
        let events = run_powerplay_decisions(&ai, &mut state);
        assert!(events.iter().any(|e| matches!(
            e,
            GameEvent::PowerplayOperationResolved {
                internal_operation: true,
                reason,
                ..
            } if reason.contains("Forced fallback")
        )));
    }

    #[test]
    fn high_militarism_launches_more_campaign_attempts() {
        let mut state = GameState::default();
        let ai_id = "battle-pilgrims".to_owned();
        for i in 0..3u64 {
            let mut colony = test_colony(test_system(700 + i as u32));
            colony.id = 7000 + i;
            colony.owner_faction = "drifters".to_owned();
            colony.population = 1_200_000.0 + i as f64 * 120_000.0;
            colony.stability = 0.45;
            state.colonies.insert(colony.id, colony);
        }
        if let Some(f) = state.factions.get_mut(&ai_id) {
            f.military.standing_army_units = 1200;
            f.treasury = 1_200_000;
        }
        let ai = FactionAiState {
            faction_id: ai_id,
            home_pos: [0.0, 0.0, 0.0],
            bootstrapped: true,
            last_survey_year: 0.0,
            last_colonize_year: 0.0,
            last_build_year: 0.0,
            last_powerplay_year: 0.0,
            last_military_year: 0.0,
            discovery_sector_cursor: 0,
        };
        let events = run_military_decisions(&ai, &mut state);
        let launches = events
            .iter()
            .filter(|e| matches!(e, GameEvent::MilitaryCampaignStarted { .. }))
            .count();
        assert!(launches >= 1);
    }

}
