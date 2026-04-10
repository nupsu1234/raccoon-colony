use eframe::egui;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::f32::consts::PI;
use std::fs;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::events::GameEvent;
use crate::game_state::{
    ColonyBuildingCostPreview, ColonyBuildingKind, ColonyBuildingSite, ColonyBuildingSiteProfile,
    ColonyPolicy, GameState,
    PendingColonyFounding, SurveyStage,
    TaxationPolicy,
};
use crate::procedural_galaxy::{
    atmosphere_resource_catalog, composition_element_resource_catalog,
    DeltaStore, GalaxyGenerator, GeneratorConfig, LuminosityClass, PlanetAtmosphereComponent,
    PlanetKind,
    SectorCoord, SectorLruCache, SpectralClass, StarBody, StellarClassification,
    SystemDetail, SystemDelta, SystemId, SystemSummary,
};
use crate::sim_tick::StrategicClock;

mod events;
mod game_state;
mod gpu_stars;
mod procedural_galaxy;
mod save;
mod sim_tick;

const X_MIN: f32 = 0.0;
const X_MAX: f32 = 100_000.0;
const Y_MIN: f32 = 0.0;
const Y_MAX: f32 = 100_000.0;
const Z_MIN: f32 = 20_000.0;
const Z_MAX: f32 = 40_000.0;
const BLACK_HOLE_EXCLUSION_RADIUS: f32 = 100.0;
const BLACK_HOLE_CLICK_RADIUS_MIN_PX: f32 = 10.0;
const SAGITTARIUS_A_NAME: &str = "Sagittarius A*";
const STAR_DRAW_RADIUS_PX: f32 = 1.5;
const GAME_SAVE_PATH: &str = "galaxy_game_state.json";
const DELTA_SAVE_PATH: &str = "galaxy_deltas.json";
const MAX_SAVED_GAME_EVENTS: usize = 10_000;

const DEFAULT_GALAXY_SEED: u64 = 0xED11_E5DA_7A5E_ED01;
const TARGET_SYSTEM_COUNT: usize = 400_000_000_000;
const SECTOR_SIZE: f32 = 2_000.0;
const MAX_CACHED_SECTORS: usize = 900;
const CACHE_SECTOR_MARGIN: usize = 128;
const RENDER_BUDGET: usize = 220_000;
const RENDER_BUDGET_MIN: usize = 20_000;
const RENDER_BUDGET_MAX: usize = 2_000_000;
const MAX_PENDING_REQUESTS: usize = 512;
const MAX_REQUESTS_PER_FRAME: usize = 64;
const MAX_RESULTS_PER_FRAME: usize = 96;
const MAX_WORKER_THREADS: usize = 8;
const SYSTEM_VIEW_POINT_SOFT_LIMIT: usize = 90_000;
const SYSTEM_VIEW_POINT_HARD_LIMIT: usize = 160_000;
const SYSTEM_VIEW_READINESS_MIN: f32 = 0.98;
const SYSTEM_VIEW_MAX_MISSING_SECTORS: usize = 6;

const DRAW_DENSITY_REF_VISIBLE: f32 = 14_000.0;
const STAR_DRAW_RADIUS_MIN_PX: f32 = 0.35;
const STAR_ALPHA_MIN: f32 = 28.0;
const STAR_ALPHA_MAX: f32 = 220.0;
const REPRESENTED_BOOST_LOG2_CAP: f32 = 8.0;
const REPRESENTED_RADIUS_BOOST_SCALE: f32 = 0.07;
const REPRESENTED_ALPHA_BOOST_SCALE: f32 = 0.02;
const LOD_TRANSITIONS: [(f32, f32, &str); 49] = [
    (13.1, 14.0, "Chunk x14"),
    (14.6, 12.4, "Chunk x12.4"),
    (16.2, 11.0, "Chunk x11"),
    (18.0, 9.8, "Chunk x9.8"),
    (20.0, 8.7, "Chunk x8.7"),
    (22.2, 7.7, "Chunk x7.7"),
    (24.7, 6.9, "Chunk x6.9"),
    (27.4, 6.2, "Chunk x6.2"),
    (30.5, 5.5, "Chunk x5.5"),
    (34.0, 4.9, "Chunk x4.9"),
    (37.8, 4.4, "Chunk x4.4"),
    (42.0, 3.95, "Chunk x3.95"),
    (46.8, 3.55, "Chunk x3.55"),
    (52.0, 3.2, "Chunk x3.2"),
    (58.0, 2.88, "Chunk x2.88"),
    (64.8, 2.58, "Chunk x2.58"),
    (72.4, 2.32, "Chunk x2.32"),
    (80.8, 2.08, "Chunk x2.08"),
    (90.2, 1.87, "Chunk x1.87"),
    (100.6, 1.68, "Chunk x1.68"),
    (112.2, 1.51, "Chunk x1.51"),
    (125.2, 1.36, "Chunk x1.36"),
    (139.8, 1.22, "Chunk x1.22"),
    (156.2, 1.10, "Chunk x1.10"),
    (174.8, 0.99, "Chunk x0.99"),
    (195.8, 0.89, "Chunk x0.89"),
    (219.8, 0.80, "Chunk x0.80"),
    (247.2, 0.72, "Chunk x0.72"),
    (278.6, 0.65, "Chunk x0.65"),
    (314.8, 0.58, "Chunk x0.58"),
    (356.8, 0.52, "Chunk x0.52"),
    (406.0, 0.47, "Chunk x0.47"),
    (464.0, 0.42, "Chunk x0.42"),
    (533.0, 0.38, "Chunk x0.38"),
    (616.0, 0.34, "Chunk x0.34"),
    (717.0, 0.30, "Chunk x0.30"),
    (842.0, 0.27, "Chunk x0.27"),
    (998.0, 0.24, "Chunk x0.24"),
    (1196.0, 0.215, "Chunk x0.215"),
    (1450.0, 0.19, "Chunk x0.19"),
    (1780.0, 0.168, "Chunk x0.168"),
    (2210.0, 0.148, "Chunk x0.148"),
    (2780.0, 0.130, "Chunk x0.130"),
    (3540.0, 0.114, "Chunk x0.114"),
    (4580.0, 0.100, "Chunk x0.100"),
    (6020.0, 0.088, "Chunk x0.088"),
    (8050.0, 0.077, "Chunk x0.077"),
    (10900.0, 0.068, "Chunk x0.068"),
    (15000.0, 0.060, "Chunk x0.060"),
];
const CHUNK_POINT_MIN_BUDGET: usize = 140_000;
const CHUNK_POINT_RAMP_SECTOR_PX: f32 = 480.0;
const MAX_GENERATED_SYSTEMS_PER_SECTOR: usize = 2_048;
const MIN_GENERATED_SYSTEMS_PER_SECTOR: usize = 8;
const CHUNK_SCATTER_BLEND: f32 = 0.72;
const CLICK_THRESHOLD_PX: f32 = 15.0;
const CHUNK_SCATTER_BLEND_Z: f32 = 0.88;
const TRAVEL_TIME_SCALE_DEFAULT: f32 = 0.02;
const TRAVEL_VIEW_ZOOM_DEFAULT: f32 = 1.0;
const TRAVEL_VIEW_ZOOM_MIN: f32 = 0.25;
const TRAVEL_VIEW_ZOOM_MAX: f32 = 6.0;
const CHUNK_Z_SIZE_RATIO: f32 = 0.22;
const CHUNK_Z_SIZE_MIN: f32 = 220.0;
const CHUNK_Z_SIZE_MAX: f32 = 3_200.0;
const CHUNK_SIZE_XY_MAX_PLAYFIELD_RATIO: f32 = 0.09;
const CHUNK_RADIUS_MULTIPLIER_MAX: f32 = 2.0;
const CHUNK_INPUTS_PER_TARGET_POINT: usize = 24;
const CHUNK_INPUT_MIN_BUDGET: usize = 60_000;
const CHUNK_INPUT_MAX_BUDGET: usize = 900_000;
const CHUNK_REP_WEIGHT_GAMMA: f64 = 0.72;
const CHUNK_GRID_ROTATION_RADIANS: f32 = 0.61;
const CHARTING_RANGE_WORLD_MAX: f32 = 16_000.0;
const CHARTING_DISTANCE_TIME_MULTIPLIER_MAX: f32 = 3.8;
const CHARTING_RESOURCE_COST_BASE: i64 = 8_000;
const CHARTING_RESOURCE_COST_MULTIPLIER_MAX: f32 = 3.0;
const SURVEY_STAGE_STELLAR_COST_BASE: i64 = 6_000;
const SURVEY_STAGE_PLANETARY_COST_BASE: i64 = 10_000;
const SURVEY_STAGE_ASSESSMENT_COST_BASE: i64 = 16_000;
const COLONY_ESTABLISH_RESOURCE_COST_BASE: i64 = 75_000;
const COLONY_ESTABLISH_RESOURCE_COST_MULTIPLIER_MAX: f32 = 3.0;
const COLONY_TRANSFER_POP_MIN: u32 = 100;
const COLONY_TRANSFER_POP_MAX: u32 = 10_000;
const COLONY_TRANSFER_TIME_YEARS_BASE: f32 = 0.8;
const COLONY_TRANSFER_TIME_YEARS_MULTIPLIER_MAX: f32 = 5.0;
const COLONY_BALANCE_NUDGE_COST: i64 = 2_000;
const COLONY_BALANCE_NUDGE_FOCUS_DELTA: f32 = 0.012;
const COLONY_BALANCE_NUDGE_SIDE_DELTA: f32 = -0.004;

const PROCEDURAL_ARM_COUNT: usize = 4;
const PROCEDURAL_ARM_PITCH_PER_WORLD_UNIT: f32 = 0.00012; // Lower = looser spiral arms, higher = tighter winding
const PROCEDURAL_ARM_WIDTH_RADIANS: f32 = 0.18; // Higher = wider arms, more stars in-between arms, less empty space, but also less contrast and more uniform distribution overall
const PROCEDURAL_ARM_CONTRAST: f32 = 1.0; // Higher = more contrast between arm and inter-arm regions, more empty space, but also more clumpy distribution overall. Note that arm width and contrast interact in non-linear ways - higher contrast can make arms feel wider even if the width parameter is low, and vice versa.
const PROCEDURAL_BULGE_RADIUS: f32 = 9_500.0;
const PROCEDURAL_BASE_SECTOR_DENSITY: f32 = 140.0;
const PROCEDURAL_RADIAL_FALLOFF_EXP: f32 = 0.88; // Higher = more aggressive falloff, more empty space in outer regions

fn galaxy_center() -> [f32; 3] {
    [
        (X_MIN + X_MAX) / 2.0,
        (Y_MIN + Y_MAX) / 2.0,
        (Z_MIN + Z_MAX) / 2.0,
    ]
}

fn playfield_radius() -> f32 {
    ((X_MAX - X_MIN).min(Y_MAX - Y_MIN)) * 0.5
}

fn world_distance(a: [f32; 3], b: [f32; 3]) -> f32 {
    let dx = a[0] - b[0];
    let dy = a[1] - b[1];
    let dz = a[2] - b[2];
    (dx * dx + dy * dy + dz * dz).sqrt()
}

type SolarSystem = SystemSummary;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ChunkCoord {
    x: i32,
    y: i32,
    z: i32,
}

#[derive(Clone, Copy, Debug)]
struct RenderPoint {
    pos: [f32; 3],
    represented_systems: u32,
}

#[derive(Clone, Copy, Debug)]
struct LodTier {
    chunk_factor: Option<f32>,
    label: &'static str,
}

impl LodTier {
    fn systems() -> Self {
        Self {
            chunk_factor: None,
            label: "Systems",
        }
    }

    fn from_zoom(zoom: f32, sector_size: f32, transition_zoom_scale: f32) -> Self {
        let sector_pixels = sector_size * zoom * transition_zoom_scale.max(0.05);
        for (max_sector_px, chunk_factor, label) in LOD_TRANSITIONS {
            if sector_pixels < max_sector_px {
                return Self {
                    chunk_factor: Some(chunk_factor),
                    label,
                };
            }
        }

        Self::systems()
    }

    fn chunk_size(self, sector_size: f32) -> Option<f32> {
        self.chunk_factor.map(|factor| {
            let coarse_chunk_size = sector_size * factor;
            let max_chunk_size = playfield_radius() * CHUNK_SIZE_XY_MAX_PLAYFIELD_RATIO;
            coarse_chunk_size.min(max_chunk_size).max(1.0)
        })
    }

    fn label(self) -> &'static str {
        self.label
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ChunkAccumulator {
    count: u64,
    sum_x: f32,
    sum_y: f32,
    sum_z: f32,
}

#[derive(Clone, Copy, Debug)]
struct ChunkSummary {
    coord: ChunkCoord,
    count: u64,
    centroid: [f32; 3],
}

#[derive(Clone, Copy, Debug)]
struct CameraFocusTween {
    target_pan: egui::Vec2,
    target_zoom: f32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TravelBodySelection {
    Star(usize),
    Planet(usize),
}

struct VisibleBuildRequest {
    galaxy_seed: u64,
    signature: u64,
    required_hash: u64,
    required_ready_hash: u64,
    lod_tier: LodTier,
    center: [f32; 3],
    sector_size: f32,
    render_budget: usize,
    desired_chunk_budget: usize,
    sectors: Vec<Arc<Vec<SolarSystem>>>,
    build_chunk_uploads: bool,
}

struct VisibleBuildResult {
    signature: u64,
    required_hash: u64,
    required_ready_hash: u64,
    lod_label: &'static str,
    visible_system_count: usize,
    render_points: Vec<RenderPoint>,
    chunk_uploads: Vec<gpu_stars::ChunkUpload>,
    build_ms: f32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LodPreset {
    Compatibility,
    Balanced,
    Quality,
    Ultra,
    Max,
}

impl LodPreset {
    fn label(self) -> &'static str {
        match self {
            LodPreset::Compatibility => "Compatibility",
            LodPreset::Balanced => "Balanced",
            LodPreset::Quality => "Quality",
            LodPreset::Ultra => "Ultra",
            LodPreset::Max => "Max",
        }
    }

    fn all() -> [Self; 5] {
        [
            Self::Compatibility,
            Self::Balanced,
            Self::Quality,
            Self::Ultra,
            Self::Max,
        ]
    }
}

#[derive(Clone, Copy, Debug)]
struct LodProfile {
    render_budget: usize,
    chunk_point_min_budget: usize,
    transition_zoom_scale: f32,
    system_view_soft_limit: usize,
    system_view_hard_limit: usize,
    system_view_readiness_min: f32,
    system_view_max_missing_sectors: usize,
}

pub struct GalaxyApp {
    sector_cache: SectorLruCache,
    procedural_generator: Arc<GalaxyGenerator>,
    galaxy_seed: u64,
    render_points: Vec<RenderPoint>,
    gpu_renderer_ready: bool,
    visible_system_count: usize,
    lod_tier: LodTier,
    target_system_count: usize,
    estimated_total_systems: usize,
    sector_size: f32,
    render_budget: usize,
    request_tx: mpsc::Sender<SectorCoord>,
    result_rx: mpsc::Receiver<(SectorCoord, Vec<SolarSystem>)>,
    visible_build_request_tx: mpsc::SyncSender<VisibleBuildRequest>,
    visible_build_result_rx: mpsc::Receiver<VisibleBuildResult>,
    visible_build_inflight: bool,
    visible_build_requested_signature: u64,
    pending_sectors: HashSet<SectorCoord>,
    last_view_center: Option<egui::Vec2>,
    worker_count: usize,
    zoom: f32,
    pan: egui::Vec2,
    pan_velocity: egui::Vec2,
    camera_focus_tween: Option<CameraFocusTween>,
    camera_lock_target: Option<[f32; 3]>,
    view_fitted: bool,
    dragging: bool,
    last_mouse_pos: egui::Pos2,
    yaw: f32,
    pitch: f32,
    rotating: bool,
    last_required_hash: u64,
    last_required_ready_hash: u64,
    last_lod_label: &'static str,
    cached_chunk_uploads: std::sync::Arc<Vec<gpu_stars::ChunkUpload>>,
    chunk_uploads_generation: u64,
    cpu_visible_update_ms_last: f32,
    cpu_visible_update_ms_smooth: f32,
    cpu_rebuild_ms_last: f32,
    cpu_rebuild_ms_smooth: f32,
    rebuilt_this_frame: bool,
    selected_system: Option<SystemDetail>,
    delta_store: DeltaStore,
    game_state: GameState,
    game_events: Vec<GameEvent>,
    game_paused: bool,
    strategic_clock: StrategicClock,
    game_autosave_accum_years: f32,
    game_save_error: Option<String>,
    game_notice: Option<String>,
    settings_window_open: bool,
    settings_seed_input: String,
    settings_render_budget: usize,
    lod_preset: LodPreset,
    settings_lod_preset: LodPreset,
    lod_chunk_point_min_budget: usize,
    lod_transition_zoom_scale: f32,
    lod_system_view_soft_limit: usize,
    lod_system_view_hard_limit: usize,
    lod_system_view_readiness_min: f32,
    lod_system_view_max_missing_sectors: usize,
    reset_progress_armed: bool,
    starting_colony_selection: Option<u64>,
    colonies_window_open: bool,
    construction_window_open: bool,
    colonies_window_player_only: bool,
    colony_transfer_source: Option<u64>,
    colony_transfer_target: Option<u64>,
    colony_transfer_amount: u32,
    colony_build_site_selection: HashMap<u64, ColonyBuildingSite>,
    colony_build_panel_selection: Option<u64>,
    legend_panel_open: bool,
    debug_panel_open: bool,
    resources_panel_open: bool,
    favorites_window_open: bool,
    travel_system: Option<SystemDetail>,
    travel_window_open: bool,
    travel_paused: bool,
    travel_time_scale: f32,
    travel_view_zoom: f32,
    travel_view_pan: egui::Vec2,
    travel_view_dragging: bool,
    travel_view_last_mouse_pos: egui::Pos2,
    travel_sim_years: f32,
    travel_last_input_time: Option<f64>,
    travel_selected_body: Option<TravelBodySelection>,
    travel_composition_info_open: bool,
    travel_atmosphere_info_open: bool,
    colony_source_selection: Option<u64>,
    colony_transfer_colonists: u32,
}

impl Default for GalaxyApp {
    fn default() -> Self {
        let worker_count = GalaxyApp::worker_thread_count();
        let target_system_count = TARGET_SYSTEM_COUNT.max(1);
        let galaxy_seed = DEFAULT_GALAXY_SEED;
        let lod_preset = LodPreset::Max;
        let lod_profile = Self::lod_profile(lod_preset);
        let generator_config = Self::generator_config(galaxy_seed, target_system_count, SECTOR_SIZE);
        let procedural_generator = Arc::new(GalaxyGenerator::new(generator_config));
        let estimated_total_systems = procedural_generator
            .estimate_total_systems()
            .min(usize::MAX as u64) as usize;
        let full_view_sector_capacity =
            GalaxyApp::full_view_sector_capacity(SECTOR_SIZE).saturating_add(CACHE_SECTOR_MARGIN);
        let sector_cache_capacity = MAX_CACHED_SECTORS.max(full_view_sector_capacity);
        let (request_tx, result_rx) =
            GalaxyApp::spawn_sector_workers(worker_count, Arc::clone(&procedural_generator));
        let (visible_build_request_tx, visible_build_result_rx) =
            GalaxyApp::spawn_visible_build_worker();
        let (mut game_state, mut game_events, game_save_error) =
            match save::load_game_save(GAME_SAVE_PATH) {
                Ok((state, events)) => (state, events, None),
                Err(err) => (
                    GameState::default(),
                    Vec::new(),
                    Some(format!("Failed to load game save: {err}")),
                ),
            };
        // Reconcile colony positions with the procedural galaxy so that
        // colony markers always sit on top of their actual star after a
        // reload (the stored system_pos may drift when the generator
        // parameters change between compilations).
        // When the index-based lookup fails (sector shrank due to parameter
        // changes), fall back to a spatial search and rebind the colony to
        // the nearest system.
        let mut rebound_system_ids: Vec<(SystemId, SystemId)> = Vec::new();
        for colony in game_state.colonies.values_mut() {
            if let Some(summary) = procedural_generator.find_system_summary(colony.system) {
                colony.system_pos = summary.pos;
            } else if let Some(nearest) = procedural_generator.find_nearest_system_by_pos(
                colony.system.sector,
                colony.system_pos,
                2_000.0,
            ) {
                let old_id = colony.system;
                colony.system = nearest.id;
                colony.system_pos = nearest.pos;
                rebound_system_ids.push((old_id, nearest.id));
            }
        }
        // Patch explored_systems and survey_records so the rebound system
        // IDs stay consistent with the new colony references.
        for (old_id, new_id) in &rebound_system_ids {
            if game_state.explored_systems.remove(old_id) {
                game_state.explored_systems.insert(*new_id);
            }
            for record in &mut game_state.survey_records {
                if record.system == *old_id {
                    record.system = *new_id;
                }
            }
            if game_state.player.location == Some(*old_id) {
                game_state.player.location = Some(*new_id);
            }
            if game_state.player.home_system == Some(*old_id) {
                game_state.player.home_system = Some(*new_id);
            }
        }
        Self::trim_game_event_history(&mut game_events);
        let loaded_starting_colony = game_state.player.starting_colony_id;
        Self {
            sector_cache: SectorLruCache::new(sector_cache_capacity),
            procedural_generator,
            galaxy_seed,
            render_points: Vec::new(),
            gpu_renderer_ready: false,
            visible_system_count: 0,
            lod_tier: LodTier::systems(),
            target_system_count,
            estimated_total_systems,
            sector_size: SECTOR_SIZE,
            render_budget: lod_profile.render_budget,
            request_tx,
            result_rx,
            visible_build_request_tx,
            visible_build_result_rx,
            visible_build_inflight: false,
            visible_build_requested_signature: 0,
            pending_sectors: HashSet::new(),
            last_view_center: None,
            worker_count,
            zoom: 0.005,
            pan: egui::Vec2::ZERO,
            pan_velocity: egui::Vec2::ZERO,
            camera_focus_tween: None,
            camera_lock_target: Some(galaxy_center()),
            view_fitted: false,
            dragging: false,
            last_mouse_pos: egui::Pos2::ZERO,
            yaw: 0.0,
            pitch: 0.0,
            rotating: false,
            last_required_hash: 0,
            last_required_ready_hash: u64::MAX,
            last_lod_label: "Systems",
            cached_chunk_uploads: std::sync::Arc::new(Vec::new()),
            chunk_uploads_generation: 0,
            cpu_visible_update_ms_last: 0.0,
            cpu_visible_update_ms_smooth: 0.0,
            cpu_rebuild_ms_last: 0.0,
            cpu_rebuild_ms_smooth: 0.0,
            rebuilt_this_frame: false,
            selected_system: None,
            delta_store: DeltaStore::load_json(DELTA_SAVE_PATH).unwrap_or_default(),
            starting_colony_selection: loaded_starting_colony,
            game_state,
            game_events,
            game_paused: true,
            strategic_clock: StrategicClock::default(),
            game_autosave_accum_years: 0.0,
            game_save_error,
            game_notice: None,
            settings_window_open: false,
            settings_seed_input: galaxy_seed.to_string(),
            settings_render_budget: lod_profile.render_budget,
            lod_preset,
            settings_lod_preset: lod_preset,
            lod_chunk_point_min_budget: lod_profile.chunk_point_min_budget,
            lod_transition_zoom_scale: lod_profile.transition_zoom_scale,
            lod_system_view_soft_limit: lod_profile.system_view_soft_limit,
            lod_system_view_hard_limit: lod_profile.system_view_hard_limit,
            lod_system_view_readiness_min: lod_profile.system_view_readiness_min,
            lod_system_view_max_missing_sectors: lod_profile.system_view_max_missing_sectors,
            reset_progress_armed: false,
            colonies_window_open: false,
            construction_window_open: false,
            colonies_window_player_only: false,
            colony_transfer_source: None,
            colony_transfer_target: None,
            colony_transfer_amount: 1000,
            colony_build_site_selection: HashMap::new(),
            colony_build_panel_selection: loaded_starting_colony,
            legend_panel_open: false,
            debug_panel_open: false,
            resources_panel_open: false,
            favorites_window_open: false,
            travel_system: None,
            travel_window_open: false,
            travel_paused: false,
            travel_time_scale: TRAVEL_TIME_SCALE_DEFAULT,
            travel_view_zoom: TRAVEL_VIEW_ZOOM_DEFAULT,
            travel_view_pan: egui::Vec2::ZERO,
            travel_view_dragging: false,
            travel_view_last_mouse_pos: egui::Pos2::ZERO,
            travel_sim_years: 0.0,
            travel_last_input_time: None,
            travel_selected_body: None,
            travel_composition_info_open: false,
            travel_atmosphere_info_open: false,
            colony_source_selection: loaded_starting_colony,
            colony_transfer_colonists: COLONY_TRANSFER_POP_MIN,
        }
    }
}

impl GalaxyApp {
    fn trim_game_event_history(events: &mut Vec<GameEvent>) {
        if events.len() <= MAX_SAVED_GAME_EVENTS {
            return;
        }
        let overflow = events.len() - MAX_SAVED_GAME_EVENTS;
        events.drain(0..overflow);
    }

    fn trim_game_events(&mut self) {
        Self::trim_game_event_history(&mut self.game_events);
    }

    fn lod_profile(preset: LodPreset) -> LodProfile {
        match preset {
            LodPreset::Compatibility => LodProfile {
                render_budget: 80_000,
                chunk_point_min_budget: 45_000,
                transition_zoom_scale: 0.72,
                system_view_soft_limit: 45_000,
                system_view_hard_limit: 80_000,
                system_view_readiness_min: 0.995,
                system_view_max_missing_sectors: 3,
            },
            LodPreset::Balanced => LodProfile {
                render_budget: RENDER_BUDGET,
                chunk_point_min_budget: CHUNK_POINT_MIN_BUDGET,
                transition_zoom_scale: 1.0,
                system_view_soft_limit: SYSTEM_VIEW_POINT_SOFT_LIMIT,
                system_view_hard_limit: SYSTEM_VIEW_POINT_HARD_LIMIT,
                system_view_readiness_min: SYSTEM_VIEW_READINESS_MIN,
                system_view_max_missing_sectors: SYSTEM_VIEW_MAX_MISSING_SECTORS,
            },
            LodPreset::Quality => LodProfile {
                render_budget: 420_000,
                chunk_point_min_budget: 210_000,
                transition_zoom_scale: 1.18,
                system_view_soft_limit: 140_000,
                system_view_hard_limit: 260_000,
                system_view_readiness_min: 0.97,
                system_view_max_missing_sectors: 8,
            },
            LodPreset::Ultra => LodProfile {
                render_budget: 800_000,
                chunk_point_min_budget: 360_000,
                transition_zoom_scale: 1.35,
                system_view_soft_limit: 210_000,
                system_view_hard_limit: 420_000,
                system_view_readiness_min: 0.94,
                system_view_max_missing_sectors: 14,
            },
            LodPreset::Max => LodProfile {
                render_budget: 2_000_000,
                chunk_point_min_budget: 1_000_000,
                transition_zoom_scale: 2.0,
                system_view_soft_limit: 1_000_000,
                system_view_hard_limit: 2_000_000,
                system_view_readiness_min: 0.90,
                system_view_max_missing_sectors: 18,
            },
        }
    }

    fn apply_lod_preset(&mut self, preset: LodPreset) {
        let profile = Self::lod_profile(preset);
        self.lod_preset = preset;
        self.settings_lod_preset = preset;
        self.render_budget = profile.render_budget.clamp(RENDER_BUDGET_MIN, RENDER_BUDGET_MAX);
        self.settings_render_budget = self.render_budget;
        self.lod_chunk_point_min_budget = profile.chunk_point_min_budget;
        self.lod_transition_zoom_scale = profile.transition_zoom_scale;
        self.lod_system_view_soft_limit = profile.system_view_soft_limit;
        self.lod_system_view_hard_limit = profile.system_view_hard_limit;
        self.lod_system_view_readiness_min = profile.system_view_readiness_min;
        self.lod_system_view_max_missing_sectors = profile.system_view_max_missing_sectors;

        self.last_required_ready_hash = u64::MAX;
        self.visible_build_inflight = false;
        self.game_notice = Some(format!(
            "LOD preset set to {} (budget {}).",
            preset.label(),
            self.render_budget
        ));
    }

    fn generator_config(galaxy_seed: u64, target_system_count: usize, sector_size: f32) -> GeneratorConfig {
        let center = galaxy_center();
        GeneratorConfig {
            galaxy_seed,
            target_system_count: target_system_count as u64,
            center,
            playfield_radius: playfield_radius(),
            sector_size,
            z_min: Z_MIN,
            z_max: Z_MAX,
            arm_count: PROCEDURAL_ARM_COUNT,
            arm_pitch_per_world_unit: PROCEDURAL_ARM_PITCH_PER_WORLD_UNIT,
            arm_width_radians: PROCEDURAL_ARM_WIDTH_RADIANS,
            arm_contrast: PROCEDURAL_ARM_CONTRAST,
            bulge_radius: PROCEDURAL_BULGE_RADIUS,
            radial_falloff_exp: PROCEDURAL_RADIAL_FALLOFF_EXP,
            base_sector_density: PROCEDURAL_BASE_SECTOR_DENSITY,
            min_materialized_per_sector: MIN_GENERATED_SYSTEMS_PER_SECTOR,
            max_materialized_per_sector: MAX_GENERATED_SYSTEMS_PER_SECTOR,
            ..GeneratorConfig::default()
        }
    }

    fn parse_seed_input(seed_input: &str) -> Option<u64> {
        let trimmed = seed_input.trim();
        if trimmed.is_empty() {
            return None;
        }

        if let Some(hex) = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
        {
            u64::from_str_radix(hex, 16).ok()
        } else {
            trimmed.parse::<u64>().ok()
        }
    }

    fn rebuild_generation_runtime(&mut self) {
        let generator_config = Self::generator_config(
            self.galaxy_seed,
            self.target_system_count,
            self.sector_size,
        );
        self.procedural_generator = Arc::new(GalaxyGenerator::new(generator_config));
        self.estimated_total_systems = self
            .procedural_generator
            .estimate_total_systems()
            .min(usize::MAX as u64) as usize;

        let full_view_sector_capacity =
            GalaxyApp::full_view_sector_capacity(self.sector_size).saturating_add(CACHE_SECTOR_MARGIN);
        let sector_cache_capacity = MAX_CACHED_SECTORS.max(full_view_sector_capacity);
        self.sector_cache = SectorLruCache::new(sector_cache_capacity);

        let (request_tx, result_rx) =
            GalaxyApp::spawn_sector_workers(self.worker_count, Arc::clone(&self.procedural_generator));
        self.request_tx = request_tx;
        self.result_rx = result_rx;

        self.pending_sectors.clear();
        self.last_view_center = None;
        self.visible_build_inflight = false;
        self.visible_build_requested_signature = 0;
        self.render_points.clear();
        self.visible_system_count = 0;
        self.last_required_hash = 0;
        self.last_required_ready_hash = u64::MAX;
        self.last_lod_label = "Systems";
        self.chunk_uploads_generation = 0;
        self.cached_chunk_uploads = Arc::new(Vec::new());
        self.selected_system = None;
        self.travel_system = None;
        self.travel_window_open = false;
        self.travel_paused = false;
        self.travel_sim_years = 0.0;
        self.travel_last_input_time = None;
        self.travel_selected_body = None;
        self.reset_travel_view();
    }

    fn restart_simulation_with_seed(&mut self, galaxy_seed: u64) {
        self.galaxy_seed = galaxy_seed;
        self.settings_seed_input = galaxy_seed.to_string();
        self.reset_all_progress();
        self.rebuild_generation_runtime();

        self.pan = egui::Vec2::ZERO;
        self.pan_velocity = egui::Vec2::ZERO;
        self.camera_focus_tween = None;
        self.camera_lock_target = Some(galaxy_center());
        self.zoom = 0.005;
        self.yaw = 0.0;
        self.pitch = 0.0;
        self.view_fitted = false;
        self.game_paused = true;

        self.game_notice = Some(format!(
            "Simulation restarted with seed {galaxy_seed}. Saved progress reset.",
        ));
    }

    fn show_settings_window(&mut self, ctx: &egui::Context) {
        if !self.settings_window_open {
            return;
        }

        let mut open = self.settings_window_open;
        let mut apply_render_budget = false;
        let mut apply_lod_preset = false;
        let mut restart_seed = None;
        let mut seed_restart_requested = false;
        let mut random_restart = false;

        egui::Window::new("Settings")
            .open(&mut open)
            .resizable(true)
            .default_size([500.0, 320.0])
            .show(ctx, |ui| {
                ui.heading("Performance");
                egui::ComboBox::from_id_source("settings_lod_preset")
                    .selected_text(self.settings_lod_preset.label())
                    .show_ui(ui, |ui| {
                        for preset in LodPreset::all() {
                            ui.selectable_value(
                                &mut self.settings_lod_preset,
                                preset,
                                preset.label(),
                            );
                        }
                    });
                if ui.button("Apply LOD preset").clicked() {
                    apply_lod_preset = true;
                }
                ui.small("Presets tune chunk/system LOD thresholds and set a recommended render budget.");
                ui.small(format!(
                    "Transition scale: {:.2}x (lower = more zoom needed for finer LOD)",
                    self.lod_transition_zoom_scale
                ));

                ui.add(
                    egui::Slider::new(
                        &mut self.settings_render_budget,
                        RENDER_BUDGET_MIN..=RENDER_BUDGET_MAX,
                    )
                    .text("Max render budget")
                    .logarithmic(true),
                );
                ui.label(format!("Current budget: {}", self.render_budget));
                if ui.button("Apply render budget").clicked() {
                    apply_render_budget = true;
                }

                ui.separator();
                ui.heading("Galaxy");
                ui.horizontal(|ui| {
                    ui.label("Seed:");
                    ui.text_edit_singleline(&mut self.settings_seed_input);
                });
                ui.small("Use decimal or 0x-prefixed hex.");
                ui.small("Restarting the simulation wipes saved progress because system IDs change with seed.");

                ui.horizontal(|ui| {
                    if ui.button("Restart with entered seed").clicked() {
                        restart_seed = Self::parse_seed_input(&self.settings_seed_input);
                        seed_restart_requested = true;
                    }
                    if ui.button("Restart with random seed").clicked() {
                        random_restart = true;
                    }
                });
            });

        self.settings_window_open = open;

        if apply_lod_preset {
            self.apply_lod_preset(self.settings_lod_preset);
        }

        if apply_render_budget {
            self.render_budget = self
                .settings_render_budget
                .clamp(RENDER_BUDGET_MIN, RENDER_BUDGET_MAX);
            self.settings_render_budget = self.render_budget;
            self.last_required_ready_hash = u64::MAX;
            self.visible_build_inflight = false;
            self.game_notice = Some(format!(
                "Render budget set to {} (preset: {}).",
                self.render_budget,
                self.lod_preset.label(),
            ));
        }

        if random_restart {
            let random_seed = rand::random::<u64>();
            self.restart_simulation_with_seed(random_seed);
            return;
        }

        if seed_restart_requested {
            if let Some(seed) = restart_seed {
                self.restart_simulation_with_seed(seed);
            } else {
                self.game_notice = Some(
                    "Invalid seed. Enter decimal or 0x-prefixed hex value.".to_owned(),
                );
            }
        }
    }

    fn show_resources_window(&mut self, ctx: &egui::Context) {
        if !self.resources_panel_open {
            return;
        }

        let player_faction_id = self.game_state.player.faction_id.clone();
        let player_faction = self
            .game_state
            .factions
            .get(&self.game_state.player.faction_id);
        let player_treasury = player_faction.map(|f| f.treasury).unwrap_or(0);
        let player_tech_level = player_faction
            .map(|f| f.colonization_tech_level)
            .unwrap_or(0);
        let player_tech_progress = player_faction
            .map(|f| f.colonization_tech_progress)
            .unwrap_or(0.0);
        let mut total_food_stockpile = 0.0_f32;
        let mut total_industry_stockpile = 0.0_f32;
        let mut total_energy_stockpile = 0.0_f32;
        let mut total_stockpile_capacity = 0.0_f32;
        let mut element_resource_amounts: HashMap<String, f32> = HashMap::new();
        let mut atmosphere_resource_amounts: HashMap<String, f32> = HashMap::new();
        let player_colony_count = self
            .game_state
            .colonies
            .values()
            .filter_map(|colony| {
                if colony.owner_faction == player_faction_id {
                    total_food_stockpile += colony.food_stockpile;
                    total_industry_stockpile += colony.industry_stockpile;
                    total_energy_stockpile += colony.energy_stockpile;
                    total_stockpile_capacity += colony.stockpile_capacity;
                    for (symbol, amount) in &colony.element_stockpiles {
                        *element_resource_amounts
                            .entry(symbol.clone())
                            .or_insert(0.0) += amount.max(0.0);
                    }
                    for (formula, amount) in &colony.atmosphere_stockpiles {
                        *atmosphere_resource_amounts
                            .entry(formula.clone())
                            .or_insert(0.0) += amount.max(0.0);
                    }
                    Some(())
                } else {
                    None
                }
            })
            .count();

        let pending_scan_count = self.game_state.pending_survey_scans.len();
        let pending_colony_count = self.game_state.pending_colony_foundings.len();
        let pending_building_count = self.game_state.pending_colony_buildings.len();

        let mut open = self.resources_panel_open;
        egui::Window::new("Resources")
            .open(&mut open)
            .resizable(true)
            .default_size([760.0, 560.0])
            .show(ctx, |ui| {
                ui.heading("Resource Overview");
                egui::Grid::new("resources_overview_grid")
                    .num_columns(3)
                    .spacing([14.0, 4.0])
                    .show(ui, |ui| {
                        ui.label(format!("Faction treasury: {}", player_treasury));
                        ui.label(format!("Pending scans: {}", pending_scan_count));
                        ui.label("");
                        ui.end_row();

                        ui.label(format!("Colonies owned: {}", player_colony_count));
                        ui.label(format!("Tech level: L{}", player_tech_level));
                        ui.label(format!(
                            "Tech progress: {:.0}%",
                            player_tech_progress.clamp(0.0, 1.0) * 100.0
                        ));
                        ui.end_row();

                        ui.label(format!("Pending colony expeditions: {}", pending_colony_count));
                        ui.label(format!("Pending building projects: {}", pending_building_count));
                        ui.label("");
                        ui.end_row();

                        ui.label(format!(
                            "Food stockpile: {:.1}/{:.1}",
                            total_food_stockpile, total_stockpile_capacity
                        ));
                        ui.label(format!(
                            "Industry stockpile: {:.1}/{:.1}",
                            total_industry_stockpile, total_stockpile_capacity
                        ));
                        ui.label(format!(
                            "Energy stockpile: {:.1}/{:.1}",
                            total_energy_stockpile, total_stockpile_capacity
                        ));
                        ui.end_row();
                    });

                ui.separator();
                ui.small(
                    "Catalog entries below include all elements and atmospheric gases that can appear in procedural planet/atmosphere compositions.",
                );
                ui.small(
                    "Element amounts are summed from colony stockpiles (the spendable pool used by construction).",
                );
                ui.small(
                    "Atmosphere amounts are harvested stockpiles gathered by Atmosphere Harvesters from local gas mixtures.",
                );

                ui.columns(2, |columns| {
                    columns[0].push_id("resource_elements_column", |ui| {
                        ui.label(egui::RichText::new("Elemental Stockpiles").strong());
                        ui.small(format!(
                            "{} catalog entries",
                            composition_element_resource_catalog().len()
                        ));
                        egui::ScrollArea::vertical()
                            .max_height(340.0)
                            .show(ui, |ui| {
                                egui::Grid::new("resource_elements_grid")
                                    .num_columns(4)
                                    .spacing([12.0, 4.0])
                                    .striped(true)
                                    .show(ui, |ui| {
                                        ui.label(egui::RichText::new("#").underline());
                                        ui.label(egui::RichText::new("Symbol").underline());
                                        ui.label(egui::RichText::new("Element").underline());
                                        ui.label(egui::RichText::new("Amount").underline());
                                        ui.end_row();

                                        for entry in composition_element_resource_catalog() {
                                            ui.label(entry.atomic_number.to_string());
                                            ui.label(entry.symbol);
                                            ui.label(entry.name);
                                            let amount = element_resource_amounts
                                                .get(entry.symbol)
                                                .copied()
                                                .unwrap_or(0.0);
                                            ui.label(format!("{amount:.2}"));
                                            ui.end_row();
                                        }
                                    });
                            });
                    });

                    columns[1].push_id("resource_atmosphere_column", |ui| {
                        ui.label(egui::RichText::new("Atmospheric Resources").strong());
                        ui.small(format!(
                            "{} catalog entries",
                            atmosphere_resource_catalog().len()
                        ));
                        egui::ScrollArea::vertical()
                            .max_height(340.0)
                            .show(ui, |ui| {
                                egui::Grid::new("resource_atmosphere_grid")
                                    .num_columns(3)
                                    .spacing([12.0, 4.0])
                                    .striped(true)
                                    .show(ui, |ui| {
                                        ui.label(egui::RichText::new("Formula").underline());
                                        ui.label(egui::RichText::new("Gas").underline());
                                        ui.label(egui::RichText::new("Amount").underline());
                                        ui.end_row();

                                        for entry in atmosphere_resource_catalog() {
                                            ui.label(entry.formula);
                                            ui.label(entry.name);
                                            let amount = atmosphere_resource_amounts
                                                .get(entry.formula)
                                                .copied()
                                                .unwrap_or(0.0);
                                            ui.label(format!("{amount:.2}"));
                                            ui.end_row();
                                        }
                                    });
                            });
                    });
                });
            });

        self.resources_panel_open = open;
    }

    fn persist_game_state(&mut self) {
        self.trim_game_events();
        match save::save_game_save(GAME_SAVE_PATH, &self.game_state, &self.game_events) {
            Ok(()) => {
                self.game_save_error = None;
            }
            Err(err) => {
                self.game_save_error = Some(format!("Failed to save game state: {err}"));
            }
        }
    }

    fn record_game_event(&mut self, event: GameEvent) {
        self.game_state.apply_event(&event);
        self.game_events.push(event);
        self.trim_game_events();
        self.game_autosave_accum_years = 0.0;
        self.persist_game_state();
    }

    fn reset_all_progress(&mut self) {
        self.delta_store = DeltaStore::default();
        self.game_state = GameState::default();
        self.game_events.clear();
        self.game_paused = true;
        self.game_autosave_accum_years = 0.0;
        self.game_save_error = None;
        self.game_notice = Some("All saved progress has been reset.".to_owned());
        self.reset_progress_armed = false;
        self.starting_colony_selection = None;
        self.colony_source_selection = None;
        self.colony_transfer_colonists = COLONY_TRANSFER_POP_MIN;
        self.colonies_window_open = false;
        self.construction_window_open = false;
        self.selected_system = None;
        self.camera_lock_target = Some(galaxy_center());
        self.camera_focus_tween = None;
        self.pan_velocity = egui::Vec2::ZERO;

        self.travel_system = None;
        self.travel_window_open = false;
        self.travel_paused = false;
        self.travel_sim_years = 0.0;
        self.travel_last_input_time = None;
        self.travel_selected_body = None;
        self.reset_travel_view();

        self.strategic_clock.reset_timebase();

        let mut remove_errors = Vec::new();
        for path in [GAME_SAVE_PATH, DELTA_SAVE_PATH] {
            if let Err(err) = fs::remove_file(path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    remove_errors.push(format!("{path}: {err}"));
                }
            }
        }

        self.persist_game_state();

        if !remove_errors.is_empty() {
            self.game_save_error = Some(format!(
                "Some save files could not be removed: {}",
                remove_errors.join(" | ")
            ));
        }
    }

    fn full_view_sector_capacity(sector_size: f32) -> usize {
        let center = galaxy_center();
        let sector_cull_radius = playfield_radius() + sector_size * 1.5;
        let sector_cull_radius_sq = sector_cull_radius * sector_cull_radius;
        let radius_sectors = (sector_cull_radius / sector_size).ceil() as i32 + 1;
        let mut count = 0usize;

        for sx in -radius_sectors..=radius_sectors {
            for sy in -radius_sectors..=radius_sectors {
                let sector_center_x = center[0] + (sx as f32 + 0.5) * sector_size;
                let sector_center_y = center[1] + (sy as f32 + 0.5) * sector_size;
                let dx = sector_center_x - center[0];
                let dy = sector_center_y - center[1];
                if dx * dx + dy * dy <= sector_cull_radius_sq {
                    count = count.saturating_add(1);
                }
            }
        }

        count
    }

    fn focus_camera_on_world_pos(&mut self, pos: [f32; 3], center: [f32; 3]) {
        let rotated = rotate_point(pos, self.yaw, self.pitch, center);
        self.pan.x = center[0] - rotated[0];
        self.pan.y = center[1] - rotated[1];
        self.pan_velocity = egui::Vec2::ZERO;
        self.camera_focus_tween = None;
        self.camera_lock_target = Some(pos);
    }

    fn recenter_camera_pan_to_target(&mut self, pos: [f32; 3], center: [f32; 3]) {
        let rotated = rotate_point(pos, self.yaw, self.pitch, center);
        self.pan.x = center[0] - rotated[0];
        self.pan.y = center[1] - rotated[1];
        self.pan_velocity = egui::Vec2::ZERO;
    }

    fn shift_camera_lock_target_by_pan_delta(&mut self, pan_delta: egui::Vec2, center: [f32; 3]) {
        let Some(lock_target) = self.camera_lock_target else {
            return;
        };
        let mut lock_rot = rotate_point(lock_target, self.yaw, self.pitch, center);
        lock_rot[0] -= pan_delta.x;
        lock_rot[1] -= pan_delta.y;
        self.camera_lock_target = Some(unrotate_point(lock_rot, self.yaw, self.pitch, center));
    }

    fn start_camera_focus_tween(&mut self, pos: [f32; 3], center: [f32; 3], zoom_target: f32) {
        let rotated = rotate_point(pos, self.yaw, self.pitch, center);
        let target_pan = egui::Vec2::new(center[0] - rotated[0], center[1] - rotated[1]);
        let target_zoom = zoom_target.clamp(0.0001, 10.0);
        self.pan_velocity = egui::Vec2::ZERO;
        self.camera_focus_tween = Some(CameraFocusTween {
            target_pan,
            target_zoom,
        });
    }

    fn tick_camera_focus_tween(&mut self, ctx: &egui::Context) {
        let Some(tween) = self.camera_focus_tween else {
            return;
        };

        let dt = ctx.input(|i| i.unstable_dt).clamp(1.0 / 240.0, 1.0 / 20.0);
        let pan_blend = 1.0 - (-dt * 10.0).exp();
        let zoom_blend = 1.0 - (-dt * 8.0).exp();

        self.pan += (tween.target_pan - self.pan) * pan_blend;
        self.zoom += (tween.target_zoom - self.zoom) * zoom_blend;
        self.zoom = self.zoom.clamp(0.0001, 10.0);

        let pan_error_sq = (tween.target_pan - self.pan).length_sq();
        let zoom_error = (tween.target_zoom - self.zoom).abs();
        if pan_error_sq < 1.0 && zoom_error < 0.0001 {
            self.pan = tween.target_pan;
            self.zoom = tween.target_zoom;
            self.camera_focus_tween = None;
        } else {
            ctx.request_repaint();
        }
    }

    fn zoom_for_system_lod(&self) -> f32 {
        let systems_threshold_sector_px = LOD_TRANSITIONS
            .last()
            .map(|(max_sector_px, _, _)| *max_sector_px)
            .unwrap_or(15_000.0);
        let denom = (self.sector_size * self.lod_transition_zoom_scale.max(0.05)).max(0.0001);
        (systems_threshold_sector_px / denom * 1.02).clamp(0.0001, 10.0)
    }

    fn load_system_detail_by_id(&mut self, id: SystemId) -> Option<SystemDetail> {
        if id == Self::sagittarius_a_system_id() {
            return Some(self.sagittarius_a_system_detail());
        }

        if !self.sector_cache.contains(id.sector) {
            let systems = self.procedural_generator.generate_sector(id.sector);
            self.sector_cache.insert(id.sector, systems);
        }

        let summary = self
            .sector_cache
            .get(id.sector)
            .and_then(|systems| {
                systems
                    .iter()
                    .find(|summary| summary.id.local_index == id.local_index)
                    .copied()
            })?;

        let mut detail = self.procedural_generator.generate_system_detail(&summary);
        self.delta_store.apply_to_detail(&mut detail);
        detail.explored = self.game_state.is_system_explored(id);
        Some(detail)
    }

    fn colony_build_site_profile(
        detail: Option<&SystemDetail>,
        site: ColonyBuildingSite,
    ) -> ColonyBuildingSiteProfile {
        let Some(detail) = detail else {
            return ColonyBuildingSiteProfile::default();
        };

        match site {
            ColonyBuildingSite::Planet(index) => {
                if let Some(planet) = detail.planets.get(index as usize) {
                    ColonyBuildingSiteProfile {
                        planet_is_gas_giant: Some(planet.kind.is_gas_giant()),
                        planet_habitable: Some(planet.habitable),
                        planet_building_slot_capacity: Some(
                            GameState::planet_building_slot_capacity_for_radius(
                                planet.radius_earth,
                            ),
                        ),
                        planet_has_atmosphere: Some(
                            !planet.atmosphere.is_empty()
                                && planet.atmosphere_pressure_atm > 0.0,
                        ),
                        star_is_scoopable: None,
                    }
                } else {
                    ColonyBuildingSiteProfile::default()
                }
            }
            ColonyBuildingSite::Star(index) => {
                if let Some(star) = detail.stars.get(index as usize) {
                    let is_scoopable = star.class.spectral.is_scoopable();
                    ColonyBuildingSiteProfile {
                        planet_is_gas_giant: None,
                        planet_habitable: None,
                        planet_building_slot_capacity: None,
                        planet_has_atmosphere: None,
                        star_is_scoopable: Some(is_scoopable),
                    }
                } else {
                    ColonyBuildingSiteProfile::default()
                }
            }
            ColonyBuildingSite::Orbital => {
                ColonyBuildingSiteProfile::default()
            }
        }
    }

    fn colony_build_site_context_label(detail: Option<&SystemDetail>, site: ColonyBuildingSite) -> String {
        let Some(detail) = detail else {
            return "Unknown site context".to_owned();
        };

        match site {
            ColonyBuildingSite::Orbital => "Station ring and orbital infrastructure".to_owned(),
            ColonyBuildingSite::Star(index) => detail
                .stars
                .get(index as usize)
                .map(|star| format!("Stellar site ({})", star.class.notation()))
                .unwrap_or_else(|| "Unknown stellar site".to_owned()),
            ColonyBuildingSite::Planet(index) => detail
                .planets
                .get(index as usize)
                .map(|planet| {
                    let habitability = if planet.habitable {
                        "habitable"
                    } else {
                        "non-habitable"
                    };
                    let slots =
                        GameState::planet_building_slot_capacity_for_radius(planet.radius_earth);
                    format!(
                        "{} world ({habitability}) | Radius {:.2} R⊕ | Slots {}",
                        planet.kind.label(),
                        planet.radius_earth,
                        slots
                    )
                })
                .unwrap_or_else(|| "Unknown planetary site".to_owned()),
        }
    }

    fn colony_building_effect_summary(kind: ColonyBuildingKind) -> String {
        let effects = kind.effect_preview_per_level();
        let mut segments = Vec::new();

        if effects.food_production_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Food production {:.4}/year",
                effects.food_production_bonus
            ));
        }
        if effects.industry_production_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Industry production {:.4}/year",
                effects.industry_production_bonus
            ));
        }
        if effects.energy_production_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Energy production {:.4}/year",
                effects.energy_production_bonus
            ));
        }
        if effects.food_demand_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Food demand {:.4}/year",
                effects.food_demand_bonus
            ));
        }
        if effects.industry_demand_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Industry demand {:.4}/year",
                effects.industry_demand_bonus
            ));
        }
        if effects.energy_demand_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Energy demand {:.4}/year",
                effects.energy_demand_bonus
            ));
        }
        if effects.element_extraction_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Element extraction {:.3} profile-rate/year",
                effects.element_extraction_bonus
            ));
        }
        if effects.atmosphere_harvest_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Atmosphere harvesting {:.3} profile-rate/year",
                effects.atmosphere_harvest_bonus
            ));
        }
        if effects.treasury_production_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Treasury {:.0}/year (pop-scaled)",
                effects.treasury_production_bonus
            ));
        }
        if effects.stability_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Stability {:.4}/year",
                effects.stability_bonus
            ));
        }
        if effects.growth_bonus.abs() > f32::EPSILON {
            segments.push(format!(
                "+Growth rate {:.5}/year",
                effects.growth_bonus
            ));
        }
        if effects.annual_upkeep != 0 {
            segments.push(format!("Annual upkeep {}", effects.annual_upkeep));
        }

        if segments.is_empty() {
            "No direct production or upkeep modifiers per level.".to_owned()
        } else {
            segments.join(" | ")
        }
    }

    fn colony_building_cost_summary(cost: &ColonyBuildingCostPreview) -> String {
        let mut text = format!(
            "Treasury {} | Food {:.1} | Industry {:.1} | Energy {:.1} | Duration {:.2}y",
            cost.treasury, cost.food, cost.industry, cost.energy, cost.duration_years,
        );

        if !cost.element_costs.is_empty() {
            let element_text = cost
                .element_costs
                .iter()
                .map(|(symbol, amount)| format!("{} {:.1}", symbol, amount))
                .collect::<Vec<_>>()
                .join(", ");
            text.push_str(" | Elements: ");
            text.push_str(&element_text);
        }

        text
    }

    fn show_favorites_window(&mut self, ctx: &egui::Context, center3d: [f32; 3]) {
        if !self.favorites_window_open {
            return;
        }

        let mut open = self.favorites_window_open;
        let mut focus_system: Option<SystemId> = None;
        let mut select_system: Option<SystemId> = None;

        let fav_ids = self.delta_store.favorited_system_ids();

        // Pre-load details for all favorited systems.
        let mut rows: Vec<(SystemId, String, usize, usize, bool)> = Vec::new();
        for id in &fav_ids {
            if let Some(detail) = self.load_system_detail_by_id(*id) {
                let habitable_count = detail.planets.iter().filter(|p| p.habitable).count();
                rows.push((
                    *id,
                    detail.display_name.clone(),
                    detail.stars.len(),
                    detail.planets.len(),
                    habitable_count > 0,
                ));
            }
        }

        egui::Window::new("Favorited Systems")
            .open(&mut open)
            .resizable(true)
            .default_size([520.0, 340.0])
            .show(ctx, |ui| {
                if rows.is_empty() {
                    ui.label("No systems have been favorited yet.");
                } else {
                    ui.label(format!("{} favorited system(s)", rows.len()));
                    ui.separator();
                    egui::ScrollArea::vertical()
                        .max_height(280.0)
                        .show(ui, |ui| {
                            egui::Grid::new("favorites_table")
                                .num_columns(6)
                                .striped(true)
                                .spacing([10.0, 4.0])
                                .show(ui, |ui| {
                                    ui.label(egui::RichText::new("Name").underline());
                                    ui.label(egui::RichText::new("Stars").underline());
                                    ui.label(egui::RichText::new("Planets").underline());
                                    ui.label(egui::RichText::new("Habitable").underline());
                                    ui.label(egui::RichText::new("Focus").underline());
                                    ui.label(egui::RichText::new("Select").underline());
                                    ui.end_row();

                                    for (id, name, stars, planets, has_habitable) in &rows {
                                        ui.label(name);
                                        ui.label(format!("{}", stars));
                                        ui.label(format!("{}", planets));
                                        ui.label(if *has_habitable { "★" } else { "" });
                                        if ui.button("Focus").clicked() {
                                            focus_system = Some(*id);
                                        }
                                        if ui.button("Select").clicked() {
                                            select_system = Some(*id);
                                        }
                                        ui.end_row();
                                    }
                                });
                        });
                }
            });

        self.favorites_window_open = open;

        if let Some(id) = focus_system {
            if let Some(detail) = self.load_system_detail_by_id(id) {
                self.focus_camera_on_world_pos(detail.pos, center3d);
                self.game_notice = Some(format!("Focused on system: {}", detail.display_name));
            }
        }

        if let Some(id) = select_system {
            if let Some(detail) = self.load_system_detail_by_id(id) {
                self.focus_camera_on_world_pos(detail.pos, center3d);
                self.selected_system = Some(detail);
            }
        }
    }

    fn show_colonies_window(&mut self, ctx: &egui::Context, center3d: [f32; 3]) {
        if !self.colonies_window_open {
            return;
        }

        let mut open = self.colonies_window_open;
        let mut focus_colony: Option<u64> = None;
        let mut select_colony: Option<u64> = None;
        let mut policy_updates: Vec<(u64, ColonyPolicy)> = Vec::new();
        let mut taxation_updates: Vec<(u64, TaxationPolicy)> = Vec::new();
        let mut nudge_updates: Vec<(u64, usize)> = Vec::new();
        let mut transfer_request: Option<(u64, u64, u32)> = None;

        egui::Window::new("Colonized Systems")
            .open(&mut open)
            .resizable(true)
            .default_size([940.0, 500.0])
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.checkbox(&mut self.colonies_window_player_only, "Player colonies only");
                    ui.label(format!("Total colonies: {}", self.game_state.colonies.len()));
                });

                let mut colonies = self
                    .game_state
                    .colonies
                    .values()
                    .filter(|colony| {
                        !self.colonies_window_player_only
                            || colony.owner_faction == self.game_state.player.faction_id
                    })
                    .map(|colony| {
                        (
                            colony.id,
                            colony.name.clone(),
                            colony.owner_faction.clone(),
                            format!("{:?}", colony.stage),
                            colony.population,
                            colony.system,
                            colony.body_index,
                            colony.policy,
                            colony.taxation_policy,
                            colony.stability,
                            colony.food_balance,
                            colony.industry_balance,
                            colony.energy_balance,
                            colony.defense_balance,
                            colony.last_tax_revenue_annual,
                            colony.last_net_revenue_annual,
                        )
                    })
                    .collect::<Vec<_>>();
                colonies.sort_by(|a, b| a.1.cmp(&b.1));

                egui::ScrollArea::vertical()
                    .max_height(410.0)
                    .show(ui, |ui| {
                        egui::Grid::new("colonies_table")
                            .num_columns(13)
                            .striped(true)
                            .spacing([10.0, 4.0])
                            .show(ui, |ui| {
                                ui.label(egui::RichText::new("Colony").underline());
                                ui.label(egui::RichText::new("Owner").underline());
                                ui.label(egui::RichText::new("Stage").underline());
                                ui.label(egui::RichText::new("Population").underline());
                                ui.label(egui::RichText::new("Sector").underline());
                                ui.label(egui::RichText::new("Policy").underline());
                                ui.label(egui::RichText::new("Tax").underline());
                                ui.label(egui::RichText::new("Stability").underline());
                                ui.label(egui::RichText::new("Balances").underline());
                                ui.label(egui::RichText::new("Revenue (yr)").underline());
                                ui.label(egui::RichText::new("Tune").underline());
                                ui.label(egui::RichText::new("Focus").underline());
                                ui.label(egui::RichText::new("Select").underline());
                                ui.end_row();

                                for (
                                    id,
                                    name,
                                    owner,
                                    stage,
                                    population,
                                    system,
                                    _body_index,
                                    policy,
                                    taxation_policy,
                                    stability,
                                    food,
                                    industry,
                                    energy,
                                    defense,
                                    tax_revenue,
                                    net_revenue,
                                ) in &colonies
                                {
                                    ui.label(name);
                                    ui.label(owner);
                                    ui.label(stage);
                                    ui.label(format!("{:.0}", population));
                                    ui.label(format!(
                                        "({}, {}) / #{}",
                                        system.sector.x, system.sector.y, system.local_index
                                    ));

                                    if owner == &self.game_state.player.faction_id {
                                        let mut selected_policy = *policy;
                                        egui::ComboBox::from_id_source(format!(
                                            "colony_policy_{}",
                                            id
                                        ))
                                        .selected_text(selected_policy.label())
                                        .show_ui(ui, |ui| {
                                            for option in ColonyPolicy::all() {
                                                ui.selectable_value(
                                                    &mut selected_policy,
                                                    option,
                                                    option.label(),
                                                ).on_hover_text(option.description());
                                            }
                                        });
                                        if selected_policy != *policy {
                                            policy_updates.push((*id, selected_policy));
                                        }
                                    } else {
                                        ui.label("NPC");
                                    }

                                    if owner == &self.game_state.player.faction_id {
                                        let mut selected_taxation = *taxation_policy;
                                        egui::ComboBox::from_id_source(format!(
                                            "colony_taxation_{}",
                                            id
                                        ))
                                        .selected_text(selected_taxation.label())
                                        .show_ui(ui, |ui| {
                                            for option in TaxationPolicy::all() {
                                                ui.selectable_value(
                                                    &mut selected_taxation,
                                                    option,
                                                    option.label(),
                                                ).on_hover_text(option.description());
                                            }
                                        });
                                        if selected_taxation != *taxation_policy {
                                            taxation_updates.push((*id, selected_taxation));
                                        }
                                    } else {
                                        ui.label("NPC");
                                    }

                                    {
                                        let pct = stability * 100.0;
                                        let color = if pct >= 70.0 {
                                            egui::Color32::from_rgb(120, 210, 120)
                                        } else if pct >= 40.0 {
                                            egui::Color32::from_rgb(220, 200, 80)
                                        } else {
                                            egui::Color32::from_rgb(220, 90, 80)
                                        };
                                        ui.label(
                                            egui::RichText::new(format!("{:.0}%", pct))
                                                .color(color),
                                        );
                                    }

                                    ui.label(format!(
                                        "F {:+.2}  I {:+.2}  E {:+.2}  D {:+.2}",
                                        food, industry, energy, defense
                                    ));

                                    ui.label(format!(
                                        "Tax {}  Net {:+}",
                                        tax_revenue, net_revenue
                                    ));

                                    if owner == &self.game_state.player.faction_id {
                                        ui.horizontal(|ui| {
                                            if ui.small_button("F+").clicked() {
                                                nudge_updates.push((*id, 0));
                                            }
                                            if ui.small_button("I+").clicked() {
                                                nudge_updates.push((*id, 1));
                                            }
                                            if ui.small_button("E+").clicked() {
                                                nudge_updates.push((*id, 2));
                                            }
                                            if ui.small_button("D+").clicked() {
                                                nudge_updates.push((*id, 3));
                                            }
                                        });
                                    } else {
                                        ui.label("-");
                                    }

                                    if ui.small_button("Focus").clicked() {
                                        focus_colony = Some(*id);
                                    }
                                    if ui.small_button("Select").clicked() {
                                        select_colony = Some(*id);
                                    }
                                    ui.end_row();
                                }
                            });
                    });

                ui.small(
                    "Policies: Balanced (steady), Growth (+pop, −stability), Industry (+output, −stability), Fortress (+defense/stability, −growth).",
                );
                ui.small("Taxation: Low (+stability/growth, −revenue), Standard, High (+revenue, −stability), Extractive (max revenue, heavy penalties).");
                ui.small("Stability affects production efficiency and population growth. Keep it high for a thriving colony.");
                ui.small(
                    "Use the standalone Construction button in the main toolbar to queue buildings and inspect costs.",
                );
                ui.small("Tune buttons apply immediate small balance shifts to player colonies.");

                // ── Population Transfer panel ──
                ui.separator();
                ui.label(egui::RichText::new("Population Transfer").strong());

                let player_colonies: Vec<(u64, String, f64)> = self
                    .game_state
                    .colonies
                    .values()
                    .filter(|c| c.owner_faction == self.game_state.player.faction_id)
                    .map(|c| (c.id, c.name.clone(), c.population))
                    .collect();

                if player_colonies.len() < 2 {
                    ui.small("You need at least two colonies to transfer population.");
                } else {
                    ui.horizontal(|ui| {
                        ui.label("From:");
                        let source_label = self.colony_transfer_source
                            .and_then(|id| player_colonies.iter().find(|c| c.0 == id))
                            .map(|c| c.1.clone())
                            .unwrap_or_else(|| "Select…".to_owned());
                        egui::ComboBox::from_id_source("transfer_source")
                            .selected_text(&source_label)
                            .show_ui(ui, |ui| {
                                for (id, name, pop) in &player_colonies {
                                    let label = format!("{} ({:.0} pop)", name, pop);
                                    if ui.selectable_value(
                                        &mut self.colony_transfer_source,
                                        Some(*id),
                                        &label,
                                    ).clicked() {}
                                }
                            });

                        ui.label("To:");
                        let target_label = self.colony_transfer_target
                            .and_then(|id| player_colonies.iter().find(|c| c.0 == id))
                            .map(|c| c.1.clone())
                            .unwrap_or_else(|| "Select…".to_owned());
                        egui::ComboBox::from_id_source("transfer_target")
                            .selected_text(&target_label)
                            .show_ui(ui, |ui| {
                                for (id, name, pop) in &player_colonies {
                                    let label = format!("{} ({:.0} pop)", name, pop);
                                    if ui.selectable_value(
                                        &mut self.colony_transfer_target,
                                        Some(*id),
                                        &label,
                                    ).clicked() {}
                                }
                            });
                    });

                    ui.horizontal(|ui| {
                        ui.label("Colonists:");
                        ui.add(egui::Slider::new(&mut self.colony_transfer_amount, 100..=50_000).logarithmic(true));

                        let cost = (self.colony_transfer_amount as f64 * 1.8).round() as i64;
                        ui.label(format!("Cost: {} cr", cost));
                    });

                    // Show pending transfers.
                    let pending_count = self.game_state.pending_population_transfers.len();
                    if pending_count > 0 {
                        ui.small(format!("{} transfer(s) in transit.", pending_count));
                    }

                    let can_transfer = self.colony_transfer_source.is_some()
                        && self.colony_transfer_target.is_some()
                        && self.colony_transfer_source != self.colony_transfer_target;

                    if ui.add_enabled(can_transfer, egui::Button::new("Send Transfer"))
                        .on_hover_text("Deducts population + stability from source immediately. Destination receives colonists after transit, with a stability penalty on arrival.")
                        .clicked()
                    {
                        if let (Some(src), Some(tgt)) = (self.colony_transfer_source, self.colony_transfer_target) {
                            transfer_request = Some((src, tgt, self.colony_transfer_amount));
                        }
                    }
                }
            });

        self.colonies_window_open = open;

        let mut colony_settings_changed = false;
        for (id, policy) in policy_updates {
            if let Some(colony) = self.game_state.colonies.get_mut(&id) {
                if colony.owner_faction == self.game_state.player.faction_id && colony.policy != policy {
                    colony.policy = policy;
                    colony_settings_changed = true;
                }
            }
        }

        for (id, taxation_policy) in taxation_updates {
            if let Some(colony) = self.game_state.colonies.get_mut(&id) {
                if colony.owner_faction == self.game_state.player.faction_id
                    && colony.taxation_policy != taxation_policy
                {
                    colony.taxation_policy = taxation_policy;
                    colony_settings_changed = true;
                }
            }
        }

        let mut nudges_applied = 0usize;
        let mut nudges_rejected = 0usize;
        for (id, target_index) in nudge_updates {
            let faction_treasury = self
                .game_state
                .factions
                .get(&self.game_state.player.faction_id)
                .map(|faction| faction.treasury)
                .unwrap_or(0);
            if faction_treasury < COLONY_BALANCE_NUDGE_COST {
                nudges_rejected += 1;
                continue;
            }

            let mut should_charge = false;
            if let Some(colony) = self.game_state.colonies.get_mut(&id) {
                if colony.owner_faction != self.game_state.player.faction_id {
                    continue;
                }

                let mut deltas = [COLONY_BALANCE_NUDGE_SIDE_DELTA; 4];
                if let Some(delta) = deltas.get_mut(target_index) {
                    *delta = COLONY_BALANCE_NUDGE_FOCUS_DELTA;
                }

                colony.food_balance = (colony.food_balance + deltas[0]).clamp(-0.35, 0.35);
                colony.industry_balance = (colony.industry_balance + deltas[1]).clamp(-0.35, 0.35);
                colony.energy_balance = (colony.energy_balance + deltas[2]).clamp(-0.35, 0.35);
                colony.defense_balance = (colony.defense_balance + deltas[3]).clamp(-0.20, 0.50);
                colony_settings_changed = true;
                should_charge = true;
            }

            if should_charge {
                if let Some(faction) = self.game_state.factions.get_mut(&self.game_state.player.faction_id)
                {
                    faction.treasury = faction.treasury.saturating_sub(COLONY_BALANCE_NUDGE_COST);
                }
                nudges_applied += 1;
            }
        }

        if colony_settings_changed {
            self.game_autosave_accum_years = 0.0;
            self.persist_game_state();
            if nudges_applied > 0 {
                self.game_notice = Some(format!(
                    "Applied {} colony tuning action(s). Treasury spent: {}.",
                    nudges_applied,
                    COLONY_BALANCE_NUDGE_COST.saturating_mul(nudges_applied as i64)
                ));
            } else if self.game_notice.is_none() {
                self.game_notice = Some("Colony management settings updated.".to_owned());
            }
        } else if nudges_rejected > 0 {
            self.game_notice = Some(format!(
                "Colony tuning denied: each action requires {} treasury.",
                COLONY_BALANCE_NUDGE_COST
            ));
        }

        if let Some(colony_id) = focus_colony {
            if let Some((pos, name)) = self
                .game_state
                .colonies
                .get(&colony_id)
                .map(|colony| (colony.system_pos, colony.name.clone()))
            {
                self.focus_camera_on_world_pos(pos, center3d);
                self.game_notice = Some(format!("Focused camera on colony: {name}"));
            }
        }

        if let Some(colony_id) = select_colony {
            if let Some((system, pos, name)) = self
                .game_state
                .colonies
                .get(&colony_id)
                .map(|colony| (colony.system, colony.system_pos, colony.name.clone()))
            {
                self.focus_camera_on_world_pos(pos, center3d);
                if let Some(detail) = self.load_system_detail_by_id(system) {
                    self.selected_system = Some(detail);
                } else {
                    self.game_notice = Some(format!(
                        "Could not load selected colony system for {name}."
                    ));
                }
            }
        }

        if let Some((src, tgt, amount)) = transfer_request {
            let faction_id = self.game_state.player.faction_id.clone();
            match self.game_state.queue_population_transfer(src, tgt, amount, &faction_id) {
                Ok(duration) => {
                    self.game_autosave_accum_years = 0.0;
                    self.persist_game_state();
                    self.game_notice = Some(format!(
                        "Population transfer dispatched. ETA: {:.1} years.",
                        duration
                    ));
                }
                Err(msg) => {
                    self.game_notice = Some(format!("Transfer failed: {msg}"));
                }
            }
        }
    }

    fn show_construction_window(&mut self, ctx: &egui::Context) {
        if !self.construction_window_open {
            return;
        }

        let mut open = self.construction_window_open;
        let mut building_queue_requests: Vec<(
            u64,
            ColonyBuildingKind,
            ColonyBuildingSite,
            ColonyBuildingSiteProfile,
        )> = Vec::new();

        egui::Window::new("Construction")
            .open(&mut open)
            .resizable(true)
            .default_size([620.0, 420.0])
            .show(ctx, |ui| {
                let mut player_colonies = self
                    .game_state
                    .colonies
                    .values()
                    .filter(|colony| colony.owner_faction == self.game_state.player.faction_id)
                    .map(|colony| {
                        (
                            colony.id,
                            colony.name.clone(),
                            colony.system,
                            colony.body_index,
                        )
                    })
                    .collect::<Vec<_>>();
                player_colonies.sort_by(|a, b| a.1.cmp(&b.1));

                if player_colonies.is_empty() {
                    ui.small("No player-owned colonies are available for construction.");
                    return;
                }

                let mut selected_colony_id = self
                    .colony_build_panel_selection
                    .filter(|selected| {
                        player_colonies
                            .iter()
                            .any(|(colony_id, _, _, _)| colony_id == selected)
                    })
                    .unwrap_or(player_colonies[0].0);

                ui.horizontal(|ui| {
                    ui.label("Colony:");
                    egui::ComboBox::from_id_source("construction_colony_select")
                        .selected_text(
                            player_colonies
                                .iter()
                                .find(|(id, _, _, _)| *id == selected_colony_id)
                                .map(|(_, name, _, _)| name.clone())
                                .unwrap_or_else(|| "Select colony".to_owned()),
                        )
                        .show_ui(ui, |ui| {
                            for (colony_id, colony_name, _, _) in &player_colonies {
                                ui.selectable_value(
                                    &mut selected_colony_id,
                                    *colony_id,
                                    colony_name.clone(),
                                );
                            }
                        });
                });
                self.colony_build_panel_selection = Some(selected_colony_id);

                let Some(selected_colony) = self.game_state.colonies.get(&selected_colony_id).cloned() else {
                    ui.small("Selected colony is no longer available.");
                    return;
                };

                let detail = self.load_system_detail_by_id(selected_colony.system);

                let mut site_options = vec![ColonyBuildingSite::Orbital];
                if let Some(system_detail) = detail.as_ref() {
                    for (star_index, _) in system_detail.stars.iter().enumerate() {
                        site_options.push(ColonyBuildingSite::Star(star_index as u16));
                    }
                    for (planet_index, _) in system_detail.planets.iter().enumerate() {
                        site_options.push(ColonyBuildingSite::Planet(planet_index as u16));
                    }
                }

                let host_site = ColonyBuildingSite::host_for_body_index(selected_colony.body_index);
                if !site_options.contains(&host_site) {
                    site_options.push(host_site);
                }
                site_options.sort_by_key(|site| match *site {
                    ColonyBuildingSite::Orbital => (0_u8, 0_u16),
                    ColonyBuildingSite::Star(index) => (1_u8, index),
                    ColonyBuildingSite::Planet(index) => (2_u8, index),
                });
                site_options.dedup();

                let mut selected_site = self
                    .colony_build_site_selection
                    .get(&selected_colony_id)
                    .copied()
                    .unwrap_or(host_site);
                if !site_options.contains(&selected_site) {
                    selected_site = host_site;
                }

                ui.horizontal(|ui| {
                    ui.label("Site:");
                    egui::ComboBox::from_id_source("construction_site_select")
                        .selected_text(selected_site.label())
                        .show_ui(ui, |ui| {
                            for site in &site_options {
                                ui.selectable_value(&mut selected_site, *site, site.label());
                            }
                        });
                });
                self.colony_build_site_selection
                    .insert(selected_colony_id, selected_site);

                let site_profile = Self::colony_build_site_profile(detail.as_ref(), selected_site);
                let slot_capacity = GameState::building_site_slot_capacity(selected_site, site_profile);
                let occupied_slots = selected_colony.occupied_building_slots_at_site(selected_site);

                ui.small(Self::colony_build_site_context_label(
                    detail.as_ref(),
                    selected_site,
                ));
                let faction_treasury = self
                    .game_state
                    .factions
                    .get(&self.game_state.player.faction_id)
                    .map(|faction| faction.treasury)
                    .unwrap_or(0);
                ui.small(format!(
                    "Faction treasury {} | Stockpiles F {:.1} I {:.1} E {:.1}",
                    faction_treasury,
                    selected_colony.food_stockpile,
                    selected_colony.industry_stockpile,
                    selected_colony.energy_stockpile,
                ));

                let pending_project = self
                    .game_state
                    .pending_colony_building_for_colony(selected_colony_id)
                    .cloned();
                if let Some(pending) = pending_project.as_ref() {
                    let eta = (pending.complete_year - self.game_state.current_year).max(0.0);
                    ui.small(format!(
                        "Active project: {} @ {} to L{} ({:.2}y remaining)",
                        pending.kind.label(),
                        pending.site.label(),
                        pending.target_level,
                        eta,
                    ));
                }

                ui.separator();
                egui::ScrollArea::vertical()
                    .max_height(320.0)
                    .show(ui, |ui| {
                        egui::Grid::new("construction_building_grid")
                            .num_columns(4)
                            .striped(true)
                            .spacing([10.0, 4.0])
                            .show(ui, |ui| {
                                ui.label(egui::RichText::new("Building").underline());
                                ui.label(egui::RichText::new("Level").underline());
                                ui.label(egui::RichText::new("Cost").underline());
                                ui.label(egui::RichText::new("").underline());
                                ui.end_row();

                                let available_buildings: Vec<_> = ColonyBuildingKind::all()
                                    .into_iter()
                                    .filter(|kind| kind.is_player_queueable())
                                    .filter(|kind| {
                                        GameState::building_site_support_error(*kind, selected_site, site_profile).is_none()
                                    })
                                    .collect();

                                if available_buildings.is_empty() {
                                    ui.label("No buildings available at this site.");
                                    ui.end_row();
                                }

                                for kind in available_buildings {
                                    let current_level =
                                        selected_colony.building_level_at_site(kind, selected_site);
                                    let at_max_level = current_level >= kind.max_level();
                                    let target_level = current_level.saturating_add(1);
                                    let cost_preview = GameState::colony_building_cost_preview(
                                        kind,
                                        target_level,
                                    );

                                    let mut queue_issues = Vec::new();
                                    if at_max_level {
                                        queue_issues.push(
                                            "This building has reached its maximum level."
                                                .to_owned(),
                                        );
                                    }
                                    if kind.consumes_site_slot() && current_level == 0 {
                                        if let Some(capacity) = slot_capacity {
                                            if occupied_slots >= capacity {
                                                queue_issues.push(
                                                    "No free building slots remain on this planet."
                                                        .to_owned(),
                                                );
                                            }
                                        }
                                    }
                                    if pending_project.is_some() {
                                        queue_issues.push(
                                            "Another building is already under construction for this colony."
                                                .to_owned(),
                                        );
                                    }

                                    if faction_treasury < cost_preview.treasury {
                                        queue_issues.push(format!(
                                            "Need {} more treasury.",
                                            cost_preview.treasury - faction_treasury
                                        ));
                                    }
                                    if selected_colony.food_stockpile + 0.0001 < cost_preview.food {
                                        queue_issues.push(format!(
                                            "Need {:.1} more food stockpile.",
                                            cost_preview.food - selected_colony.food_stockpile
                                        ));
                                    }
                                    if selected_colony.industry_stockpile + 0.0001
                                        < cost_preview.industry
                                    {
                                        queue_issues.push(format!(
                                            "Need {:.1} more industry stockpile.",
                                            cost_preview.industry - selected_colony.industry_stockpile
                                        ));
                                    }
                                    if selected_colony.energy_stockpile + 0.0001 < cost_preview.energy {
                                        queue_issues.push(format!(
                                            "Need {:.1} more energy stockpile.",
                                            cost_preview.energy - selected_colony.energy_stockpile
                                        ));
                                    }
                                    for (symbol, amount) in &cost_preview.element_costs {
                                        let available = selected_colony
                                            .element_stockpiles
                                            .get(symbol)
                                            .copied()
                                            .unwrap_or(0.0);
                                        if available + 0.0001 < *amount {
                                            queue_issues.push(format!(
                                                "Need {:.1} more {}.",
                                                amount - available,
                                                symbol
                                            ));
                                        }
                                    }

                                    let can_queue = queue_issues.is_empty();

                                    let building_label = ui.label(kind.label());
                                    building_label.on_hover_text(format!(
                                        "{}\n\n{}",
                                        kind.role_description(),
                                        Self::colony_building_effect_summary(kind),
                                    ));

                                    if at_max_level {
                                        ui.label(format!("L{} (max)", current_level));
                                    } else {
                                        ui.label(format!("L{} → L{}", current_level, cost_preview.target_level));
                                    }

                                    ui.small(Self::colony_building_cost_summary(&cost_preview));

                                    ui.vertical(|ui| {
                                        if !at_max_level {
                                            if ui
                                                .add_enabled(
                                                    can_queue,
                                                    egui::Button::new(kind.queue_button_label()),
                                                )
                                                .clicked()
                                            {
                                                building_queue_requests.push((
                                                    selected_colony_id,
                                                    kind,
                                                    selected_site,
                                                    site_profile,
                                                ));
                                            }
                                        }
                                        for issue in queue_issues.iter().take(2) {
                                            ui.small(issue);
                                        }
                                        if queue_issues.len() > 2 {
                                            ui.small(format!(
                                                "{} additional requirement(s)",
                                                queue_issues.len() - 2
                                            ));
                                        }
                                    });
                                    ui.end_row();
                                }
                            });
                    });
            });

        self.construction_window_open = open;

        let mut building_queue_count = 0usize;
        let mut first_queue_error: Option<String> = None;
        for (colony_id, kind, site, site_profile) in building_queue_requests {
            match self.game_state.queue_colony_building_with_profile(
                self.game_state.current_year,
                colony_id,
                kind,
                site,
                site_profile,
            ) {
                Ok((_duration_years, _construction_cost, _target_level)) => {
                    building_queue_count += 1;
                }
                Err(message) => {
                    if first_queue_error.is_none() {
                        first_queue_error = Some(message.to_owned());
                    }
                }
            }
        }

        if building_queue_count > 0 {
            self.game_autosave_accum_years = 0.0;
            self.persist_game_state();
            self.game_notice = Some(format!(
                "Queued {} colony building project(s).",
                building_queue_count
            ));
        } else if let Some(error) = first_queue_error {
            self.game_notice = Some(error);
        }
    }

    fn smooth_ms(previous: f32, sample: f32) -> f32 {
        if previous <= 0.0 {
            sample
        } else {
            previous * 0.85 + sample * 0.15
        }
    }

    fn luminosity_visual_radius_multiplier(class: LuminosityClass) -> f32 {
        class.visual_radius_multiplier()
    }

    fn sagittarius_a_system_id() -> SystemId {
        SystemId {
            sector: SectorCoord { x: 0, y: 0 },
            local_index: u32::MAX,
        }
    }

    fn sagittarius_a_system_detail(&self) -> SystemDetail {
        let mut detail = SystemDetail {
            id: Self::sagittarius_a_system_id(),
            canonical_name: SAGITTARIUS_A_NAME.to_owned(),
            display_name: SAGITTARIUS_A_NAME.to_owned(),
            pos: galaxy_center(),
            represented_systems: 1,
            stars: vec![StarBody {
                class: StellarClassification::new(
                    SpectralClass::BH,
                    0,
                    LuminosityClass::VII,
                ),
                mass_solar: 4_300_000.0,
                luminosity_solar: 0.000_03,
            }],
            planets: Vec::new(),
            explored: false,
            favorite: false,
            note: Some("Supermassive black hole at the galactic center.".to_owned()),
        };

        self.delta_store.apply_to_detail(&mut detail);
        detail.canonical_name = SAGITTARIUS_A_NAME.to_owned();
        detail.display_name = SAGITTARIUS_A_NAME.to_owned();
        detail
    }

    fn star_visual_color(class: SpectralClass) -> egui::Color32 {
        let c = class.visual_color_rgb();
        egui::Color32::from_rgb(c[0], c[1], c[2])
    }

    fn star_visual_radius_px(star: &StarBody) -> f32 {
        if star.class.spectral == SpectralClass::BH {
            let mass_log = star.mass_solar.max(1.0).log10().clamp(0.0, 7.0);
            return (8.0 + mass_log * 3.0).clamp(8.0, 34.0);
        }
        if star.class.spectral == SpectralClass::NS {
            let mass_term = ((star.mass_solar - 1.0) / 1.3).clamp(0.0, 1.0);
            return (2.0 + mass_term * 1.8).clamp(2.0, 4.4);
        }

        let mass = star.mass_solar.max(0.005);
        let lum = star.luminosity_solar.max(0.000_000_1);
        let mass_term = mass.log10().clamp(-2.0, 2.0) + 2.0;
        let lum_term = lum.log10().clamp(-6.0, 6.0) + 6.0;
        let base = 2.4 + mass_term * 1.55 + lum_term * 0.28;
        let class_scale = Self::luminosity_visual_radius_multiplier(star.class.luminosity);
        (base * class_scale).clamp(2.0, 34.0)
    }

    fn lerp_color(a: egui::Color32, b: egui::Color32, t: f32) -> egui::Color32 {
        let t = t.clamp(0.0, 1.0);
        let lerp = |x: u8, y: u8| -> u8 {
            (x as f32 + (y as f32 - x as f32) * t).round() as u8
        };
        egui::Color32::from_rgb(lerp(a.r(), b.r()), lerp(a.g(), b.g()), lerp(a.b(), b.b()))
    }

    fn planet_visual_color(kind: PlanetKind, temperature_k: f32, habitable: bool) -> egui::Color32 {
        let t = ((temperature_k - 40.0) / 900.0).clamp(0.0, 1.0);
        let def = kind.definition();
        let cold = egui::Color32::from_rgb(def.visual_color_cold[0], def.visual_color_cold[1], def.visual_color_cold[2]);
        let hot = egui::Color32::from_rgb(def.visual_color_hot[0], def.visual_color_hot[1], def.visual_color_hot[2]);
        let base = Self::lerp_color(cold, hot, t);

        if habitable {
            Self::lerp_color(base, egui::Color32::from_rgb(102, 190, 110), 0.45)
        } else {
            base
        }
    }

    fn planet_visual_radius_px(
        kind: PlanetKind,
        temperature_k: f32,
        habitable: bool,
        radius_earth: f32,
        is_moon: bool,
    ) -> f32 {
        let kind_base = kind.definition().base_visual_radius_px;
        let size_term = radius_earth.max(0.03).powf(0.42) * 2.35;
        let temp_boost = ((temperature_k - 60.0) / 900.0).clamp(0.0, 1.0) * 0.65;
        let habitability_boost = if kind == PlanetKind::EarthLikeWorld {
            0.70
        } else if habitable {
            0.45
        } else {
            0.0
        };
        let moon_scale = if is_moon { 0.84 } else { 1.0 };
        ((kind_base + size_term + temp_boost + habitability_boost) * moon_scale).clamp(1.3, 12.5)
    }

    fn atmosphere_visual_color(
        atmosphere: &[PlanetAtmosphereComponent],
        habitable: bool,
    ) -> egui::Color32 {
        if habitable {
            return egui::Color32::from_rgb(120, 206, 152);
        }

        let dominant_formula = atmosphere
            .iter()
            .max_by(|a, b| {
                a.percent
                    .partial_cmp(&b.percent)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|gas| gas.formula.as_str())
            .unwrap_or("N2");

        match dominant_formula {
            "H2" => egui::Color32::from_rgb(148, 186, 255),
            "He" => egui::Color32::from_rgb(206, 192, 255),
            "N2" => egui::Color32::from_rgb(150, 178, 210),
            "O2" => egui::Color32::from_rgb(128, 208, 186),
            "CO2" => egui::Color32::from_rgb(208, 160, 138),
            "CH4" => egui::Color32::from_rgb(170, 204, 140),
            "NH3" => egui::Color32::from_rgb(206, 188, 144),
            "H2O" => egui::Color32::from_rgb(124, 188, 228),
            "SO2" => egui::Color32::from_rgb(236, 178, 114),
            "Ne" | "Ar" | "Kr" | "Xe" => egui::Color32::from_rgb(188, 198, 220),
            _ => egui::Color32::from_rgb(170, 196, 226),
        }
    }

    fn atmosphere_halo_factor(pressure_atm: f32) -> f32 {
        if pressure_atm <= 0.0 {
            return 0.0;
        }

        let normalized = ((pressure_atm + 0.05).ln() / 5.2).clamp(0.0, 1.0);
        0.10 + normalized * 0.52
    }

    fn stable_orbit_phase(&self, system: &SystemDetail, body_index: usize) -> f32 {
        let mut h = self.galaxy_seed
            ^ (system.id.sector.x as i64 as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (system.id.sector.y as i64 as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
            ^ (system.id.local_index as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93)
            ^ (body_index as u64).wrapping_mul(0xA24B_AED4_963E_E407);
        h ^= h >> 30;
        h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h ^= h >> 27;
        h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^= h >> 31;

        let phase_01 = ((h >> 40) as u32) as f32 / ((1u32 << 24) - 1) as f32;
        phase_01 * 2.0 * PI
    }

    fn reset_travel_view(&mut self) {
        self.travel_view_zoom = TRAVEL_VIEW_ZOOM_DEFAULT;
        self.travel_view_pan = egui::Vec2::ZERO;
        self.travel_view_dragging = false;
        self.travel_view_last_mouse_pos = egui::Pos2::ZERO;
    }

    fn apply_travel_view_zoom(
        &mut self,
        previous_zoom: f32,
        new_zoom: f32,
        focus_local: egui::Vec2,
    ) {
        let old_zoom = previous_zoom.clamp(TRAVEL_VIEW_ZOOM_MIN, TRAVEL_VIEW_ZOOM_MAX);
        let next_zoom = new_zoom.clamp(TRAVEL_VIEW_ZOOM_MIN, TRAVEL_VIEW_ZOOM_MAX);

        if !old_zoom.is_finite() || old_zoom <= 0.0 {
            self.travel_view_zoom = next_zoom;
            return;
        }

        if (next_zoom - old_zoom).abs() <= f32::EPSILON {
            self.travel_view_zoom = next_zoom;
            return;
        }

        let focus_scene = (focus_local - self.travel_view_pan) / old_zoom;
        self.travel_view_pan = focus_local - focus_scene * next_zoom;
        self.travel_view_zoom = next_zoom;
    }

    fn show_travel_window(&mut self, ctx: &egui::Context) {
        if !self.travel_window_open {
            self.travel_last_input_time = None;
            self.travel_selected_body = None;
            self.travel_composition_info_open = false;
            self.travel_atmosphere_info_open = false;
            self.travel_view_dragging = false;
            return;
        }

        let now = ctx.input(|i| i.time);
        if let Some(last) = self.travel_last_input_time {
            let dt = (now - last) as f32;
            if dt.is_finite() && dt > 0.0 && !self.travel_paused {
                self.travel_sim_years += dt * self.travel_time_scale.max(0.0);
            }
        }
        self.travel_last_input_time = Some(now);

        let Some(detail) = self.travel_system.clone() else {
            self.travel_window_open = false;
            self.travel_last_input_time = None;
            self.travel_selected_body = None;
            self.travel_composition_info_open = false;
            self.travel_atmosphere_info_open = false;
            self.travel_view_dragging = false;
            return;
        };

        let mut open = self.travel_window_open;
        let mut close_requested = false;

        egui::Window::new(format!("System Top View - {}", detail.display_name))
            .open(&mut open)
            .resizable(true)
            .default_size([920.0, 650.0])
            .show(ctx, |ui| {
                let slider_zoom_before = self.travel_view_zoom;
                ui.horizontal(|ui| {
                    ui.checkbox(&mut self.travel_paused, "Pause");
                    ui.add(
                        egui::Slider::new(&mut self.travel_time_scale, 0.02..=8.0)
                            .text("Years / sec")
                            .logarithmic(true),
                    );
                    ui.add(
                        egui::Slider::new(
                            &mut self.travel_view_zoom,
                            TRAVEL_VIEW_ZOOM_MIN..=TRAVEL_VIEW_ZOOM_MAX,
                        )
                        .text("View zoom")
                        .logarithmic(true),
                    );
                    if ui.button("Reset time").clicked() {
                        self.travel_sim_years = 0.0;
                    }
                    if ui.button("Reset view").clicked() {
                        self.reset_travel_view();
                    }
                    if ui.button("Exit travel").clicked() {
                        close_requested = true;
                    }
                });

                if (self.travel_view_zoom - slider_zoom_before).abs() > f32::EPSILON {
                    self.apply_travel_view_zoom(
                        slider_zoom_before,
                        self.travel_view_zoom,
                        egui::Vec2::ZERO,
                    );
                } else {
                    self.travel_view_zoom = self
                        .travel_view_zoom
                        .clamp(TRAVEL_VIEW_ZOOM_MIN, TRAVEL_VIEW_ZOOM_MAX);
                }

                ui.label(format!(
                    "Simulated time: {:.2} years | Stars: {} | Planets: {}",
                    self.travel_sim_years,
                    detail.stars.len(),
                    detail.planets.len(),
                ));
                ui.label("Scroll or use the zoom slider to zoom, drag to pan, click a body to inspect it.");

                if let Some(selection) = self.travel_selected_body {
                    match selection {
                        TravelBodySelection::Star(star_index) => {
                            self.travel_composition_info_open = false;
                            self.travel_atmosphere_info_open = false;
                            if let Some(star) = detail.stars.get(star_index) {
                                ui.label(format!(
                                    "Selected: Star S{} | Class {} | Mass {:.2} M☉ | Lum {:.2} L☉",
                                    star_index + 1,
                                    star.class.notation(),
                                    star.mass_solar,
                                    star.luminosity_solar,
                                ));
                            } else {
                                self.travel_selected_body = None;
                            }
                        }
                        TravelBodySelection::Planet(planet_index) => {
                            if let Some(planet) = detail.planets.get(planet_index) {
                                let orbit_desc = if let Some(host_index) = planet.host_planet_index {
                                    format!(
                                        "Moon of P{} | Moon orbit {:.4} AU",
                                        host_index as usize + 1,
                                        planet.moon_orbit_au.unwrap_or(0.0),
                                    )
                                } else {
                                    format!("Primary orbit {:.3} AU around star", planet.orbit_au)
                                };
                                let atmosphere_desc = if planet.atmosphere.is_empty() {
                                    " | No atmosphere".to_owned()
                                } else {
                                    format!(" | Atm {:.2} atm", planet.atmosphere_pressure_atm.max(0.0))
                                };
                                ui.label(format!(
                                    "Selected: P{} ({}) | {} | Radius {:.2} R⊕ | Mass {:.2} M⊕ | Temp {:.0} K{}{}",
                                    planet_index + 1,
                                    planet.kind.label(),
                                    orbit_desc,
                                    planet.radius_earth,
                                    planet.mass_earth,
                                    planet.temperature_k,
                                    if planet.kind == PlanetKind::EarthLikeWorld {
                                        " | ELW"
                                    } else if planet.habitable {
                                        " | Habitable"
                                    } else {
                                        ""
                                    },
                                    atmosphere_desc,
                                ));
                                ui.horizontal(|ui| {
                                    if ui.button("Composition info").clicked() {
                                        self.travel_composition_info_open = true;
                                    }
                                    if ui
                                        .add_enabled(
                                            !planet.atmosphere.is_empty(),
                                            egui::Button::new("Atmosphere info"),
                                        )
                                        .clicked()
                                    {
                                        self.travel_atmosphere_info_open = true;
                                    }
                                });
                                if planet.atmosphere.is_empty() {
                                    self.travel_atmosphere_info_open = false;
                                }
                            } else {
                                self.travel_selected_body = None;
                                self.travel_composition_info_open = false;
                                self.travel_atmosphere_info_open = false;
                            }
                        }
                    }
                } else {
                    self.travel_composition_info_open = false;
                    self.travel_atmosphere_info_open = false;
                    ui.label("Click a star or planet to inspect details.");
                }

                let available = ui.available_size_before_wrap();
                let desired = egui::Vec2::new(available.x.max(420.0), available.y.max(320.0));
                let (response, painter) =
                    ui.allocate_painter(desired, egui::Sense::click_and_drag());
                let rect = response.rect;

                if response.hovered() {
                    let scroll = ctx.input(|i| i.raw_scroll_delta.y);
                    if scroll != 0.0 {
                        let focus_local = ctx
                            .input(|i| i.pointer.hover_pos())
                            .filter(|pos| rect.contains(*pos))
                            .map(|pos| pos - rect.center())
                            .unwrap_or(egui::Vec2::ZERO);
                        let next_zoom = self.travel_view_zoom * 1.1_f32.powf(scroll / 20.0);
                        self.apply_travel_view_zoom(
                            self.travel_view_zoom,
                            next_zoom,
                            focus_local,
                        );
                    }
                }

                if response.drag_started_by(egui::PointerButton::Primary) {
                    self.travel_view_dragging = true;
                    if let Some(mouse_pos) = ctx.pointer_latest_pos() {
                        self.travel_view_last_mouse_pos = mouse_pos;
                    }
                }
                if self.travel_view_dragging && ctx.input(|i| i.pointer.primary_down()) {
                    if let Some(mouse_pos) = ctx.pointer_latest_pos() {
                        let delta = mouse_pos - self.travel_view_last_mouse_pos;
                        self.travel_view_pan += delta;
                        self.travel_view_last_mouse_pos = mouse_pos;
                    }
                }
                if response.drag_stopped_by(egui::PointerButton::Primary) {
                    self.travel_view_dragging = false;
                }

                painter.rect_filled(rect, 10.0, egui::Color32::from_rgb(4, 8, 18));
                let mut hit_targets = Vec::<(TravelBodySelection, egui::Pos2, f32)>::new();

                let scene_center = rect.center() + self.travel_view_pan;
                let max_draw_radius = (rect.width().min(rect.height()) * 0.5 - 46.0).max(48.0);
                for ring_factor in [0.25_f32, 0.5, 0.75, 1.0] {
                    let ring_alpha = (20.0 + ring_factor * 18.0) as u8;
                    let ring_radius = max_draw_radius * ring_factor * self.travel_view_zoom;
                    if ring_radius > 1.0 {
                        painter.circle_stroke(
                            scene_center,
                            ring_radius,
                            egui::Stroke::new(
                                1.0,
                                egui::Color32::from_rgba_unmultiplied(90, 120, 170, ring_alpha),
                            ),
                        );
                    }
                }

                let total_mass = detail
                    .stars
                    .iter()
                    .map(|star| star.mass_solar.max(0.01))
                    .sum::<f32>()
                    .max(0.08);

                let star_count = detail.stars.len();
                let mut star_draw_radii = detail
                    .stars
                    .iter()
                    .map(Self::star_visual_radius_px)
                    .collect::<Vec<_>>();

                // Keep stars legible but avoid giant overlap at small canvas sizes.
                let draw_scale = (max_draw_radius / 140.0).clamp(0.58, 1.0);
                for radius in &mut star_draw_radii {
                    *radius = (*radius * draw_scale).clamp(2.0, 24.0);
                }

                let mut star_orbit_radii = vec![0.0f32; star_count];
                let mut star_positions = vec![egui::Vec2::ZERO; star_count];

                if star_count == 1 {
                    star_positions[0] = egui::Vec2::ZERO;
                } else if star_count == 2 {
                    // Binary systems: opposite sides of barycenter to prevent overlap/crossing.
                    let m0 = detail.stars[0].mass_solar.max(0.05);
                    let m1 = detail.stars[1].mass_solar.max(0.05);
                    let total = (m0 + m1).max(0.1);
                    let min_separation = star_draw_radii[0] + star_draw_radii[1] + 12.0;
                    let separation = (max_draw_radius * 0.30)
                        .clamp(24.0, max_draw_radius * 0.72)
                        .max(min_separation);

                    let d0 = (separation * (m1 / total)).max(star_draw_radii[0] + 6.0);
                    let d1 = (separation * (m0 / total)).max(star_draw_radii[1] + 6.0);
                    star_orbit_radii[0] = d0;
                    star_orbit_radii[1] = d1;

                    let phase = self.stable_orbit_phase(&detail, 10_000);
                    let period_years = (2.2 + separation * 0.07).max(1.0);
                    let angle = (self.travel_sim_years / period_years) * (2.0 * PI) + phase;
                    let dir = egui::Vec2::new(angle.cos(), angle.sin());
                    star_positions[0] = dir * d0;
                    star_positions[1] = dir * -d1;
                } else if star_count > 2 {
                    // N-body approximation: nested non-intersecting rings around barycenter.
                    let mut current_orbit = (max_draw_radius * 0.08).clamp(14.0, 36.0);
                    for idx in 0..star_count {
                        if idx == 0 {
                            current_orbit = current_orbit.max(star_draw_radii[idx] + 10.0);
                        } else {
                            current_orbit +=
                                star_draw_radii[idx - 1] + star_draw_radii[idx] + 10.0;
                        }
                        star_orbit_radii[idx] = current_orbit;
                    }

                    let max_allowed_orbit = (max_draw_radius * 0.42).max(34.0);
                    if let Some(&outermost) = star_orbit_radii.last() {
                        if outermost > max_allowed_orbit && star_count > 1 {
                            let first = star_orbit_radii[0];
                            let src_span = (outermost - first).max(1.0);
                            let dst_span = (max_allowed_orbit - first).max(1.0);
                            let compression = (dst_span / src_span).clamp(0.25, 1.0);

                            for orbit in star_orbit_radii.iter_mut().skip(1) {
                                *orbit = first + (*orbit - first) * compression;
                            }

                            let radius_scale = compression.clamp(0.68, 1.0);
                            for radius in &mut star_draw_radii {
                                *radius = (*radius * radius_scale).clamp(1.6, 24.0);
                            }

                            for idx in 1..star_count {
                                let min_gap =
                                    star_draw_radii[idx - 1] + star_draw_radii[idx] + 8.0;
                                let gap = star_orbit_radii[idx] - star_orbit_radii[idx - 1];
                                if gap < min_gap {
                                    star_orbit_radii[idx] = star_orbit_radii[idx - 1] + min_gap;
                                }
                            }
                        }
                    }

                    for idx in 0..star_count {
                        let phase = self.stable_orbit_phase(&detail, 10_000 + idx)
                            + idx as f32 * (2.0 * PI / star_count as f32);
                        let period_years =
                            (4.0 + star_orbit_radii[idx] * 0.09 + idx as f32 * 1.8).max(1.0);
                        let angle = (self.travel_sim_years / period_years) * (2.0 * PI) + phase;
                        star_positions[idx] =
                            egui::Vec2::new(angle.cos(), angle.sin()) * star_orbit_radii[idx];
                    }
                }

                for orbit_radius in star_orbit_radii.iter().copied() {
                    let orbit_radius_screen = orbit_radius * self.travel_view_zoom;
                    if orbit_radius_screen > 1.0 {
                        painter.circle_stroke(
                            scene_center,
                            orbit_radius_screen,
                            egui::Stroke::new(
                                1.0,
                                egui::Color32::from_rgba_unmultiplied(120, 170, 220, 48),
                            ),
                        );
                    }
                }

                for (idx, star) in detail.stars.iter().enumerate() {
                    let scene_pos = star_positions.get(idx).copied().unwrap_or(egui::Vec2::ZERO);
                    let pos = scene_center + scene_pos * self.travel_view_zoom;
                    let radius = (star_draw_radii.get(idx).copied().unwrap_or(3.0)
                        * self.travel_view_zoom)
                        .max(1.4);
                    let color = Self::star_visual_color(star.class.spectral);
                    painter.circle_filled(
                        pos,
                        radius * 2.7,
                        egui::Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 26),
                    );
                    painter.circle_filled(pos, radius, color);
                    painter.circle_stroke(
                        pos,
                        radius,
                        egui::Stroke::new(
                            1.0,
                            egui::Color32::from_rgba_unmultiplied(255, 255, 255, 92),
                        ),
                    );
                    hit_targets.push((TravelBodySelection::Star(idx), pos, radius + 5.0));
                }

                if detail.planets.is_empty() {
                    painter.text(
                        scene_center
                            + egui::Vec2::new(
                                0.0,
                                max_draw_radius * 0.55 * self.travel_view_zoom,
                            ),
                        egui::Align2::CENTER_CENTER,
                        "No planets in this system",
                        egui::FontId::proportional(13.0),
                        egui::Color32::from_rgb(170, 190, 210),
                    );
                } else {
                    let planet_draw_radii = detail
                        .planets
                        .iter()
                        .map(|planet| {
                            Self::planet_visual_radius_px(
                                planet.kind,
                                planet.temperature_k,
                                planet.habitable,
                                planet.radius_earth,
                                planet.host_planet_index.is_some(),
                            )
                        })
                        .collect::<Vec<_>>();
                    let max_planet_radius = planet_draw_radii
                        .iter()
                        .copied()
                        .fold(2.0f32, f32::max);

                    // Keep all planet orbits outside the maximum star sweep radius
                    // (including star glow), with a little visual clearance.
                    let star_exclusion_radius = star_orbit_radii
                        .iter()
                        .copied()
                        .zip(star_draw_radii.iter().copied())
                        .map(|(orbit_r, star_r)| orbit_r + star_r * 2.7)
                        .fold(0.0f32, f32::max);
                    let min_planet_orbit_radius =
                        (star_exclusion_radius + max_planet_radius + 10.0).max(14.0);

                    let max_primary_orbit_au = detail
                        .planets
                        .iter()
                        .filter(|planet| planet.host_planet_index.is_none())
                        .map(|planet| planet.orbit_au)
                        .fold(0.35f32, f32::max)
                        .max(0.35);
                    let orbit_scale = max_draw_radius / max_primary_orbit_au.max(0.05);

                    let mut planet_orbit_radii = vec![0.0f32; detail.planets.len()];
                    let mut planet_positions = vec![egui::Vec2::ZERO; detail.planets.len()];

                    let mut primary_indices = detail
                        .planets
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, planet)| {
                            if planet.host_planet_index.is_none() {
                                Some(idx)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    if primary_indices.is_empty() {
                        primary_indices.extend(0..detail.planets.len());
                    }

                    for (primary_order, planet_index) in
                        primary_indices.iter().copied().enumerate()
                    {
                        let planet = &detail.planets[planet_index];
                        let mut orbit_radius =
                            (planet.orbit_au * orbit_scale).max(min_planet_orbit_radius);

                        if primary_order > 0 {
                            let prev_index = primary_indices[primary_order - 1];
                            let min_gap =
                                planet_draw_radii[prev_index] + planet_draw_radii[planet_index] + 7.0;
                            let min_orbit = planet_orbit_radii[prev_index] + min_gap;
                            if orbit_radius < min_orbit {
                                orbit_radius = min_orbit;
                            }
                        }

                        planet_orbit_radii[planet_index] = orbit_radius;
                    }

                    for planet_index in primary_indices.iter().copied() {
                        let orbit_radius =
                            planet_orbit_radii[planet_index].max(min_planet_orbit_radius);
                        let orbit_radius_screen = orbit_radius * self.travel_view_zoom;
                        if orbit_radius_screen > 1.0 {
                            painter.circle_stroke(
                                scene_center,
                                orbit_radius_screen,
                                egui::Stroke::new(
                                    1.0,
                                    egui::Color32::from_rgba_unmultiplied(115, 162, 212, 70),
                                ),
                            );
                        }
                    }

                    for planet_index in primary_indices.iter().copied() {
                        let planet = &detail.planets[planet_index];
                        let orbit_radius =
                            planet_orbit_radii[planet_index].max(min_planet_orbit_radius);
                        let period_years = (planet.orbit_au.max(0.03).powf(3.0) / total_mass)
                            .sqrt()
                            .max(0.04);
                        let angle =
                            (self.travel_sim_years / period_years) * (2.0 * PI)
                                + self.stable_orbit_phase(&detail, 100_000 + planet_index);

                        planet_positions[planet_index] = egui::Vec2::new(
                            angle.cos() * orbit_radius,
                            angle.sin() * orbit_radius,
                        );
                    }

                    let mut host_moon_last_radius = HashMap::<usize, f32>::new();
                    for moon_index in 0..detail.planets.len() {
                        let moon = &detail.planets[moon_index];
                        let Some(host_u8) = moon.host_planet_index else {
                            continue;
                        };
                        let host_index = host_u8 as usize;
                        if host_index >= detail.planets.len() {
                            continue;
                        }

                        let host_pos = planet_positions[host_index];
                        let host_orbit_radius = planet_orbit_radii[host_index].max(min_planet_orbit_radius);
                        let raw_moon_orbit_au = moon.moon_orbit_au.unwrap_or(0.008).max(0.0004);
                        let mut moon_orbit_radius =
                            (raw_moon_orbit_au * orbit_scale * 8.5).max(
                                planet_draw_radii[host_index] + planet_draw_radii[moon_index] + 3.0,
                            );

                        if let Some(last_radius) = host_moon_last_radius.get(&host_index).copied() {
                            let min_gap = planet_draw_radii[moon_index] + 5.0;
                            moon_orbit_radius = moon_orbit_radius.max(last_radius + min_gap);
                        }

                        let inward_limit = host_orbit_radius
                            - (star_exclusion_radius + planet_draw_radii[moon_index] + 4.0);
                        if inward_limit.is_finite() && inward_limit > 1.0 {
                            moon_orbit_radius = moon_orbit_radius.min(inward_limit);
                        }

                        moon_orbit_radius =
                            moon_orbit_radius.min((host_orbit_radius * 0.36).max(6.0));
                        let moon_orbit_radius_screen = moon_orbit_radius * self.travel_view_zoom;
                        if moon_orbit_radius_screen > 1.0 {
                            painter.circle_stroke(
                                scene_center + host_pos * self.travel_view_zoom,
                                moon_orbit_radius_screen,
                                egui::Stroke::new(
                                    1.0,
                                    egui::Color32::from_rgba_unmultiplied(92, 146, 204, 68),
                                ),
                            );
                        }

                        let host_mass_solar =
                            (detail.planets[host_index].mass_earth / 332_946.0).max(0.000_001);
                        let period_years = (raw_moon_orbit_au.powf(3.0) / host_mass_solar)
                            .sqrt()
                            .max(0.0025);
                        let angle =
                            (self.travel_sim_years / period_years) * (2.0 * PI)
                                + self.stable_orbit_phase(&detail, 200_000 + moon_index);

                        planet_positions[moon_index] = host_pos
                            + egui::Vec2::new(
                                angle.cos() * moon_orbit_radius,
                                angle.sin() * moon_orbit_radius,
                            );
                        host_moon_last_radius.insert(host_index, moon_orbit_radius);
                    }

                    for (planet_index, planet) in detail.planets.iter().enumerate() {
                        let scene_pos = planet_positions[planet_index];
                        let pos = scene_center + scene_pos * self.travel_view_zoom;
                        let radius =
                            (planet_draw_radii.get(planet_index).copied().unwrap_or(2.2)
                                * self.travel_view_zoom)
                                .max(1.2);
                        let color =
                            Self::planet_visual_color(planet.kind, planet.temperature_k, planet.habitable);
                        if !planet.atmosphere.is_empty() && planet.atmosphere_pressure_atm > 0.0 {
                            let halo_factor = Self::atmosphere_halo_factor(planet.atmosphere_pressure_atm);
                            let halo_radius = radius * (1.0 + halo_factor);
                            let halo_color = Self::atmosphere_visual_color(&planet.atmosphere, planet.habitable);
                            painter.circle_filled(
                                pos,
                                halo_radius,
                                egui::Color32::from_rgba_unmultiplied(
                                    halo_color.r(),
                                    halo_color.g(),
                                    halo_color.b(),
                                    24,
                                ),
                            );
                            painter.circle_stroke(
                                pos,
                                halo_radius,
                                egui::Stroke::new(
                                    (0.8 + halo_factor).clamp(0.8, 2.0),
                                    egui::Color32::from_rgba_unmultiplied(
                                        halo_color.r(),
                                        halo_color.g(),
                                        halo_color.b(),
                                        112,
                                    ),
                                ),
                            );
                        }

                        painter.circle_filled(pos, radius, color);
                        painter.circle_stroke(
                            pos,
                            radius,
                            egui::Stroke::new(
                                1.0,
                                egui::Color32::from_rgba_unmultiplied(255, 255, 255, 88),
                            ),
                        );
                        hit_targets.push((
                            TravelBodySelection::Planet(planet_index),
                            pos,
                            radius + 4.0,
                        ));
                    }
                }

                if response.clicked_by(egui::PointerButton::Primary) {
                    if let Some(click_pos) = ui.ctx().input(|i| i.pointer.interact_pos()) {
                        let mut closest = None;
                        for (selection, pos, hit_radius) in &hit_targets {
                            let dx = pos.x - click_pos.x;
                            let dy = pos.y - click_pos.y;
                            let dist_sq = dx * dx + dy * dy;
                            let hit_sq = hit_radius * hit_radius;
                            if dist_sq <= hit_sq {
                                if closest.map_or(true, |(best_dist_sq, _)| dist_sq < best_dist_sq) {
                                    closest = Some((dist_sq, *selection));
                                }
                            }
                        }
                        self.travel_selected_body = closest.map(|(_, selection)| selection);
                    }
                }

                if let Some(selection) = self.travel_selected_body {
                    if let Some((_, pos, hit_radius)) =
                        hit_targets.iter().find(|(candidate, _, _)| *candidate == selection)
                    {
                        let ring = (hit_radius + 2.0).max(8.0);
                        painter.circle_stroke(
                            *pos,
                            ring,
                            egui::Stroke::new(1.8, egui::Color32::from_rgb(92, 220, 255)),
                        );
                        painter.circle_stroke(
                            *pos,
                            ring + 3.0,
                            egui::Stroke::new(
                                1.0,
                                egui::Color32::from_rgba_unmultiplied(90, 210, 255, 90),
                            ),
                        );
                    } else {
                        self.travel_selected_body = None;
                        self.travel_composition_info_open = false;
                        self.travel_atmosphere_info_open = false;
                    }
                }

                painter.text(
                    rect.left_top() + egui::Vec2::new(10.0, 10.0),
                    egui::Align2::LEFT_TOP,
                    "Top-down orbital plane view",
                    egui::FontId::proportional(12.0),
                    egui::Color32::from_rgb(160, 190, 220),
                );
            });

        if self.travel_composition_info_open {
            if let Some(TravelBodySelection::Planet(planet_index)) = self.travel_selected_body {
                if let Some(planet) = detail.planets.get(planet_index) {
                    let mut composition_window_open = self.travel_composition_info_open;
                    egui::Window::new(format!(
                        "Planet Composition - P{} ({})",
                        planet_index + 1,
                        planet.kind.label()
                    ))
                    .open(&mut composition_window_open)
                    .resizable(true)
                    .default_size([430.0, 360.0])
                    .show(ctx, |ui| {
                        ui.label(
                            "Estimated bulk elemental composition.");
                        ui.small(format!(
                            "Reference body: {} | Radius {:.2} Rearth | Mass {:.2} Mearth | Temp {:.0} K",
                            planet.kind.label(),
                            planet.radius_earth,
                            planet.mass_earth,
                            planet.temperature_k,
                        ));
                        ui.separator();

                        if planet.composition.is_empty() {
                            ui.small("No composition data is available for this body.");
                        } else {
                            egui::Grid::new(format!(
                                "travel_planet_composition_grid_{}",
                                planet_index
                            ))
                            .num_columns(4)
                            .striped(true)
                            .spacing([10.0, 4.0])
                            .show(ui, |ui| {
                                ui.label(egui::RichText::new("#").underline());
                                ui.label(egui::RichText::new("Symbol").underline());
                                ui.label(egui::RichText::new("Element").underline());
                                ui.label(egui::RichText::new("Share").underline());
                                ui.end_row();

                                for component in &planet.composition {
                                    ui.label(component.atomic_number.to_string());
                                    ui.label(component.symbol.as_str());
                                    ui.label(component.name.as_str());
                                    ui.label(format!("{:.2}%", component.percent));
                                    ui.end_row();
                                }
                            });
                        }
                    });
                    self.travel_composition_info_open = composition_window_open;
                } else {
                    self.travel_composition_info_open = false;
                }
            } else {
                self.travel_composition_info_open = false;
            }
        }

        if self.travel_atmosphere_info_open {
            if let Some(TravelBodySelection::Planet(planet_index)) = self.travel_selected_body {
                if let Some(planet) = detail.planets.get(planet_index) {
                    let mut atmosphere_window_open = self.travel_atmosphere_info_open;
                    egui::Window::new(format!(
                        "Planet Atmosphere - P{} ({})",
                        planet_index + 1,
                        planet.kind.label()
                    ))
                    .open(&mut atmosphere_window_open)
                    .resizable(true)
                    .default_size([430.0, 340.0])
                    .show(ctx, |ui| {
                        ui.label("Estimated atmospheric gas mixture.");
                        ui.small(format!(
                            "Pressure: {:.2} atm | Temp {:.0} K | {}",
                            planet.atmosphere_pressure_atm.max(0.0),
                            planet.temperature_k,
                            if planet.habitable {
                                "Potentially life-supporting"
                            } else {
                                "Non-habitable"
                            }
                        ));
                        ui.separator();

                        if planet.atmosphere.is_empty() {
                            ui.small("No sustained atmosphere detected.");
                        } else {
                            egui::Grid::new(format!(
                                "travel_planet_atmosphere_grid_{}",
                                planet_index
                            ))
                            .num_columns(3)
                            .striped(true)
                            .spacing([10.0, 4.0])
                            .show(ui, |ui| {
                                ui.label(egui::RichText::new("Formula").underline());
                                ui.label(egui::RichText::new("Gas").underline());
                                ui.label(egui::RichText::new("Share").underline());
                                ui.end_row();

                                for gas in &planet.atmosphere {
                                    ui.label(gas.formula.as_str());
                                    ui.label(gas.name.as_str());
                                    ui.label(format!("{:.2}%", gas.percent));
                                    ui.end_row();
                                }
                            });
                        }
                    });
                    self.travel_atmosphere_info_open = atmosphere_window_open;
                } else {
                    self.travel_atmosphere_info_open = false;
                }
            } else {
                self.travel_atmosphere_info_open = false;
            }
        }

        self.travel_window_open = open && !close_requested;
        if !self.travel_window_open {
            self.travel_system = None;
            self.travel_last_input_time = None;
            self.travel_selected_body = None;
            self.travel_composition_info_open = false;
            self.travel_atmosphere_info_open = false;
            self.reset_travel_view();
        } else {
            ctx.request_repaint();
        }
    }

    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let mut app = Self::default();
        app.gpu_renderer_ready = gpu_stars::initialize(cc);
        app
    }

    fn worker_thread_count() -> usize {
        thread::available_parallelism()
            .map(|n| n.get().saturating_sub(1).max(1))
            .unwrap_or(2)
            .min(MAX_WORKER_THREADS)
    }

    fn gpu_chunk_key(ix: i32, iy: i32, iz: i32, lod_bucket: i32, galaxy_seed: u64) -> u64 {
        let mut h = galaxy_seed
            ^ (ix as i64 as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (iy as i64 as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
            ^ (iz as i64 as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93)
            ^ (lod_bucket as i64 as u64).wrapping_mul(0xA24B_AED4_963E_E407);
        h ^= h >> 30;
        h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h ^= h >> 27;
        h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^ (h >> 31)
    }

    fn visible_build_signature(
        required_hash: u64,
        required_ready_hash: u64,
        lod_label: &'static str,
        galaxy_seed: u64,
    ) -> u64 {
        let mut h = galaxy_seed
            ^ required_hash.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ required_ready_hash.wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
            ^ (lod_label.as_ptr() as usize as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        h ^= h >> 30;
        h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h ^= h >> 27;
        h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^ (h >> 31)
    }

    fn build_gpu_chunk_uploads_from_points(
        render_points: &[RenderPoint],
        center: [f32; 3],
        lod_tier: LodTier,
        sector_size: f32,
        galaxy_seed: u64,
    ) -> Vec<gpu_stars::ChunkUpload> {
        let chunk_size_xy = lod_tier
            .chunk_size(sector_size)
            .unwrap_or(sector_size * 0.75)
            .max(1.0);
        let chunk_size_z = Self::chunk_size_z(chunk_size_xy).max(1.0);
        let inv_chunk_xy = 1.0 / chunk_size_xy;
        let inv_chunk_z = 1.0 / chunk_size_z;
        let lod_bucket = (chunk_size_xy * 10.0).round() as i32;

        let mut chunk_map = HashMap::<u64, Vec<gpu_stars::StarPoint>>::new();
        for point in render_points {
            let ix = ((point.pos[0] - center[0]) * inv_chunk_xy).floor() as i32;
            let iy = ((point.pos[1] - center[1]) * inv_chunk_xy).floor() as i32;
            let iz = ((point.pos[2] - center[2]) * inv_chunk_z).floor() as i32;
            let key = Self::gpu_chunk_key(ix, iy, iz, lod_bucket, galaxy_seed);
            chunk_map.entry(key).or_default().push(gpu_stars::StarPoint {
                pos: point.pos,
                represented_systems: point.represented_systems,
            });
        }

        let mut uploads = chunk_map
            .into_iter()
            .map(|(key, points)| gpu_stars::ChunkUpload { key, points })
            .collect::<Vec<_>>();
        uploads.sort_by_key(|chunk| chunk.key);
        uploads
    }

    fn spawn_sector_workers(
        worker_count: usize,
        generator: Arc<GalaxyGenerator>,
    ) -> (
        mpsc::Sender<SectorCoord>,
        mpsc::Receiver<(SectorCoord, Vec<SolarSystem>)>,
    ) {
        let (request_tx, request_rx) = mpsc::channel::<SectorCoord>();
        let (result_tx, result_rx) = mpsc::channel::<(SectorCoord, Vec<SolarSystem>)>();
        let request_rx = Arc::new(Mutex::new(request_rx));

        for _ in 0..worker_count {
            let request_rx = Arc::clone(&request_rx);
            let result_tx = result_tx.clone();
            let generator = Arc::clone(&generator);
            thread::spawn(move || {
                loop {
                    let coord = {
                        let receiver = match request_rx.lock() {
                            Ok(receiver) => receiver,
                            Err(_) => break,
                        };
                        match receiver.recv() {
                            Ok(coord) => coord,
                            Err(_) => break,
                        }
                    };

                    let systems = generator.generate_sector(coord);
                    if result_tx.send((coord, systems)).is_err() {
                        break;
                    }
                }
            });
        }

        (request_tx, result_rx)
    }

    fn spawn_visible_build_worker(
    ) -> (
        mpsc::SyncSender<VisibleBuildRequest>,
        mpsc::Receiver<VisibleBuildResult>,
    ) {
        let (request_tx, request_rx) = mpsc::sync_channel::<VisibleBuildRequest>(1);
        let (result_tx, result_rx) = mpsc::channel::<VisibleBuildResult>();

        thread::spawn(move || {
            while let Ok(request) = request_rx.recv() {
                let build_start = Instant::now();

                let sample_seed = request.galaxy_seed
                    ^ (request.render_budget as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);

                let (visible_system_count, render_points) =
                    if let Some(chunk_size) = request.lod_tier.chunk_size(request.sector_size) {
                        let chunk_budget = request.desired_chunk_budget.max(1);
                        let chunk_size_z = GalaxyApp::chunk_size_z(chunk_size);
                        GalaxyApp::build_chunk_points(
                            &request.sectors,
                            request.center,
                            chunk_size,
                            chunk_size_z,
                            chunk_budget,
                            sample_seed,
                            request.galaxy_seed,
                        )
                    } else {
                        let mut visible_system_count = 0usize;
                        let mut visible = Vec::new();
                        for systems in &request.sectors {
                            visible_system_count = visible_system_count.saturating_add(
                                systems
                                    .iter()
                                    .map(|system| system.represented_systems as usize)
                                    .sum::<usize>(),
                            );
                            visible.extend_from_slice(systems.as_ref());
                        }

                        let systems =
                            GalaxyApp::sample_with_budget(visible, request.render_budget, sample_seed);
                        let render_points = systems
                            .into_iter()
                            .map(|system| RenderPoint {
                                pos: system.pos,
                                represented_systems: system.represented_systems.max(1),
                            })
                            .collect::<Vec<_>>();

                        (visible_system_count, render_points)
                    };

                let chunk_uploads = if request.build_chunk_uploads {
                    GalaxyApp::build_gpu_chunk_uploads_from_points(
                        &render_points,
                        request.center,
                        request.lod_tier,
                        request.sector_size,
                        request.galaxy_seed,
                    )
                } else {
                    Vec::new()
                };

                let result = VisibleBuildResult {
                    signature: request.signature,
                    required_hash: request.required_hash,
                    required_ready_hash: request.required_ready_hash,
                    lod_label: request.lod_tier.label(),
                    visible_system_count,
                    render_points,
                    chunk_uploads,
                    build_ms: build_start.elapsed().as_secs_f32() * 1000.0,
                };

                if result_tx.send(result).is_err() {
                    break;
                }
            }
        });

        (request_tx, result_rx)
    }

    fn drain_visible_build_results(&mut self) {
        let mut latest_matching = None;
        while let Ok(result) = self.visible_build_result_rx.try_recv() {
            self.visible_build_inflight = false;
            if result.signature == self.visible_build_requested_signature {
                latest_matching = Some(result);
            }
        }

        let Some(result) = latest_matching else {
            return;
        };

        self.last_required_hash = result.required_hash;
        self.last_required_ready_hash = result.required_ready_hash;
        self.last_lod_label = result.lod_label;
        self.visible_system_count = result.visible_system_count;
        self.render_points = result.render_points;

        if self.gpu_renderer_ready {
            self.chunk_uploads_generation = self.chunk_uploads_generation.wrapping_add(1);
            self.cached_chunk_uploads = Arc::new(result.chunk_uploads);
        }

        self.rebuilt_this_frame = true;
        self.cpu_rebuild_ms_last = result.build_ms;
        self.cpu_rebuild_ms_smooth =
            Self::smooth_ms(self.cpu_rebuild_ms_smooth, self.cpu_rebuild_ms_last);
    }

    fn drain_generation_results(&mut self) {
        let mut processed = 0;
        while processed < MAX_RESULTS_PER_FRAME {
            match self.result_rx.try_recv() {
                Ok((coord, systems)) => {
                    self.pending_sectors.remove(&coord);
                    self.sector_cache.insert(coord, systems);
                    processed += 1;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.pending_sectors.clear();
                    break;
                }
            }
        }
    }

    fn sector_priority_score(
        coord: SectorCoord,
        center_sector: SectorCoord,
        motion_sectors: egui::Vec2,
    ) -> f32 {
        let dx = (coord.x - center_sector.x) as f32;
        let dy = (coord.y - center_sector.y) as f32;
        let mut score = dx * dx + dy * dy;

        let motion_len_sq = motion_sectors.length_sq();
        let sector_len_sq = score;
        if motion_len_sq > 0.0001 && sector_len_sq > 0.0001 {
            let motion_len = motion_len_sq.sqrt();
            let sector_len = sector_len_sq.sqrt();
            let alignment = (dx / sector_len) * (motion_sectors.x / motion_len)
                + (dy / sector_len) * (motion_sectors.y / motion_len);
            // Higher alignment means the sector is in the movement direction, so prioritize it.
            score -= alignment.max(0.0) * 3.5;
        }

        score
    }

    fn sample_with_budget(items: Vec<SolarSystem>, budget: usize, sample_seed: u64) -> Vec<SolarSystem> {
        if items.len() <= budget {
            return items;
        }

        let mut scored = items
            .into_iter()
            .map(|system| {
                let mut h = sample_seed
                    ^ (system.pos[0].to_bits() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    ^ (system.pos[1].to_bits() as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
                    ^ (system.pos[2].to_bits() as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93);
                h ^= h >> 30;
                h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
                h ^= h >> 27;
                h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
                h ^= h >> 31;
                (h, system)
            })
            .collect::<Vec<_>>();

        scored.select_nth_unstable_by(budget, |a, b| a.0.cmp(&b.0));
        scored.truncate(budget);
        scored.into_iter().map(|(_, system)| system).collect()
    }

    fn chunk_size_z(chunk_size_xy: f32) -> f32 {
        (chunk_size_xy * CHUNK_Z_SIZE_RATIO).clamp(CHUNK_Z_SIZE_MIN, CHUNK_Z_SIZE_MAX)
    }

    fn chunk_seed(coord: ChunkCoord, extra: u64) -> u64 {
        let mut z = extra
            ^ (coord.x as i64 as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (coord.y as i64 as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
            ^ (coord.z as i64 as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        z ^= z >> 30;
        z = z.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z ^= z >> 27;
        z = z.wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn desired_chunk_point_budget(&self) -> usize {
        let min_budget = self.lod_chunk_point_min_budget.min(self.render_budget);
        if min_budget >= self.render_budget {
            return self.render_budget;
        }

        let sector_pixels = self.sector_size * self.zoom;
        let t = (sector_pixels / CHUNK_POINT_RAMP_SECTOR_PX)
            .clamp(0.0, 1.0)
            .powf(0.85);
        let budget_range = self.render_budget - min_budget;
        min_budget + (budget_range as f32 * t).round() as usize
    }

    fn choose_lod_tier(
        &mut self,
        required: &[SectorCoord],
        zoom: f32,
        sector_size: f32,
    ) -> LodTier {
        let zoom_based =
            LodTier::from_zoom(zoom, sector_size, self.lod_transition_zoom_scale);
        if zoom_based.chunk_factor.is_none() {
            return zoom_based;
        }

        let mut cached_sector_count = 0usize;
        let mut cached_point_count = 0usize;
        for coord in required {
            if let Some(systems) = self.sector_cache.get(*coord) {
                cached_sector_count += 1;
                cached_point_count = cached_point_count.saturating_add(systems.len());
            }
        }

        if cached_sector_count == 0 {
            return zoom_based;
        }

        let readiness = cached_sector_count as f32 / required.len().max(1) as f32;
        let missing_sector_count = required.len().saturating_sub(cached_sector_count);
        let point_limit = self
            .render_budget
            .min(self.lod_system_view_hard_limit)
            .max(self.lod_system_view_soft_limit);

        if readiness >= self.lod_system_view_readiness_min
            && missing_sector_count <= self.lod_system_view_max_missing_sectors
            && cached_point_count <= point_limit
        {
            LodTier::systems()
        } else {
            zoom_based
        }
    }

    fn chunk_input_stride(total_input_systems: usize, target_points: usize) -> usize {
        if total_input_systems <= 1 {
            return 1;
        }

        let target_inputs = target_points
            .saturating_mul(CHUNK_INPUTS_PER_TARGET_POINT)
            .clamp(CHUNK_INPUT_MIN_BUDGET, CHUNK_INPUT_MAX_BUDGET)
            .max(target_points.max(1));

        if total_input_systems <= target_inputs {
            1
        } else {
            (total_input_systems + target_inputs - 1) / target_inputs
        }
    }

    fn chunk_sample_phase(sample_seed: u64, sector_index: usize, stride: usize) -> usize {
        if stride <= 1 {
            return 0;
        }

        let mut h = sample_seed ^ (sector_index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= h >> 30;
        h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h ^= h >> 27;
        h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
        ((h ^ (h >> 31)) as usize) % stride
    }

    fn allocate_representatives(chunks: &[ChunkSummary], desired_points: usize) -> Vec<u32> {
        if chunks.is_empty() {
            return Vec::new();
        }

        let weights = chunks
            .iter()
            .map(|chunk| (chunk.count as f64).powf(CHUNK_REP_WEIGHT_GAMMA))
            .collect::<Vec<_>>();

        let total_systems = chunks
            .iter()
            .map(|chunk| chunk.count as usize)
            .sum::<usize>()
            .max(1);
        let target = desired_points.clamp(1, total_systems);

        if target >= chunks.len() {
            let mut reps = vec![1u32; chunks.len()];
            let capacities = chunks
                .iter()
                .map(|chunk| (chunk.count as usize).saturating_sub(1))
                .collect::<Vec<_>>();
            let weighted_capacity_total = capacities
                .iter()
                .zip(weights.iter())
                .map(|(capacity, weight)| *capacity as f64 * *weight)
                .sum::<f64>();
            if weighted_capacity_total <= f64::EPSILON {
                return reps;
            }

            let capacity_total = capacities.iter().sum::<usize>();
            let extra_target = (target - chunks.len()).min(capacity_total);
            let mut used_extra = 0usize;
            let mut remainders = Vec::with_capacity(chunks.len());

            for (idx, capacity) in capacities.iter().copied().enumerate() {
                if capacity == 0 {
                    continue;
                }
                let weighted_capacity = capacity as f64 * weights[idx];
                let ideal = (weighted_capacity * extra_target as f64) / weighted_capacity_total;
                let base = ideal.floor() as usize;
                if base > 0 {
                    reps[idx] = reps[idx].saturating_add(base as u32);
                    used_extra += base;
                }
                remainders.push((idx, ideal - base as f64));
            }

            if used_extra < extra_target {
                remainders.sort_by(|a, b| b.1.total_cmp(&a.1));
                let mut remaining = extra_target - used_extra;
                for (idx, _) in remainders {
                    if remaining == 0 {
                        break;
                    }
                    let max_reps = chunks[idx].count as usize;
                    if reps[idx] as usize >= max_reps {
                        continue;
                    }
                    reps[idx] = reps[idx].saturating_add(1);
                    remaining -= 1;
                }
            }

            reps
        } else {
            let mut reps = vec![0u32; chunks.len()];
            let mut used = 0usize;
            let mut remainders = Vec::with_capacity(chunks.len());
            let weight_total = weights.iter().sum::<f64>().max(f64::EPSILON);

            for (idx, chunk) in chunks.iter().enumerate() {
                let ideal = (weights[idx] * target as f64) / weight_total;
                let base = ideal.floor() as usize;
                if base > 0 {
                    let base_capped = base.min(chunk.count as usize);
                    reps[idx] = base_capped as u32;
                    used += base_capped;
                }
                remainders.push((idx, ideal - base as f64, chunk.count));
            }

            if used < target {
                remainders.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| b.2.cmp(&a.2)));
                let mut remaining = target - used;
                for (idx, _, _) in remainders {
                    if remaining == 0 {
                        break;
                    }
                    if u64::from(reps[idx]) < chunks[idx].count {
                        reps[idx] = reps[idx].saturating_add(1);
                        remaining -= 1;
                    }
                }
            }

            reps
        }
    }

    fn chunk_coord_for_position(
        pos: [f32; 3],
        center: [f32; 3],
        chunk_size_xy: f32,
        chunk_size_z: f32,
    ) -> ChunkCoord {
        let inv_chunk_size_xy = 1.0 / chunk_size_xy.max(1.0);
        let inv_chunk_size_z = 1.0 / chunk_size_z.max(1.0);
        let dx = pos[0] - center[0];
        let dy = pos[1] - center[1];
        let (sin_grid, cos_grid) = CHUNK_GRID_ROTATION_RADIANS.sin_cos();
        let rotated_x = cos_grid * dx - sin_grid * dy;
        let rotated_y = sin_grid * dx + cos_grid * dy;
        ChunkCoord {
            x: (rotated_x * inv_chunk_size_xy).floor() as i32,
            y: (rotated_y * inv_chunk_size_xy).floor() as i32,
            z: ((pos[2] - center[2]) * inv_chunk_size_z).floor() as i32,
        }
    }

    fn build_chunk_points(
        sectors: &[Arc<Vec<SolarSystem>>],
        center: [f32; 3],
        chunk_size_xy: f32,
        chunk_size_z: f32,
        target_points: usize,
        sample_seed: u64,
        galaxy_seed: u64,
    ) -> (usize, Vec<RenderPoint>) {
        let total_input_systems = sectors.iter().map(|systems| systems.len()).sum::<usize>();
        if total_input_systems == 0 {
            return (0, Vec::new());
        }

        let stride = Self::chunk_input_stride(total_input_systems, target_points.max(1));
        let mut chunks = HashMap::<ChunkCoord, ChunkAccumulator>::new();
        let mut visible_system_count = 0usize;

        for (sector_index, systems) in sectors.iter().enumerate() {
            if systems.is_empty() {
                continue;
            }

            if stride <= 1 {
                for system in systems.iter() {
                    let represented = system.represented_systems.max(1) as u64;
                    visible_system_count = visible_system_count.saturating_add(
                        represented.min(usize::MAX as u64) as usize,
                    );

                    let chunk_coord = Self::chunk_coord_for_position(
                        system.pos,
                        center,
                        chunk_size_xy,
                        chunk_size_z,
                    );
                    let entry = chunks.entry(chunk_coord).or_default();
                    let represented_f = represented as f32;
                    entry.count = entry.count.saturating_add(represented);
                    entry.sum_x += system.pos[0] * represented_f;
                    entry.sum_y += system.pos[1] * represented_f;
                    entry.sum_z += system.pos[2] * represented_f;
                }
                continue;
            }

            let mut idx = Self::chunk_sample_phase(sample_seed, sector_index, stride);
            if idx >= systems.len() {
                idx = systems.len() - 1;
            }

            while idx < systems.len() {
                let system = systems[idx];
                let represented = (system.represented_systems.max(1) as u64)
                    .saturating_mul(stride as u64)
                    .max(1);
                visible_system_count = visible_system_count.saturating_add(
                    represented.min(usize::MAX as u64) as usize,
                );

                let chunk_coord = Self::chunk_coord_for_position(
                    system.pos,
                    center,
                    chunk_size_xy,
                    chunk_size_z,
                );
                let entry = chunks.entry(chunk_coord).or_default();
                let represented_f = represented as f32;
                entry.count = entry.count.saturating_add(represented);
                entry.sum_x += system.pos[0] * represented_f;
                entry.sum_y += system.pos[1] * represented_f;
                entry.sum_z += system.pos[2] * represented_f;

                idx += stride;
            }
        }

        if chunks.is_empty() {
            return (visible_system_count, Vec::new());
        }

        let mut summaries = chunks
            .into_iter()
            .map(|(coord, chunk)| {
                let inv_count = 1.0 / chunk.count.max(1) as f32;
                ChunkSummary {
                    coord,
                    count: chunk.count.max(1),
                    centroid: [
                        chunk.sum_x * inv_count,
                        chunk.sum_y * inv_count,
                        chunk.sum_z * inv_count,
                    ],
                }
            })
            .collect::<Vec<_>>();

        summaries.sort_by_key(|chunk| (chunk.coord.x, chunk.coord.y, chunk.coord.z));

        let rep_counts = Self::allocate_representatives(&summaries, target_points);
        let point_capacity = rep_counts.iter().map(|count| *count as usize).sum::<usize>();
        let mut points = Vec::with_capacity(point_capacity);

        for (idx, chunk) in summaries.iter().enumerate() {
            let reps = rep_counts[idx] as usize;
            if reps == 0 {
                continue;
            }

            let represented_per_point = ((chunk.count as usize + reps - 1) / reps)
                .min(u32::MAX as usize) as u32;
            // Seed jitter from stable chunk coordinates so points do not reshuffle
            // globally when nearby sectors stream in/out.
            let jitter_seed = galaxy_seed ^ 0xA24B_AED4_963E_E407;
            let mut rng = StdRng::seed_from_u64(Self::chunk_seed(chunk.coord, jitter_seed));

            // Spread around the centroid and allow cross-boundary overlap to avoid
            // visible square-grid voids in coarse LOD.
            let spread_xy_scale = (3_200.0 / chunk_size_xy.max(1.0)).sqrt().clamp(0.72, 1.0);
            let spread_z_scale = (900.0 / chunk_size_z.max(1.0)).sqrt().clamp(0.55, 1.0);
            let spread_xy = chunk_size_xy * CHUNK_SCATTER_BLEND * spread_xy_scale;
            let spread_z = chunk_size_z * CHUNK_SCATTER_BLEND_Z * spread_z_scale;
            let mut sample_axis = |spread: f32| -> f32 {
                (rng.r#gen::<f32>() - rng.r#gen::<f32>()) * spread
            };

            if reps == 1 {
                let x = (chunk.centroid[0] + sample_axis(spread_xy * 0.35)).clamp(X_MIN, X_MAX);
                let y = (chunk.centroid[1] + sample_axis(spread_xy * 0.35)).clamp(Y_MIN, Y_MAX);
                let z = (chunk.centroid[2] + sample_axis(spread_z * 0.35)).clamp(Z_MIN, Z_MAX);

                points.push(RenderPoint {
                    pos: [x, y, z],
                    represented_systems: represented_per_point.max(1),
                });
                continue;
            }

            for _ in 0..reps {
                let x = (chunk.centroid[0] + sample_axis(spread_xy)).clamp(X_MIN, X_MAX);
                let y = (chunk.centroid[1] + sample_axis(spread_xy)).clamp(Y_MIN, Y_MAX);
                let z = (chunk.centroid[2] + sample_axis(spread_z)).clamp(Z_MIN, Z_MAX);

                points.push(RenderPoint {
                    pos: [x, y, z],
                    represented_systems: represented_per_point.max(1),
                });
            }
        }

        (visible_system_count, points)
    }

    fn update_visible_buffer(&mut self, center: [f32; 3], viewport_size: egui::Vec2) {
        let update_start = Instant::now();
        self.rebuilt_this_frame = false;
        if self.zoom <= 0.0 {
            self.cpu_visible_update_ms_last = 0.0;
            return;
        }

        self.drain_generation_results();
        self.drain_visible_build_results();

        let view_center = egui::Vec2::new(center[0] - self.pan.x, center[1] - self.pan.y);
        let motion_world = match self.last_view_center {
            Some(last_view_center) => view_center - last_view_center,
            None => egui::Vec2::ZERO,
        };
        self.last_view_center = Some(view_center);
        let motion_sectors = motion_world / self.sector_size;

        let view_center_x = view_center.x;
        let view_center_y = view_center.y;
        let view_radius_world = (viewport_size.x.max(viewport_size.y) * 0.5) / self.zoom + self.sector_size * 1.5;

        let center_sector = SectorCoord {
            x: ((view_center_x - center[0]) / self.sector_size).floor() as i32,
            y: ((view_center_y - center[1]) / self.sector_size).floor() as i32,
        };

        let sector_cull_radius = playfield_radius() + self.sector_size * 1.5;
        let sector_cull_radius_sq = sector_cull_radius * sector_cull_radius;
        let max_sector_radius = (sector_cull_radius / self.sector_size).ceil() as i32 + 1;
        let radius_sectors = ((view_radius_world / self.sector_size).ceil() as i32)
            .min(max_sector_radius);

        let mut required = Vec::new();
        for sx in (center_sector.x - radius_sectors)..=(center_sector.x + radius_sectors) {
            for sy in (center_sector.y - radius_sectors)..=(center_sector.y + radius_sectors) {
                let sector_center_x = center[0] + (sx as f32 + 0.5) * self.sector_size;
                let sector_center_y = center[1] + (sy as f32 + 0.5) * self.sector_size;
                let dx = sector_center_x - center[0];
                let dy = sector_center_y - center[1];
                if dx * dx + dy * dy > sector_cull_radius_sq {
                    continue;
                }
                required.push(SectorCoord { x: sx, y: sy });
            }
        }

        let mut missing = required
            .iter()
            .copied()
            .filter(|coord| {
                !self.sector_cache.contains(*coord) && !self.pending_sectors.contains(coord)
            })
            .collect::<Vec<_>>();

        if !missing.is_empty() {
            missing.sort_by(|a, b| {
                let score_a = Self::sector_priority_score(*a, center_sector, motion_sectors);
                let score_b = Self::sector_priority_score(*b, center_sector, motion_sectors);
                score_a.total_cmp(&score_b)
            });

            let open_slots = MAX_PENDING_REQUESTS.saturating_sub(self.pending_sectors.len());
            let request_count = open_slots.min(MAX_REQUESTS_PER_FRAME).min(missing.len());

            for coord in missing.into_iter().take(request_count) {
                if self.request_tx.send(coord).is_ok() {
                    self.pending_sectors.insert(coord);
                } else {
                    let systems = self.procedural_generator.generate_sector(coord);
                    self.sector_cache.insert(coord, systems);
                }
            }
        }

        // Determine LOD tier early so a tier change also triggers a rebuild.
        // Sparse regions can stay in full system view much farther out.
        let new_lod_tier = self.choose_lod_tier(&required, self.zoom, self.sector_size);

        // A cheap hash over the required-sector list detects view changes.
        let required_hash = required.iter().fold(self.galaxy_seed, |h, c| {
            h ^ (c.x as i64 as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
              ^ (c.y as i64 as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
        });

        self.lod_tier = new_lod_tier;

        let required_ready_hash = required.iter().fold(0xD6E8_FEB8_6659_FD93u64, |h, coord| {
            let coord_hash = (coord.x as i64 as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ (coord.y as i64 as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
            let ready_hash = if self.sector_cache.contains(*coord) {
                0xA24B_AED4_963E_E407u64
            } else {
                0x94D0_49BB_1331_11EBu64
            };
            let mut z = h ^ coord_hash ^ ready_hash;
            z ^= z >> 30;
            z = z.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z ^= z >> 27;
            z = z.wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        });

        let needs_rebuild = required_hash != self.last_required_hash
            || required_ready_hash != self.last_required_ready_hash
            || self.lod_tier.label() != self.last_lod_label;

        if needs_rebuild {
            let desired_signature = Self::visible_build_signature(
                required_hash,
                required_ready_hash,
                self.lod_tier.label(),
                self.galaxy_seed,
            );

            if !self.visible_build_inflight
                || self.visible_build_requested_signature != desired_signature
            {
                let mut sectors = Vec::new();
                for coord in &required {
                    if let Some(systems) = self.sector_cache.get(*coord) {
                        sectors.push(systems);
                    }
                }

                let request = VisibleBuildRequest {
                    galaxy_seed: self.galaxy_seed,
                    signature: desired_signature,
                    required_hash,
                    required_ready_hash,
                    lod_tier: self.lod_tier,
                    center,
                    sector_size: self.sector_size,
                    render_budget: self.render_budget,
                    desired_chunk_budget: self.desired_chunk_point_budget(),
                    sectors,
                    build_chunk_uploads: self.gpu_renderer_ready,
                };

                match self.visible_build_request_tx.try_send(request) {
                    Ok(()) => {
                        self.visible_build_requested_signature = desired_signature;
                        self.visible_build_inflight = true;
                    }
                    Err(mpsc::TrySendError::Full(_)) => {}
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        self.visible_build_inflight = false;
                    }
                }
            }
        }

        self.cpu_visible_update_ms_last = update_start.elapsed().as_secs_f32() * 1000.0;
        self.cpu_visible_update_ms_smooth =
            Self::smooth_ms(self.cpu_visible_update_ms_smooth, self.cpu_visible_update_ms_last);
    }
}

fn rotate_point(point: [f32; 3], yaw: f32, pitch: f32, center: [f32; 3]) -> [f32; 3] {
    // Yaw (around y axis)
    let (sy, cy) = yaw.sin_cos();
    let (sp, cp) = pitch.sin_cos();
    let mut x = point[0] - center[0];
    let mut y = point[1] - center[1];
    let mut z = point[2] - center[2];
    // Yaw rotation (y axis)
    let xz = cy * x - sy * z;
    let zz = sy * x + cy * z;
    x = xz;
    z = zz;
    // Pitch rotation (x axis)
    let yz = cp * y - sp * z;
    let zz = sp * y + cp * z;
    y = yz;
    z = zz;
    [x + center[0], y + center[1], z + center[2]]
}

fn unrotate_point(point: [f32; 3], yaw: f32, pitch: f32, center: [f32; 3]) -> [f32; 3] {
    let (sy, cy) = yaw.sin_cos();
    let (sp, cp) = pitch.sin_cos();
    let x = point[0] - center[0];
    let y = point[1] - center[1];
    let z = point[2] - center[2];

    // Inverse pitch rotation.
    let y1 = cp * y + sp * z;
    let z1 = -sp * y + cp * z;

    // Inverse yaw rotation.
    let x1 = cy * x + sy * z1;
    let z2 = -sy * x + cy * z1;

    [x1 + center[0], y1 + center[1], z2 + center[2]]
}

impl eframe::App for GalaxyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let sim_advance = self
            .strategic_clock
            .advance(ctx.input(|i| i.time), self.game_paused);
        if sim_advance.ticks > 0 {
            for _ in 0..sim_advance.ticks {
                let generated_events = self.game_state.advance_strategic_tick(sim_advance.tick_years);
                for event in generated_events {
                    self.game_state.apply_event(&event);
                    self.game_events.push(event);
                }
            }
            self.trim_game_events();
            self.game_autosave_accum_years +=
                sim_advance.tick_years * sim_advance.ticks as f32;
            if self.game_autosave_accum_years >= 0.5 {
                self.game_autosave_accum_years = 0.0;
                self.persist_game_state();
            }
        }

        let center3d = galaxy_center();
        egui::CentralPanel::default().show(ctx, |ui| {
            let gpu_timing = gpu_stars::latest_timing_snapshot();
            let gpu_runtime = gpu_stars::latest_runtime_snapshot();
            let gpu_work = gpu_stars::latest_work_snapshot();
            let format_opt_ms = |value: Option<f32>| -> String {
                value
                    .map(|ms| format!("{ms:.2} ms"))
                    .unwrap_or_else(|| "n/a".to_owned())
            };
            let rebuild_state = if self.rebuilt_this_frame {
                format!("{:.2} ms", self.cpu_rebuild_ms_last)
            } else {
                "idle".to_owned()
            };
            let timestamp_mode = if !gpu_timing.timestamp_supported {
                "unsupported"
            } else if gpu_timing.render_timestamp_supported {
                "cull+render"
            } else {
                "cull-only"
            };

            ui.heading("Milky Way Simulator");
            ui.label("Mouse wheel: zoom | Left drag: pan | Right drag: rotate camera | Double-click system: center camera");
            ui.horizontal(|ui| {
                ui.checkbox(&mut self.game_paused, "Strategic pause");
                let mut years_per_second = self.strategic_clock.years_per_real_second();
                if ui
                    .add(
                        egui::Slider::new(&mut years_per_second, 0.01..=5.0)
                            .text("Strategic years / sec")
                            .logarithmic(true),
                    )
                    .changed()
                {
                    self.strategic_clock
                        .set_years_per_real_second(years_per_second);
                }
                if ui.button("Colonized Systems").clicked() {
                    self.colonies_window_open = true;
                }
                if ui.button("Construction").clicked() {
                    self.construction_window_open = true;
                }
                if ui.button("Favorites").clicked() {
                    self.favorites_window_open = !self.favorites_window_open;
                }
                if ui.button("Settings").clicked() {
                    self.settings_window_open = true;
                    self.settings_seed_input = self.galaxy_seed.to_string();
                    self.settings_render_budget = self.render_budget;
                    self.settings_lod_preset = self.lod_preset;
                }
                if ui
                    .button(if self.resources_panel_open {
                        "Hide resources"
                    } else {
                        "Resources window"
                    })
                    .clicked()
                {
                    self.resources_panel_open = !self.resources_panel_open;
                }
                if ui
                    .button(if self.debug_panel_open {
                        "Hide debug"
                    } else {
                        "Show debug"
                    })
                    .clicked()
                {
                    self.debug_panel_open = !self.debug_panel_open;
                }

                if !self.reset_progress_armed {
                    if ui.button("Reset all progress").clicked() {
                        self.reset_progress_armed = true;
                        self.game_notice = Some(
                            "Click 'Confirm reset' to wipe all saved progress.".to_owned(),
                        );
                    }
                } else {
                    if ui.button("Confirm reset").clicked() {
                        self.reset_all_progress();
                    }
                    if ui.button("Cancel reset").clicked() {
                        self.reset_progress_armed = false;
                    }
                }
            });
            ui.label(format!(
                "Year: {:.2} | Known systems: {} | Fully assessed: {} | Colonies: {} | Factions: {} | Game events: {}",
                self.game_state.current_year,
                self.game_state.explored_systems.len(),
                self.game_state.fully_surveyed_system_count(),
                self.game_state.colonies.len(),
                self.game_state.factions.len(),
                self.game_events.len(),
            ));
            ui.horizontal(|ui| {
                let mut base_range = self.game_state.base_colonization_range_world;
                if ui
                    .add(
                        egui::Slider::new(&mut base_range, 100.0..=5_000.0)
                            .text("Base colonization range")
                            .logarithmic(true),
                    )
                    .changed()
                {
                    self.game_state.base_colonization_range_world = base_range;
                    self.game_autosave_accum_years = 0.0;
                    self.persist_game_state();
                }

                let player_tech_level = self
                    .game_state
                    .factions
                    .get(&self.game_state.player.faction_id)
                    .map(|faction| faction.colonization_tech_level)
                    .unwrap_or(0);
                let player_range = self
                    .game_state
                    .faction_colonization_range_world(&self.game_state.player.faction_id);
                ui.label(format!(
                    "Player colonization tech: L{} | Effective range: {:.0}",
                    player_tech_level, player_range
                ));
            });
            if self.game_state.player.home_system.is_some() {
                ui.label("Home system selected");
            } else {
                ui.colored_label(
                    egui::Color32::from_rgb(255, 210, 120),
                    "Select a home system first to begin charting nearby systems.",
                );
            }
            let mut player_colonies = self
                .game_state
                .colonies
                .values()
                .filter(|colony| colony.owner_faction == self.game_state.player.faction_id)
                .map(|colony| (colony.id, colony.name.clone()))
                .collect::<Vec<_>>();
            player_colonies.sort_by_key(|(id, _)| *id);
            if player_colonies.is_empty() {
                self.starting_colony_selection = None;
                ui.label("Starting colony: none (found your first colony to set one)");
            } else {
                let first_colony_id = player_colonies[0].0;
                if self.starting_colony_selection.is_none() {
                    self.starting_colony_selection =
                        self.game_state.player.starting_colony_id.or(Some(first_colony_id));
                }
                if let Some(selected_id) = self.starting_colony_selection {
                    if !player_colonies.iter().any(|(id, _)| *id == selected_id) {
                        self.starting_colony_selection = Some(first_colony_id);
                    }
                }

                let selected_text = self
                    .starting_colony_selection
                    .and_then(|id| {
                        player_colonies
                            .iter()
                            .find(|(candidate_id, _)| *candidate_id == id)
                            .map(|(_, name)| name.clone())
                    })
                    .unwrap_or_else(|| "Select colony".to_owned());

                ui.horizontal(|ui| {
                    ui.label("Starting colony:");
                    egui::ComboBox::from_id_source("starting_colony_selector")
                        .selected_text(selected_text)
                        .show_ui(ui, |ui| {
                            for (id, name) in &player_colonies {
                                ui.selectable_value(
                                    &mut self.starting_colony_selection,
                                    Some(*id),
                                    name,
                                );
                            }
                        });

                    if ui.button("Set starting colony").clicked() {
                        if let Some(colony_id) = self.starting_colony_selection {
                            self.record_game_event(GameEvent::StartingColonySelected {
                                at_year: self.game_state.current_year,
                                colony_id,
                            });
                            self.starting_colony_selection =
                                self.game_state.player.starting_colony_id;
                            self.game_notice = Some("Starting colony updated.".to_owned());
                        }
                    }
                });
            }
            if let Some(err) = &self.game_save_error {
                ui.colored_label(egui::Color32::YELLOW, err);
            }
            if let Some(notice) = &self.game_notice {
                ui.colored_label(egui::Color32::from_rgb(130, 205, 255), notice);
            }
            if ui
                .small_button(if self.legend_panel_open {
                    "Hide legend"
                } else {
                    "Show legend"
                })
                .clicked()
            {
                self.legend_panel_open = !self.legend_panel_open;
            }
            if self.legend_panel_open {
                ui.group(|ui| {
                    ui.label("Legend");
                    ui.horizontal_wrapped(|ui| {
                        ui.colored_label(
                            egui::Color32::from_rgb(70, 210, 170),
                            "+ marker = player colony",
                        );
                        ui.colored_label(
                            egui::Color32::from_rgb(226, 160, 82),
                            "+ marker = non-player colony",
                        );
                        ui.colored_label(
                            egui::Color32::from_rgb(92, 220, 255),
                            "blue ring = selected system",
                        );
                        ui.colored_label(
                            egui::Color32::from_rgb(80, 220, 110),
                            "green range ring/line = colonization possible",
                        );
                        ui.colored_label(
                            egui::Color32::from_rgb(230, 120, 100),
                            "red range ring/line = out of range",
                        );
                    });
                });
            } else {
                ui.small("Open 'Legend' for colony markers and range colors.");
            }
            if self.debug_panel_open {
                ui.group(|ui| {
                    ui.label(egui::RichText::new("Debug").strong());
                    ui.label(format!(
                        "Target systems: {} | Estimated full-galaxy systems: {} | Visible systems: {} | Draw points: {} (budget {}) | Preset: {} | LOD: {} | Render path: {} | Cached sectors: {}/{} | Pending: {} | Workers: {} | Rebuild worker: {}",
                        self.target_system_count,
                        self.estimated_total_systems,
                        self.visible_system_count,
                        self.render_points.len(),
                        self.render_budget,
                        self.lod_preset.label(),
                        self.lod_tier.label(),
                        if self.gpu_renderer_ready { "GPU (wgpu)" } else { "CPU" },
                        self.sector_cache.len(),
                        self.sector_cache.capacity(),
                        self.pending_sectors.len(),
                        self.worker_count,
                        if self.visible_build_inflight { "in-flight" } else { "idle" },
                    ));
                    ui.label(format!(
                        "CPU visible update: {:.2} ms (avg {:.2}) | CPU rebuild: {} (avg {:.2}) | GPU cull: {} | GPU render: {} | GPU total: {} | GPU timestamps: {}",
                        self.cpu_visible_update_ms_last,
                        self.cpu_visible_update_ms_smooth,
                        rebuild_state,
                        self.cpu_rebuild_ms_smooth,
                        format_opt_ms(gpu_timing.cull_ms),
                        format_opt_ms(gpu_timing.render_ms),
                        format_opt_ms(gpu_timing.total_ms),
                        timestamp_mode,
                    ));
                    ui.label(format!(
                        "GPU adapter: {} | Backend: {} | Type: {} | Driver: {} ({}) | Timestamp features enabled: query={} inside_pass={}",
                        gpu_runtime.adapter_name,
                        gpu_runtime.backend,
                        gpu_runtime.device_type,
                        gpu_runtime.driver,
                        gpu_runtime.driver_info,
                        gpu_runtime.timestamp_feature_enabled,
                        gpu_runtime.timestamp_inside_pass_feature_enabled,
                    ));
                    ui.label(format!(
                        "GPU work: prepare frame {} | resident instances {} | max visible {} | cull groups {} | keep_prob {:.3}",
                        gpu_work.prepared_frames,
                        gpu_work.resident_instances,
                        gpu_work.max_visible_count,
                        gpu_work.cull_dispatch_groups,
                        gpu_work.keep_prob,
                    ));
                    if gpu_runtime.renderer_initialized && gpu_runtime.likely_software_adapter {
                        ui.colored_label(
                            egui::Color32::YELLOW,
                            "Warning: a software adapter was detected (likely llvmpipe/CPU rasterizer), so the discrete GPU may not be used.",
                        );
                    }
                });
            } else {
                ui.small("Debug telemetry is hidden. Use 'Show debug' to inspect renderer/runtime metrics.");
            }

            let available = ui.available_size();
            let canvas_size = egui::Vec2::new(available.x.max(1.0), available.y.max(1.0));
            let (response, painter) = ui.allocate_painter(
                canvas_size,
                egui::Sense::click_and_drag(),
            );

            if !self.view_fitted {
                // Fit the circular playfield into the current canvas with a small margin.
                let fit_radius_px = response.rect.width().min(response.rect.height()) * 0.46;
                self.zoom = (fit_radius_px / playfield_radius()).clamp(0.0001, 10.0);
                self.pan = egui::Vec2::ZERO;
                self.pan_velocity = egui::Vec2::ZERO;
                self.camera_focus_tween = None;
                self.camera_lock_target = Some(center3d);
                self.view_fitted = true;
            }

            // Handle zoom
            if response.hovered() {
                if let Some(scroll) = ctx.input(|i| i.raw_scroll_delta.y).into() {
                    if scroll != 0.0 {
                        self.camera_focus_tween = None;
                        let zoom_factor = 1.1_f32.powf(scroll / 20.0);
                        self.zoom *= zoom_factor;
                        self.zoom = self.zoom.clamp(0.0001, 10.0);
                    }
                }
            }

            // Handle pan (left drag)
            if response.drag_started_by(egui::PointerButton::Primary) {
                self.dragging = true;
                self.camera_focus_tween = None;
                if let Some(mouse_pos) = ctx.pointer_latest_pos() {
                    self.last_mouse_pos = mouse_pos;
                }
            }
            if self.dragging && ctx.input(|i| i.pointer.primary_down()) {
                if let Some(mouse_pos) = ctx.pointer_latest_pos() {
                    let delta = mouse_pos - self.last_mouse_pos;
                    // Pan is in world coordinates, but damp movement as zoom increases
                    // so close-in navigation remains controllable.
                    let zoom = self.zoom.max(0.0001);
                    let zoom_damping = (1.0 / (1.0 + zoom * 4.0)).clamp(0.22, 1.0);
                    let target_delta = egui::Vec2::new(
                        (delta.x / zoom) * zoom_damping,
                        (delta.y / zoom) * zoom_damping,
                    );
                    self.pan_velocity = self.pan_velocity * 0.2 + target_delta * 0.8;
                    let pan_delta = self.pan_velocity;
                    self.pan += pan_delta;
                    self.shift_camera_lock_target_by_pan_delta(pan_delta, center3d);
                    self.last_mouse_pos = mouse_pos;
                }
            }
            if response.drag_stopped_by(egui::PointerButton::Primary) {
                self.dragging = false;
            }
            if !self.dragging {
                let pan_delta = self.pan_velocity;
                self.pan += pan_delta;
                self.shift_camera_lock_target_by_pan_delta(pan_delta, center3d);
                self.pan_velocity *= 0.84;
                if self.pan_velocity.length_sq() < 0.000001 {
                    self.pan_velocity = egui::Vec2::ZERO;
                } else {
                    ctx.request_repaint();
                }
            }

            // Handle camera rotation (right drag)
            if response.drag_started_by(egui::PointerButton::Secondary) {
                self.rotating = true;
                self.camera_focus_tween = None;
                if let Some(mouse_pos) = ctx.pointer_latest_pos() {
                    self.last_mouse_pos = mouse_pos;
                }
            }
            if self.rotating && ctx.input(|i| i.pointer.secondary_down()) {
                if let Some(mouse_pos) = ctx.pointer_latest_pos() {
                    let delta = mouse_pos - self.last_mouse_pos;
                    let zoom = self.zoom.max(0.0001);
                    let rotation_damping = (1.0 / (1.0 + zoom * 6.5)).clamp(0.05, 1.0);
                    let yaw_delta = (delta.x * 0.008 * rotation_damping).clamp(-0.05, 0.05);
                    let pitch_delta = (delta.y * 0.007 * rotation_damping).clamp(-0.04, 0.04);
                    self.yaw += yaw_delta;
                    self.pitch += pitch_delta;
                    self.pitch = self.pitch.clamp(-PI / 2.0, PI / 2.0);
                    if let Some(lock_target) = self.camera_lock_target {
                        self.recenter_camera_pan_to_target(lock_target, center3d);
                    }
                    self.last_mouse_pos = mouse_pos;
                }
            }
            if response.drag_stopped_by(egui::PointerButton::Secondary) {
                self.rotating = false;
            }

            // Handle left-click to select a star system (mutually exclusive with pan drag)
            if response.clicked_by(egui::PointerButton::Primary) {
                let is_double_click = response.double_clicked_by(egui::PointerButton::Primary);
                if let Some(click_pos) = ctx.input(|i| i.pointer.interact_pos()) {
                    // We need center2d to project, but it's computed below; use a temporary
                    // approximation based on the rect center (same value, just earlier).
                    let center2d_early = response.rect.center();
                    let black_hole_rotated = rotate_point(center3d, self.yaw, self.pitch, center3d);
                    let black_hole_draw_pos_early = center2d_early
                        + egui::Vec2::new(
                            (black_hole_rotated[0] - center3d[0] + self.pan.x) * self.zoom,
                            (black_hole_rotated[1] - center3d[1] + self.pan.y) * self.zoom,
                        );
                    let black_hole_click_radius =
                        (BLACK_HOLE_EXCLUSION_RADIUS * self.zoom).max(BLACK_HOLE_CLICK_RADIUS_MIN_PX);
                    let black_hole_delta = click_pos - black_hole_draw_pos_early;

                    if black_hole_delta.length_sq() <= black_hole_click_radius * black_hole_click_radius {
                        self.selected_system = Some(self.sagittarius_a_system_detail());
                        if is_double_click {
                            self.camera_lock_target = Some(center3d);
                            let target_zoom = self.zoom_for_system_lod();
                            self.start_camera_focus_tween(center3d, center3d, target_zoom);
                        }
                    } else {
                        let threshold_sq = CLICK_THRESHOLD_PX * CLICK_THRESHOLD_PX;

                        // Step 1: find closest render_point to click in screen space.
                        // Collect as an owned value so the render_points borrow ends here.
                        let proxy_pos: Option<[f32; 3]> = self
                            .render_points
                            .iter()
                            .filter_map(|point| {
                                let r = rotate_point(point.pos, self.yaw, self.pitch, center3d);
                                let sx = center2d_early.x
                                    + (r[0] - center3d[0] + self.pan.x) * self.zoom;
                                let sy = center2d_early.y
                                    + (r[1] - center3d[1] + self.pan.y) * self.zoom;
                                let dx = sx - click_pos.x;
                                let dy = sy - click_pos.y;
                                let dist_sq = dx * dx + dy * dy;
                                if dist_sq <= threshold_sq {
                                    Some((dist_sq, point.pos))
                                } else {
                                    None
                                }
                            })
                            .min_by(|a, b| a.0.total_cmp(&b.0))
                            .map(|(_, pos)| pos);

                        // Step 2: find the nearest actual SystemSummary in the sector cache by
                        // searching sectors around the proxy position in world space.
                        self.selected_system = None;
                        if let Some(pp) = proxy_pos {
                            let generator = Arc::clone(&self.procedural_generator);
                            let sector_x =
                                ((pp[0] - center3d[0]) / self.sector_size).floor() as i32;
                            let sector_y =
                                ((pp[1] - center3d[1]) / self.sector_size).floor() as i32;
                            let mut best: Option<(f32, SystemSummary)> = None;

                            for sdx in -1i32..=1 {
                                for sdy in -1i32..=1 {
                                    let coord = SectorCoord {
                                        x: sector_x + sdx,
                                        y: sector_y + sdy,
                                    };
                                    // Generate on demand if not yet cached (fast, deterministic).
                                    if !self.sector_cache.contains(coord) {
                                        let systems = generator.generate_sector(coord);
                                        self.sector_cache.insert(coord, systems);
                                    }
                                    if let Some(systems) = self.sector_cache.get(coord) {
                                        for sys in systems.as_ref() {
                                            let d = (sys.pos[0] - pp[0]).powi(2)
                                                + (sys.pos[1] - pp[1]).powi(2);
                                            if best.map_or(true, |(bd, _)| d < bd) {
                                                best = Some((d, *sys));
                                            }
                                        }
                                    }
                                }
                            }

                            if let Some((_, summary)) = best {
                                let mut detail =
                                    generator.generate_system_detail(&summary);
                                self.delta_store.apply_to_detail(&mut detail);
                                if is_double_click {
                                    self.camera_lock_target = Some(summary.pos);
                                    let target_zoom = self.zoom_for_system_lod();
                                    self.start_camera_focus_tween(summary.pos, center3d, target_zoom);
                                }
                                self.selected_system = Some(detail);
                            }
                        }
                    }
                }
            }

            self.tick_camera_focus_tween(ctx);

            self.update_visible_buffer(center3d, response.rect.size());
            if !self.pending_sectors.is_empty() || self.visible_build_inflight {
                ctx.request_repaint();
            }

            // Center of the painter area
            let center2d = response.rect.center();

            // Draw supermassive black hole at the galaxy center
            let black_hole_3d = rotate_point(center3d, self.yaw, self.pitch, center3d);
            let black_hole_draw_pos = center2d + egui::Vec2::new(
                (black_hole_3d[0] - center3d[0] + self.pan.x) * self.zoom,
                (black_hole_3d[1] - center3d[1] + self.pan.y) * self.zoom,
            );
            let black_hole_radius_px = BLACK_HOLE_EXCLUSION_RADIUS * self.zoom;
            // Base visual density on drawable points (what actually gets rasterized),
            // not represented systems (which can be billions in coarse LOD tiers).
            let draw_point_count = self.render_points.len().max(1) as f32;
            let draw_density = (draw_point_count / DRAW_DENSITY_REF_VISIBLE).sqrt().max(1.0);
            let star_draw_radius = (STAR_DRAW_RADIUS_PX / draw_density)
                .clamp(STAR_DRAW_RADIUS_MIN_PX, STAR_DRAW_RADIUS_PX);
            let star_alpha = (STAR_ALPHA_MAX / draw_density).clamp(STAR_ALPHA_MIN, STAR_ALPHA_MAX) as u8;

            let max_point_radius = star_draw_radius * CHUNK_RADIUS_MULTIPLIER_MAX;
            let star_cull_radius_px = black_hole_radius_px + max_point_radius + 1.0;
            let star_cull_radius_sq = star_cull_radius_px * star_cull_radius_px;

            if self.gpu_renderer_ready {
                // Late drain: pick up any build results that the worker
                // completed while the UI was rendering this frame.  This
                // reduces perceived LOD-transition latency by up to one frame.
                self.drain_visible_build_results();

                // Reuse the cached chunk uploads built in update_visible_buffer.
                // Only rebuilt when sectors/LOD actually change, not every frame.
                let chunk_uploads = std::sync::Arc::clone(&self.cached_chunk_uploads);
                let uploads_generation = self.chunk_uploads_generation;
                let random_seed = (self.galaxy_seed as u32)
                    ^ (uploads_generation as u32).wrapping_mul(0x9E37_79B9);

                let black_hole_local = egui::Vec2::new(
                    black_hole_draw_pos.x - response.rect.min.x,
                    black_hole_draw_pos.y - response.rect.min.y,
                );

                gpu_stars::add_chunked_paint_callback(
                    &painter,
                    response.rect,
                    chunk_uploads,
                    uploads_generation,
                    gpu_stars::FrameUniformInput {
                        center3d,
                        pan: self.pan,
                        zoom: self.zoom,
                        yaw: self.yaw,
                        pitch: self.pitch,
                        canvas_size: response.rect.size(),
                        black_hole_local,
                        black_hole_cull_radius: star_cull_radius_px,
                        star_draw_radius,
                        max_point_radius,
                        star_alpha: star_alpha as f32,
                        star_alpha_min: STAR_ALPHA_MIN,
                        star_alpha_max: STAR_ALPHA_MAX,
                        star_color_rgb: [1.0, 235.0 / 255.0, 120.0 / 255.0],
                        max_visible_count: self.render_budget as u32,
                        density_keep: 1.0,
                        random_seed,
                    },
                );
            } else {
                // Project and draw each visible render point on CPU when GPU renderer is unavailable.
                for point in &self.render_points {
                    let rotated = rotate_point(point.pos, self.yaw, self.pitch, center3d);
                    let draw_pos = center2d + egui::Vec2::new(
                        (rotated[0] - center3d[0] + self.pan.x) * self.zoom,
                        (rotated[1] - center3d[1] + self.pan.y) * self.zoom,
                    );
                    let delta = draw_pos - black_hole_draw_pos;
                    if delta.length_sq() <= star_cull_radius_sq {
                        continue;
                    }

                    let chunk_boost = (point.represented_systems as f32)
                        .log2()
                        .clamp(0.0, REPRESENTED_BOOST_LOG2_CAP);
                    let point_radius =
                        (star_draw_radius * (1.0 + chunk_boost * REPRESENTED_RADIUS_BOOST_SCALE))
                        .clamp(star_draw_radius, max_point_radius);
                    let point_alpha =
                        (star_alpha as f32 * (1.0 + chunk_boost * REPRESENTED_ALPHA_BOOST_SCALE))
                        .clamp(STAR_ALPHA_MIN, STAR_ALPHA_MAX) as u8;
                    let point_color = egui::Color32::from_rgba_unmultiplied(255, 235, 120, point_alpha);

                    painter.circle_filled(draw_pos, point_radius, point_color);
                }
            }

            painter.circle_filled(black_hole_draw_pos, black_hole_radius_px, egui::Color32::BLACK);
            painter.circle_stroke(
                black_hole_draw_pos,
                black_hole_radius_px,
                egui::Stroke::new(2.0, egui::Color32::WHITE),
            );

            // Draw colony markers so colonized systems are visible in galaxy view.
            for colony in self.game_state.colonies.values() {
                let rotated = rotate_point(colony.system_pos, self.yaw, self.pitch, center3d);
                let colony_screen = center2d
                    + egui::Vec2::new(
                        (rotated[0] - center3d[0] + self.pan.x) * self.zoom,
                        (rotated[1] - center3d[1] + self.pan.y) * self.zoom,
                    );
                let is_player_colony = colony.owner_faction == self.game_state.player.faction_id;
                let marker_color = if is_player_colony {
                    egui::Color32::from_rgb(70, 210, 170)
                } else {
                    egui::Color32::from_rgb(226, 160, 82)
                };
                let marker_radius = if self.game_state.player.starting_colony_id == Some(colony.id) {
                    6.0
                } else {
                    4.2
                };
                painter.circle_stroke(
                    colony_screen,
                    marker_radius,
                    egui::Stroke::new(1.5, marker_color),
                );
                painter.line_segment(
                    [
                        colony_screen + egui::Vec2::new(-marker_radius, 0.0),
                        colony_screen + egui::Vec2::new(marker_radius, 0.0),
                    ],
                    egui::Stroke::new(1.0, marker_color),
                );
                painter.line_segment(
                    [
                        colony_screen + egui::Vec2::new(0.0, -marker_radius),
                        colony_screen + egui::Vec2::new(0.0, marker_radius),
                    ],
                    egui::Stroke::new(1.0, marker_color),
                );
            }
            painter.text(
                black_hole_draw_pos + egui::Vec2::new(black_hole_radius_px + 6.0, -8.0),
                egui::Align2::LEFT_TOP,
                SAGITTARIUS_A_NAME,
                egui::FontId::proportional(11.0),
                egui::Color32::from_rgb(188, 214, 246),
            );

            // Draw selection ring around the selected system.
            if let Some(detail) = &self.selected_system {
                let rotated = rotate_point(detail.pos, self.yaw, self.pitch, center3d);
                let sel_screen = center2d
                    + egui::Vec2::new(
                        (rotated[0] - center3d[0] + self.pan.x) * self.zoom,
                        (rotated[1] - center3d[1] + self.pan.y) * self.zoom,
                    );
                if let Some(nearest) = self.game_state.nearest_colony_for_faction(
                    &self.game_state.player.faction_id,
                    detail.pos,
                ) {
                    let nearest_rotated = rotate_point(
                        nearest.system_pos,
                        self.yaw,
                        self.pitch,
                        center3d,
                    );
                    let nearest_screen = center2d
                        + egui::Vec2::new(
                            (nearest_rotated[0] - center3d[0] + self.pan.x) * self.zoom,
                            (nearest_rotated[1] - center3d[1] + self.pan.y) * self.zoom,
                        );
                    let range_world = self
                        .game_state
                        .faction_colonization_range_world(&self.game_state.player.faction_id);
                    let in_range = nearest.distance <= range_world;
                    let range_color = if in_range {
                        egui::Color32::from_rgba_unmultiplied(80, 220, 110, 80)
                    } else {
                        egui::Color32::from_rgba_unmultiplied(230, 120, 100, 80)
                    };
                    let range_radius_px = (range_world * self.zoom).max(4.0);
                    painter.circle_stroke(
                        nearest_screen,
                        range_radius_px,
                        egui::Stroke::new(1.3, range_color),
                    );
                    painter.line_segment(
                        [nearest_screen, sel_screen],
                        egui::Stroke::new(1.0, range_color),
                    );
                }
                let sel_ring_radius = (star_draw_radius * 4.0).max(6.0);
                painter.circle_stroke(
                    sel_screen,
                    sel_ring_radius,
                    egui::Stroke::new(1.5, egui::Color32::from_rgb(80, 200, 255)),
                );
                // Label the selected system near the ring.
                painter.text(
                    sel_screen + egui::Vec2::new(sel_ring_radius + 3.0, -6.0),
                    egui::Align2::LEFT_TOP,
                    &detail.display_name,
                    egui::FontId::proportional(11.0),
                    egui::Color32::from_rgb(180, 230, 255),
                );
            }

            // System detail window (floats over the galaxy view).
            let selected_survey_snapshot = self.selected_system.as_ref().map(|detail| {
                (
                    self.game_state.survey_stage(detail.id),
                    self.game_state.survey_record(detail.id).cloned(),
                    self.game_state.pending_scan_for(detail.id).cloned(),
                    self.game_state.pending_scan_progress(detail.id),
                    self.game_state.pending_colony_founding_for_system(detail.id).cloned(),
                    self.game_state.current_year,
                )
            });
            let mut window_close = false;
            let mut window_save = false;
            let mut window_travel = false;
            let mut window_set_home_system = false;
            let mut window_advance_survey = false;
            let mut window_found_colony = false;
            if let Some(detail) = self.selected_system.as_mut() {
                let (survey_stage, survey_record, pending_scan, pending_progress, pending_colony_founding, current_year) = selected_survey_snapshot
                    .clone()
                    .unwrap_or((SurveyStage::Unknown, None, None, None, None, self.game_state.current_year));
                let home_system = self.game_state.player.home_system;
                // Before the first colony is founded, all system details are
                // visible globally so the player can scout for a good home
                // system.  Once a colony exists the fog of war returns.
                let pre_colony_mode = self.game_state.player.starting_colony_id.is_none();
                let display_survey_stage = if pre_colony_mode {
                    SurveyStage::ColonyAssessment
                } else {
                    survey_stage
                };
                let can_advance_survey = !pre_colony_mode
                    && survey_stage.next().is_some()
                    && (home_system.is_some() || survey_stage != SurveyStage::Unknown)
                    && pending_scan.is_none();
                egui::Window::new("System Details")
                    .resizable(true)
                    .default_width(360.0)
                    .show(ui.ctx(), |ui| {
                        egui::Grid::new("sys_info")
                            .num_columns(2)
                            .spacing([8.0, 4.0])
                            .show(ui, |ui| {
                                ui.label("Name:");
                                ui.text_edit_singleline(&mut detail.display_name);
                                ui.end_row();
                                ui.label("Canonical ID:");
                                ui.label(&detail.canonical_name);
                                ui.end_row();
                                ui.label("Position:");
                                ui.label(format!(
                                    "[{:.0}, {:.0}, {:.0}]",
                                    detail.pos[0], detail.pos[1], detail.pos[2]
                                ));
                                ui.end_row();
                            });

                        if !pre_colony_mode {
                            ui.separator();
                            ui.label(egui::RichText::new("Survey Status").strong());
                            egui::Grid::new("survey_status")
                                .num_columns(2)
                                .spacing([8.0, 4.0])
                                .show(ui, |ui| {
                                    ui.label("Home system:");
                                    ui.label(if home_system.is_some() { "Selected" } else { "Not set" });
                                    ui.end_row();

                                    ui.label("Stage:");
                                    ui.label(survey_stage.label());
                                    ui.end_row();

                                    ui.label("Surveyed bodies:");
                                    ui.label(
                                        survey_record
                                            .as_ref()
                                            .map(|record| record.surveyed_body_count.to_string())
                                            .unwrap_or_else(|| "0".to_owned()),
                                    );
                                    ui.end_row();

                                    ui.label("Habitable worlds:");
                                    ui.label(
                                        survey_record
                                            .as_ref()
                                            .map(|record| record.habitable_body_count.to_string())
                                            .unwrap_or_else(|| "0".to_owned()),
                                    );
                                    ui.end_row();

                                    ui.label("Scan queue:");
                                    if let Some(scan) = pending_scan.as_ref() {
                                        let progress = pending_progress.unwrap_or(0.0);
                                        let eta_years = (scan.complete_year - current_year).max(0.0);
                                        ui.label(format!(
                                            "{} ({:.0}%) · ETA {:.2}y",
                                            scan.target_stage.label(),
                                            progress * 100.0,
                                            eta_years,
                                        ));
                                    } else {
                                        ui.label("Idle");
                                    }
                                    ui.end_row();
                                });

                            if let Some(next_stage) = survey_stage.next() {
                                let (_, reward_tech) = GameState::survey_stage_rewards(next_stage);
                                ui.label(format!(
                                    "Next survey reward: +{:.2} technology points",
                                    reward_tech
                                ));
                            }
                        }

                        if display_survey_stage < SurveyStage::StellarSurvey {
                            ui.label("Run a stellar survey to reveal star classification and luminosity.");
                        } else {
                            ui.separator();
                            ui.label(egui::RichText::new("Stars").strong());
                            for star in &detail.stars {
                                let class = star.class.notation();
                                ui.label(format!(
                                    "  {class} — {:.2} M☉   {:.1} L☉",
                                    star.mass_solar, star.luminosity_solar
                                ));
                            }
                        }

                        ui.separator();
                        ui.label(egui::RichText::new("Planets").strong());
                        if display_survey_stage < SurveyStage::PlanetarySurvey {
                            ui.label("  Planetary data unavailable until world survey is completed");
                        } else if detail.planets.is_empty() {
                            ui.label("  No planets detected");
                        } else {
                            egui::Grid::new("planets")
                                .num_columns(5)
                                .spacing([6.0, 2.0])
                                .show(ui, |ui| {
                                    ui.label(egui::RichText::new("Type").underline());
                                    ui.label(egui::RichText::new("Around").underline());
                                    ui.label(egui::RichText::new("Orbit (AU)").underline());
                                    ui.label(egui::RichText::new("Temp (K)").underline());
                                    ui.label(egui::RichText::new("Habitable").underline());
                                    ui.end_row();
                                    for planet in &detail.planets {
                                        let kind = planet.kind.label();
                                        let around = match planet.host_planet_index {
                                            Some(host_index) => {
                                                format!("P{}", host_index as usize + 1)
                                            }
                                            None => "Star".to_owned(),
                                        };
                                        let orbit_label = match planet.moon_orbit_au {
                                            Some(moon_orbit_au) => {
                                                format!("{moon_orbit_au:.3} (m)")
                                            }
                                            None => format!("{:.2}", planet.orbit_au),
                                        };
                                        ui.label(kind);
                                        ui.label(around);
                                        ui.label(orbit_label);
                                        ui.label(format!("{:.0}", planet.temperature_k));
                                        ui.label(if planet.kind == PlanetKind::EarthLikeWorld {
                                            "ELW"
                                        } else if planet.habitable {
                                            "★"
                                        } else {
                                            ""
                                        });
                                        ui.end_row();
                                    }
                                });
                        }

                        ui.separator();
                        if display_survey_stage < SurveyStage::ColonyAssessment {
                            ui.label("Colony founding requires a completed colony assessment.");
                        } else if survey_record
                            .as_ref()
                            .and_then(|record| record.viable_body_index)
                            .is_some()
                        {
                            ui.label("A colony site has been confirmed.");
                        } else {
                            ui.label("Colony assessment complete: use an orbital habitat fallback site.");
                        }

                        if let Some(pending) = pending_colony_founding.as_ref() {
                            let eta = (pending.complete_year - current_year).max(0.0);
                            if let Some(source_colony_id) = pending.source_colony_id {
                                ui.label(format!(
                                    "Expedition in transit from colony #{} with {} colonists · ETA {:.2}y",
                                    source_colony_id,
                                    pending.colonists_sent,
                                    eta
                                ));
                            } else {
                                ui.label(format!(
                                    "Home-system colony expedition in transit with {} colonists · ETA {:.2}y",
                                    pending.colonists_sent,
                                    eta
                                ));
                            }
                        } else {
                            let mut player_colonies = self
                                .game_state
                                .colonies
                                .values()
                                .filter(|colony| colony.owner_faction == self.game_state.player.faction_id)
                                .map(|colony| {
                                    (
                                        colony.id,
                                        colony.name.clone(),
                                        colony.population.max(0.0) as u64,
                                    )
                                })
                                .collect::<Vec<_>>();
                            player_colonies.sort_by_key(|(id, _, _)| *id);

                            if !player_colonies.is_empty() {
                                if self.colony_source_selection.is_none() {
                                    self.colony_source_selection = Some(player_colonies[0].0);
                                }
                                if let Some(selected_id) = self.colony_source_selection {
                                    if !player_colonies.iter().any(|(id, _, _)| *id == selected_id) {
                                        self.colony_source_selection = Some(player_colonies[0].0);
                                    }
                                }

                                let source_label = self
                                    .colony_source_selection
                                    .and_then(|id| {
                                        player_colonies
                                            .iter()
                                            .find(|(candidate_id, _, _)| *candidate_id == id)
                                            .map(|(_, name, pop)| format!("{} (pop {})", name, pop))
                                    })
                                    .unwrap_or_else(|| "Select source colony".to_owned());

                                ui.horizontal(|ui| {
                                    ui.label("Source colony:");
                                    egui::ComboBox::from_id_source("colony_source_selector")
                                        .selected_text(source_label)
                                        .show_ui(ui, |ui| {
                                            for (id, name, pop) in &player_colonies {
                                                ui.selectable_value(
                                                    &mut self.colony_source_selection,
                                                    Some(*id),
                                                    format!("{} (pop {})", name, pop),
                                                );
                                            }
                                        });
                                });

                                self.colony_transfer_colonists = self
                                    .colony_transfer_colonists
                                    .clamp(COLONY_TRANSFER_POP_MIN, COLONY_TRANSFER_POP_MAX);
                                ui.add(
                                    egui::Slider::new(
                                        &mut self.colony_transfer_colonists,
                                        COLONY_TRANSFER_POP_MIN..=COLONY_TRANSFER_POP_MAX,
                                    )
                                    .text("Colonists to send"),
                                );
                                ui.small("Minimum transfer is 100 colonists.");
                            } else {
                                ui.label("No source colony available yet for colonization expeditions.");
                            }
                        }
                        ui.add_enabled(
                            !pre_colony_mode,
                            egui::Checkbox::new(&mut detail.favorite, "Favorite"),
                        );

                        ui.separator();
                        ui.label("Notes:");
                        if detail.note.is_none() {
                            detail.note = Some(String::new());
                        }
                        if let Some(note) = &mut detail.note {
                            ui.text_edit_multiline(note);
                        }
                        // Collapse empty note to None
                        if detail
                            .note
                            .as_deref()
                            .map(str::trim)
                            .unwrap_or("")
                            .is_empty()
                        {
                            detail.note = None;
                        }

                        ui.separator();
                        ui.horizontal(|ui| {
                            if ui.button("Save").clicked() {
                                window_save = true;
                            }
                            if ui
                                .add_enabled(
                                    home_system.is_none(),
                                    egui::Button::new("Set Home System"),
                                )
                                .clicked()
                            {
                                window_set_home_system = true;
                            }
                            if !pre_colony_mode {
                                if ui
                                    .add_enabled(
                                        can_advance_survey,
                                        egui::Button::new(survey_stage.action_label()),
                                    )
                                    .clicked()
                                {
                                    window_advance_survey = true;
                                }
                            }
                            if ui.button("Found Colony").clicked() {
                                window_found_colony = true;
                            }
                            if ui
                                .add_enabled(
                                    display_survey_stage >= SurveyStage::ColonyAssessment,
                                    egui::Button::new("Travel"),
                                )
                                .clicked()
                            {
                                window_travel = true;
                            }
                            if ui.button("Close").clicked() {
                                window_close = true;
                            }
                        });
                    });
            }

            // Apply deferred window actions.
            if window_save {
                if let Some(detail) = &self.selected_system {
                    let delta = SystemDelta {
                        rename_to: if detail.display_name != detail.canonical_name {
                            Some(detail.display_name.clone())
                        } else {
                            None
                        },
                        explored: None,
                        favorite: if detail.favorite { Some(true) } else { None },
                        note: detail.note.clone(),
                    };
                    self.delta_store.upsert(detail.id, delta);
                    let _ = self.delta_store.save_json(DELTA_SAVE_PATH);
                }
            }
            if window_travel {
                if let Some(detail) = &self.selected_system {
                    let pre_colony = self.game_state.player.starting_colony_id.is_none();
                    if !pre_colony && self.game_state.survey_stage(detail.id) < SurveyStage::ColonyAssessment {
                        self.game_notice = Some(
                            "Travel unavailable: complete all scans for this system first.".to_owned(),
                        );
                        return;
                    }
                    self.travel_system = Some(detail.clone());
                    self.travel_window_open = true;
                    self.travel_paused = false;
                    self.travel_time_scale = TRAVEL_TIME_SCALE_DEFAULT;
                    self.reset_travel_view();
                    self.travel_sim_years = 0.0;
                    self.travel_last_input_time = None;
                    self.travel_selected_body = None;
                }
            }
            if window_set_home_system {
                if let Some(detail) = self.selected_system.as_ref() {
                    let system_id = detail.id;
                    if self.game_state.player.home_system.is_none() {
                        self.record_game_event(GameEvent::HomeSystemSelected {
                            at_year: self.game_state.current_year,
                            system: system_id,
                        });
                        self.game_notice = Some(
                            "Home system selected. Chart nearby systems within range.".to_owned(),
                        );
                    } else {
                        self.game_notice = Some("Home system is already set.".to_owned());
                    }
                }
            }
            if window_advance_survey {
                if let Some(detail) = self.selected_system.clone() {
                    let current_stage = self.game_state.survey_stage(detail.id);
                    if let Some(next_stage) = current_stage.next() {
                        let mut charting_duration_scale = 1.0;
                        let mut survey_resource_cost = match next_stage {
                            SurveyStage::StellarSurvey => SURVEY_STAGE_STELLAR_COST_BASE,
                            SurveyStage::PlanetarySurvey => SURVEY_STAGE_PLANETARY_COST_BASE,
                            SurveyStage::ColonyAssessment => SURVEY_STAGE_ASSESSMENT_COST_BASE,
                            _ => 0,
                        };
                        if next_stage == SurveyStage::Located {
                            let mut nearest_anchor: Option<(&'static str, f32)> = None;

                            if let Some(home_system) = self.game_state.player.home_system {
                                let Some(home_detail) = self.load_system_detail_by_id(home_system) else {
                                    self.game_notice = Some(
                                        "Unable to load home system data for range check.".to_owned(),
                                    );
                                    return;
                                };
                                nearest_anchor = Some(("home", world_distance(detail.pos, home_detail.pos)));
                            }

                            if let Some(nearest_colony) = self
                                .game_state
                                .nearest_colony_for_faction(&self.game_state.player.faction_id, detail.pos)
                            {
                                nearest_anchor = match nearest_anchor {
                                    Some((kind, current_distance)) => {
                                        if nearest_colony.distance < current_distance {
                                            Some(("colony", nearest_colony.distance))
                                        } else {
                                            Some((kind, current_distance))
                                        }
                                    }
                                    None => Some(("colony", nearest_colony.distance)),
                                };
                            }

                            let Some((anchor_kind, anchor_distance)) = nearest_anchor else {
                                self.game_notice = Some(
                                    "Charting denied: establish a home system or colony first.".to_owned(),
                                );
                                return;
                            };

                            let player_range = self
                                .game_state
                                .faction_colonization_range_world(&self.game_state.player.faction_id);
                            let charting_limit = player_range.min(CHARTING_RANGE_WORLD_MAX);
                            if anchor_distance > charting_limit {
                                self.game_notice = Some(format!(
                                    "Charting denied: nearest {} anchor is {:.0} units away (charting limit {:.0}).",
                                    anchor_kind, anchor_distance, charting_limit
                                ));
                                return;
                            }

                            let normalized_distance = if charting_limit > 0.0 {
                                (anchor_distance / charting_limit).clamp(0.0, 1.0)
                            } else {
                                1.0
                            };
                            charting_duration_scale =
                                1.0 + normalized_distance * (CHARTING_DISTANCE_TIME_MULTIPLIER_MAX - 1.0);
                            survey_resource_cost = ((CHARTING_RESOURCE_COST_BASE as f32)
                                * (1.0
                                    + normalized_distance
                                        * (CHARTING_RESOURCE_COST_MULTIPLIER_MAX - 1.0)))
                                .round() as i64;
                        }

                        let faction_treasury_available = self
                            .game_state
                            .factions
                            .get(&self.game_state.player.faction_id)
                            .map(|faction| faction.treasury)
                            .unwrap_or(0);
                        if faction_treasury_available < survey_resource_cost {
                            let action_text = if next_stage == SurveyStage::Located {
                                "Charting"
                            } else {
                                "Survey"
                            };
                            self.game_notice = Some(format!(
                                "{} denied: requires {} treasury resources (available {}).",
                                action_text,
                                survey_resource_cost,
                                faction_treasury_available
                            ));
                            return;
                        }

                        let surveyed_body_count = if next_stage >= SurveyStage::PlanetarySurvey {
                            detail.planets.len().min(u16::MAX as usize) as u16
                        } else {
                            0
                        };
                        let viable_body_index = if next_stage >= SurveyStage::ColonyAssessment {
                            detail
                                .planets
                                .iter()
                                .position(|planet| planet.kind == PlanetKind::EarthLikeWorld)
                                .or_else(|| detail.planets.iter().position(|planet| planet.habitable))
                                .or_else(|| {
                                    if detail.planets.is_empty() {
                                        None
                                    } else {
                                        Some(0)
                                    }
                                })
                                .map(|index| index as u16)
                                .or_else(|| {
                                    if detail.planets.is_empty() {
                                        Some(u16::MAX)
                                    } else {
                                        None
                                    }
                                })
                        } else {
                            None
                        };
                        let habitable_body_count = if next_stage >= SurveyStage::PlanetarySurvey {
                            detail
                                .planets
                                .iter()
                                .filter(|planet| planet.habitable)
                                .count()
                                .min(u16::MAX as usize) as u16
                        } else {
                            0
                        };

                        match self.game_state.queue_survey_scan(
                            detail.id,
                            self.game_state.player_faction_name().to_owned(),
                            self.game_state.current_year,
                            next_stage,
                            surveyed_body_count,
                            habitable_body_count,
                            viable_body_index,
                            charting_duration_scale,
                        ) {
                            Ok(duration_years) => {
                                if survey_resource_cost > 0 {
                                    if let Some(faction) = self
                                        .game_state
                                        .factions
                                        .get_mut(&self.game_state.player.faction_id)
                                    {
                                        faction.treasury = faction
                                            .treasury
                                            .saturating_sub(survey_resource_cost);
                                    }
                                    self.game_autosave_accum_years = 0.0;
                                    self.persist_game_state();
                                }
                                self.game_notice = Some(format!(
                                    "{} queued. Completion in {:.2} strategic years. Treasury cost: {}.",
                                    current_stage.action_label(),
                                    duration_years,
                                    survey_resource_cost.max(0)
                                ));
                            }
                            Err(message) => {
                                self.game_notice = Some(message.to_owned());
                            }
                        }

                        if let Some(selected) = self.selected_system.as_mut() {
                            selected.explored = self.game_state.is_system_explored(selected.id);
                        }
                    } else {
                        self.game_notice = Some("This system has already been fully assessed.".to_owned());
                    }
                }
            }
            if window_found_colony {
                if let Some(detail) = self.selected_system.clone() {
                    let pre_colony = self.game_state.player.starting_colony_id.is_none();
                    let survey_stage = self.game_state.survey_stage(detail.id);
                    if !pre_colony && survey_stage < SurveyStage::ColonyAssessment {
                        self.game_notice = Some(
                            "Colonization denied: complete a colony assessment first.".to_owned(),
                        );
                        return;
                    }

                    let candidate_body = self
                        .game_state
                        .colony_candidate_body(detail.id)
                        .or_else(|| {
                            if detail.planets.is_empty() {
                                Some(u16::MAX)
                            } else {
                                Some(0)
                            }
                        });

                    if let Some(body_index) = candidate_body {
                        if self.game_state.has_colony_at(detail.id, body_index)
                            || self
                                .game_state
                                .pending_colony_founding_for_target(detail.id, body_index)
                                .is_some()
                        {
                            self.game_notice = Some(
                                "A colony already exists or is already being established at this colony site."
                                    .to_owned(),
                            );
                        } else {
                            let is_home_system_colony = self
                                .game_state
                                .player
                                .home_system
                                .map(|home_system| home_system == detail.id)
                                .unwrap_or(false);

                            let mut source_colony_id_opt: Option<u64> = None;
                            let mut colonists_to_send = self
                                .colony_transfer_colonists
                                .clamp(COLONY_TRANSFER_POP_MIN, COLONY_TRANSFER_POP_MAX);
                            let source_distance = if is_home_system_colony {
                                colonists_to_send = COLONY_TRANSFER_POP_MIN;
                                0.0
                            } else {
                                let Some(source_colony_id) = self.colony_source_selection else {
                                    self.game_notice = Some(
                                        "Select a source colony for this expedition first.".to_owned(),
                                    );
                                    return;
                                };

                                let Some(source_colony) = self.game_state.colonies.get(&source_colony_id) else {
                                    self.game_notice = Some(
                                        "Selected source colony could not be found.".to_owned(),
                                    );
                                    return;
                                };
                                if source_colony.owner_faction != self.game_state.player.faction_id {
                                    self.game_notice = Some(
                                        "Selected source colony is not owned by your faction.".to_owned(),
                                    );
                                    return;
                                }

                                let source_population = source_colony.population.max(0.0) as u64;
                                if source_population
                                    < (colonists_to_send as u64 + COLONY_TRANSFER_POP_MIN as u64)
                                {
                                    self.game_notice = Some(format!(
                                        "Colonization denied: source colony needs at least {} population plus {} reserve.",
                                        colonists_to_send,
                                        COLONY_TRANSFER_POP_MIN,
                                    ));
                                    return;
                                }

                                source_colony_id_opt = Some(source_colony_id);
                                world_distance(source_colony.system_pos, detail.pos)
                            };

                            let allowed_range = self
                                .game_state
                                .faction_colonization_range_world(&self.game_state.player.faction_id);
                            if source_distance > allowed_range {
                                self.game_notice = Some(format!(
                                    "Colonization denied: source colony is {:.0} units from target (limit {:.0}).",
                                    source_distance,
                                    allowed_range,
                                ));
                                return;
                            }

                            let colony_id = self.game_state.reserve_colony_id();
                            let colony_name = format!("{} Colony {}", detail.display_name, colony_id);
                            let habitable_site = if body_index == u16::MAX {
                                false
                            } else {
                                detail
                                    .planets
                                    .get(body_index as usize)
                                    .map(|planet| planet.habitable)
                                    .unwrap_or(false)
                            };
                            let earth_like_world = if body_index == u16::MAX {
                                false
                            } else {
                                detail
                                    .planets
                                    .get(body_index as usize)
                                    .map(|planet| planet.kind == PlanetKind::EarthLikeWorld)
                                    .unwrap_or(false)
                            };
                            let element_resource_profile = if body_index == u16::MAX {
                                HashMap::new()
                            } else {
                                detail
                                    .planets
                                    .get(body_index as usize)
                                    .map(|planet| {
                                        planet
                                            .composition
                                            .iter()
                                            .map(|component| {
                                                (component.symbol.clone(), component.percent.max(0.0))
                                            })
                                            .collect::<HashMap<_, _>>()
                                    })
                                    .unwrap_or_default()
                            };
                            let atmosphere_resource_profile = if body_index == u16::MAX {
                                HashMap::new()
                            } else {
                                detail
                                    .planets
                                    .get(body_index as usize)
                                    .map(|planet| {
                                        planet
                                            .atmosphere
                                            .iter()
                                            .map(|gas| (gas.formula.clone(), gas.percent.max(0.0)))
                                            .collect::<HashMap<_, _>>()
                                    })
                                    .unwrap_or_default()
                            };
                            let atmosphere_pressure_atm = if body_index == u16::MAX {
                                0.0
                            } else {
                                detail
                                    .planets
                                    .get(body_index as usize)
                                    .map(|planet| planet.atmosphere_pressure_atm.max(0.0))
                                    .unwrap_or(0.0)
                            };

                            let normalized_distance = if allowed_range > 0.0 {
                                (source_distance / allowed_range).clamp(0.0, 1.0)
                            } else {
                                1.0
                            };
                            let treasury_cost = if is_home_system_colony {
                                0
                            } else {
                                ((COLONY_ESTABLISH_RESOURCE_COST_BASE as f32)
                                    * (1.0
                                        + normalized_distance
                                            * (COLONY_ESTABLISH_RESOURCE_COST_MULTIPLIER_MAX - 1.0)))
                                    .round() as i64
                            };
                            let faction_treasury_available = self
                                .game_state
                                .factions
                                .get(&self.game_state.player.faction_id)
                                .map(|faction| faction.treasury)
                                .unwrap_or(0);

                            if faction_treasury_available < treasury_cost {
                                self.game_notice = Some(format!(
                                    "Colonization denied: requires {} treasury resources (available {}).",
                                    treasury_cost,
                                    faction_treasury_available,
                                ));
                                return;
                            }

                            let transfer_duration = COLONY_TRANSFER_TIME_YEARS_BASE
                                * (1.0
                                    + normalized_distance
                                        * (COLONY_TRANSFER_TIME_YEARS_MULTIPLIER_MAX - 1.0));

                            let pending = PendingColonyFounding {
                                colony_id,
                                colony_name,
                                founder_faction: self.game_state.player.faction_id.clone(),
                                system: detail.id,
                                body_index,
                                habitable_site,
                                earth_like_world,
                                system_pos: detail.pos,
                                element_resource_profile,
                                atmosphere_resource_profile,
                                atmosphere_pressure_atm,
                                source_colony_id: source_colony_id_opt,
                                colonists_sent: colonists_to_send,
                                start_year: self.game_state.current_year,
                                complete_year: self.game_state.current_year + transfer_duration,
                            };

                            match self.game_state.queue_colony_founding(self.game_state.current_year, pending) {
                                Ok(duration_years) => {
                                    if let Some(faction) = self
                                        .game_state
                                        .factions
                                        .get_mut(&self.game_state.player.faction_id)
                                    {
                                        faction.treasury = faction.treasury.saturating_sub(treasury_cost);
                                    }
                                    self.game_autosave_accum_years = 0.0;
                                    self.persist_game_state();

                                    self.game_notice = Some(format!(
                                        "Colony expedition launched{} with {} colonists. ETA {:.2}y. Treasury cost: {}{}.",
                                        source_colony_id_opt
                                            .map(|id| format!(" from #{}", id))
                                            .unwrap_or_else(|| " (home-system bypass)".to_owned()),
                                        colonists_to_send,
                                        duration_years,
                                        treasury_cost,
                                        if is_home_system_colony {
                                            " (home system colony is free)"
                                        } else {
                                            ""
                                        }
                                    ));
                                }
                                Err(message) => {
                                    self.game_notice = Some(message.to_owned());
                                }
                            }
                        }
                    } else {
                        self.game_notice = Some(
                            "Colonization denied: no colony site could be determined for this system."
                                .to_owned(),
                        );
                    }
                }
            }
            if window_close {
                self.selected_system = None;
            }
        });

        self.show_travel_window(ctx);
        self.show_resources_window(ctx);
        self.show_colonies_window(ctx, center3d);
        self.show_favorites_window(ctx, center3d);
        self.show_construction_window(ctx);
        self.show_settings_window(ctx);

        // Keep simulation and rendering advancing even when there is no input.
        // eframe/egui is otherwise event-driven and may sleep between interactions.
        ctx.request_repaint_after(Duration::from_millis(16));
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        renderer: eframe::Renderer::Wgpu,
        vsync: false,
        wgpu_options: eframe::egui_wgpu::WgpuConfiguration {
            device_descriptor: Arc::new(|adapter| {
                let base_limits = if adapter.get_info().backend == eframe::egui_wgpu::wgpu::Backend::Gl {
                    eframe::egui_wgpu::wgpu::Limits::downlevel_webgl2_defaults()
                } else {
                    eframe::egui_wgpu::wgpu::Limits::default()
                };

                eframe::egui_wgpu::wgpu::DeviceDescriptor {
                    label: Some("egui wgpu device"),
                    required_features: eframe::egui_wgpu::wgpu::Features::empty(),
                    required_limits: base_limits,
                }
            }),
            power_preference: eframe::egui_wgpu::wgpu::PowerPreference::default(),
            ..Default::default()
        },
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 720.0])
            .with_max_inner_size([1920.0, 1080.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Milky Way Simulator",
        options,
        Box::new(|cc| Box::new(GalaxyApp::new(cc))),
    )
}