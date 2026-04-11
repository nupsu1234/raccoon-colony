#![allow(dead_code)]

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Normal};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::f32::consts::PI;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

const SECTOR_HASH_X: u64 = 0x9E37_79B9_7F4A_7C15;
const SECTOR_HASH_Y: u64 = 0xC2B2_AE3D_27D4_EB4F;
const SYSTEM_HASH_I: u64 = 0xD6E8_FEB8_6659_FD93;
const MIX1: u64 = 0xBF58_476D_1CE4_E5B9;
const MIX2: u64 = 0x94D0_49BB_1331_11EB;
const SECTOR_DOMAIN_TAG: u64 = 0x19E6_52C3_FCAB_4A1D;
const SPAWN_CELL_DOMAIN_TAG: u64 = 0x4C7A_91F2_6DB8_3E05;
const SYSTEM_DOMAIN_TAG: u64 = 0x6F18_BB6E_2A11_91C3;
const GENERATION_VERSION: u64 = 1;
const POSITION_REJECTION_TRIES: usize = 6;
const SECTOR_DENSITY_SAMPLES_PER_AXIS: usize = 12;
const POSITION_FALLBACK_SAMPLES_PER_AXIS: usize = 4;
const POSITION_SUBCELLS_PER_AXIS: usize = 8;
const ARM_DENSITY_SHARPNESS: f32 = 6.0;
const TARGET_REPRESENTED_SYSTEMS_PER_POINT: u64 = 50_000;
const SPAWN_CELL_SIZE: f32 = 320.0;

#[inline]
fn mix64(mut z: u64) -> u64 {
    z ^= z >> 30;
    z = z.wrapping_mul(MIX1);
    z ^= z >> 27;
    z = z.wrapping_mul(MIX2);
    z ^ (z >> 31)
}

#[inline]
fn wrap_angle_radians(radians: f32) -> f32 {
    let two_pi = 2.0 * PI;
    (radians + PI).rem_euclid(two_pi) - PI
}

#[inline]
fn unit_f32_from_u64(seed: u64) -> f32 {
    let mantissa24 = (seed >> 40) as u32;
    mantissa24 as f32 / ((1u32 << 24) - 1) as f32
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SectorCoord {
    pub x: i32,
    pub y: i32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SystemId {
    pub sector: SectorCoord,
    pub local_index: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct SpawnCellCoord {
    x: i32,
    y: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SpectralClass {
    BH,
    NS,
    O,
    B,
    A,
    F,
    G,
    K,
    M,
    W,
    WN,
    WC,
    WO,
    L,
    T,
    Y,
    C,
    S,
    D,
    DA,
    DB,
    DC,
}

impl SpectralClass {
    pub fn definition(self) -> &'static SpectralClassDefinition {
        match self {
            SpectralClass::BH => &SPECTRAL_DEF_BH,
            SpectralClass::NS => &SPECTRAL_DEF_NS,
            SpectralClass::O => &SPECTRAL_DEF_O,
            SpectralClass::B => &SPECTRAL_DEF_B,
            SpectralClass::A => &SPECTRAL_DEF_A,
            SpectralClass::F => &SPECTRAL_DEF_F,
            SpectralClass::G => &SPECTRAL_DEF_G,
            SpectralClass::K => &SPECTRAL_DEF_K,
            SpectralClass::M => &SPECTRAL_DEF_M,
            SpectralClass::W => &SPECTRAL_DEF_W,
            SpectralClass::WN => &SPECTRAL_DEF_WN,
            SpectralClass::WC => &SPECTRAL_DEF_WC,
            SpectralClass::WO => &SPECTRAL_DEF_WO,
            SpectralClass::L => &SPECTRAL_DEF_L,
            SpectralClass::T => &SPECTRAL_DEF_T,
            SpectralClass::Y => &SPECTRAL_DEF_Y,
            SpectralClass::C => &SPECTRAL_DEF_C,
            SpectralClass::S => &SPECTRAL_DEF_S,
            SpectralClass::D => &SPECTRAL_DEF_D,
            SpectralClass::DA => &SPECTRAL_DEF_DA,
            SpectralClass::DB => &SPECTRAL_DEF_DB,
            SpectralClass::DC => &SPECTRAL_DEF_DC,
        }
    }

    pub fn code(self) -> &'static str {
        self.definition().code
    }

    pub fn is_scoopable(self) -> bool {
        self.definition().is_scoopable
    }

    pub fn visual_color_rgb(self) -> [u8; 3] {
        self.definition().visual_color
    }
}

pub struct SpectralClassDefinition {
    pub code: &'static str,
    pub visual_color: [u8; 3],
    pub is_scoopable: bool,
    /// Mass range (solar masses) for star generation. `None` for BH/NS (handled specially).
    pub mass_range: Option<(f32, f32)>,
    /// Luminosity range (solar) for star generation. `None` for BH/NS (handled specially).
    pub luminosity_range: Option<(f32, f32)>,
}

const SPECTRAL_DEF_BH: SpectralClassDefinition = SpectralClassDefinition {
    code: "BH", visual_color: [28, 32, 44], is_scoopable: false,
    mass_range: None, luminosity_range: None,
};
const SPECTRAL_DEF_NS: SpectralClassDefinition = SpectralClassDefinition {
    code: "NS", visual_color: [198, 226, 255], is_scoopable: false,
    mass_range: None, luminosity_range: None,
};
const SPECTRAL_DEF_O: SpectralClassDefinition = SpectralClassDefinition {
    code: "O", visual_color: [150, 180, 255], is_scoopable: true,
    mass_range: Some((16.0, 60.0)), luminosity_range: Some((30_000.0, 1_200_000.0)),
};
const SPECTRAL_DEF_B: SpectralClassDefinition = SpectralClassDefinition {
    code: "B", visual_color: [175, 200, 255], is_scoopable: true,
    mass_range: Some((2.1, 16.0)), luminosity_range: Some((25.0, 30_000.0)),
};
const SPECTRAL_DEF_A: SpectralClassDefinition = SpectralClassDefinition {
    code: "A", visual_color: [220, 232, 255], is_scoopable: true,
    mass_range: Some((1.4, 2.1)), luminosity_range: Some((5.0, 25.0)),
};
const SPECTRAL_DEF_F: SpectralClassDefinition = SpectralClassDefinition {
    code: "F", visual_color: [246, 245, 255], is_scoopable: true,
    mass_range: Some((1.04, 1.4)), luminosity_range: Some((1.5, 5.0)),
};
const SPECTRAL_DEF_G: SpectralClassDefinition = SpectralClassDefinition {
    code: "G", visual_color: [255, 236, 170], is_scoopable: true,
    mass_range: Some((0.8, 1.04)), luminosity_range: Some((0.6, 1.5)),
};
const SPECTRAL_DEF_K: SpectralClassDefinition = SpectralClassDefinition {
    code: "K", visual_color: [255, 196, 122], is_scoopable: true,
    mass_range: Some((0.45, 0.8)), luminosity_range: Some((0.08, 0.6)),
};
const SPECTRAL_DEF_M: SpectralClassDefinition = SpectralClassDefinition {
    code: "M", visual_color: [255, 122, 92], is_scoopable: true,
    mass_range: Some((0.08, 0.45)), luminosity_range: Some((0.0005, 0.08)),
};
const SPECTRAL_DEF_W: SpectralClassDefinition = SpectralClassDefinition {
    code: "W", visual_color: [182, 214, 255], is_scoopable: false,
    mass_range: Some((12.0, 80.0)), luminosity_range: Some((120_000.0, 2_000_000.0)),
};
const SPECTRAL_DEF_WN: SpectralClassDefinition = SpectralClassDefinition {
    code: "WN", visual_color: [194, 222, 255], is_scoopable: false,
    mass_range: Some((20.0, 90.0)), luminosity_range: Some((180_000.0, 2_300_000.0)),
};
const SPECTRAL_DEF_WC: SpectralClassDefinition = SpectralClassDefinition {
    code: "WC", visual_color: [170, 206, 255], is_scoopable: false,
    mass_range: Some((15.0, 70.0)), luminosity_range: Some((120_000.0, 1_700_000.0)),
};
const SPECTRAL_DEF_WO: SpectralClassDefinition = SpectralClassDefinition {
    code: "WO", visual_color: [156, 195, 255], is_scoopable: false,
    mass_range: Some((18.0, 85.0)), luminosity_range: Some((200_000.0, 2_500_000.0)),
};
const SPECTRAL_DEF_L: SpectralClassDefinition = SpectralClassDefinition {
    code: "L", visual_color: [255, 142, 82], is_scoopable: false,
    mass_range: Some((0.013, 0.08)), luminosity_range: Some((0.00002, 0.0005)),
};
const SPECTRAL_DEF_T: SpectralClassDefinition = SpectralClassDefinition {
    code: "T", visual_color: [228, 112, 72], is_scoopable: false,
    mass_range: Some((0.010, 0.060)), luminosity_range: Some((0.000003, 0.00003)),
};
const SPECTRAL_DEF_Y: SpectralClassDefinition = SpectralClassDefinition {
    code: "Y", visual_color: [212, 172, 118], is_scoopable: false,
    mass_range: Some((0.008, 0.030)), luminosity_range: Some((0.0000003, 0.000003)),
};
const SPECTRAL_DEF_C: SpectralClassDefinition = SpectralClassDefinition {
    code: "C", visual_color: [255, 94, 70], is_scoopable: false,
    mass_range: Some((0.8, 4.0)), luminosity_range: Some((1.0, 10_000.0)),
};
const SPECTRAL_DEF_S: SpectralClassDefinition = SpectralClassDefinition {
    code: "S", visual_color: [255, 132, 90], is_scoopable: false,
    mass_range: Some((0.8, 3.0)), luminosity_range: Some((0.8, 5_000.0)),
};
const SPECTRAL_DEF_D: SpectralClassDefinition = SpectralClassDefinition {
    code: "D", visual_color: [236, 240, 255], is_scoopable: false,
    mass_range: Some((0.17, 1.33)), luminosity_range: Some((0.00001, 0.10)),
};
const SPECTRAL_DEF_DA: SpectralClassDefinition = SpectralClassDefinition {
    code: "DA", visual_color: [224, 236, 255], is_scoopable: false,
    mass_range: Some((0.20, 1.35)), luminosity_range: Some((0.00002, 0.12)),
};
const SPECTRAL_DEF_DB: SpectralClassDefinition = SpectralClassDefinition {
    code: "DB", visual_color: [236, 233, 255], is_scoopable: false,
    mass_range: Some((0.18, 1.25)), luminosity_range: Some((0.00001, 0.09)),
};
const SPECTRAL_DEF_DC: SpectralClassDefinition = SpectralClassDefinition {
    code: "DC", visual_color: [210, 218, 236], is_scoopable: false,
    mass_range: Some((0.16, 1.10)), luminosity_range: Some((0.000005, 0.05)),
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LuminosityClass {
    I,
    Ia,
    Iab,
    Ib,
    II,
    III,
    IV,
    V,
    Va,
    Vab,
    Vb,
    Vz,
    VI,
    VII,
}

impl LuminosityClass {
    pub fn definition(self) -> &'static LuminosityClassDefinition {
        match self {
            LuminosityClass::I => &LUM_DEF_I,
            LuminosityClass::Ia => &LUM_DEF_IA,
            LuminosityClass::Iab => &LUM_DEF_IAB,
            LuminosityClass::Ib => &LUM_DEF_IB,
            LuminosityClass::II => &LUM_DEF_II,
            LuminosityClass::III => &LUM_DEF_III,
            LuminosityClass::IV => &LUM_DEF_IV,
            LuminosityClass::V => &LUM_DEF_V,
            LuminosityClass::Va => &LUM_DEF_VA,
            LuminosityClass::Vab => &LUM_DEF_VAB,
            LuminosityClass::Vb => &LUM_DEF_VB,
            LuminosityClass::Vz => &LUM_DEF_VZ,
            LuminosityClass::VI => &LUM_DEF_VI,
            LuminosityClass::VII => &LUM_DEF_VII,
        }
    }

    pub fn code(self) -> &'static str {
        self.definition().code
    }

    pub fn visual_radius_multiplier(self) -> f32 {
        self.definition().visual_radius_multiplier
    }
}

pub struct LuminosityClassDefinition {
    pub code: &'static str,
    pub visual_radius_multiplier: f32,
    /// (mass_scale, luminosity_scale) used during star generation.
    pub generation_scales: (f32, f32),
}

const LUM_DEF_I: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "I", visual_radius_multiplier: 2.4, generation_scales: (2.6, 180.0),
};
const LUM_DEF_IA: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "Ia/0", visual_radius_multiplier: 2.6, generation_scales: (2.8, 220.0),
};
const LUM_DEF_IAB: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "Iab", visual_radius_multiplier: 2.2, generation_scales: (2.3, 120.0),
};
const LUM_DEF_IB: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "Ib", visual_radius_multiplier: 2.0, generation_scales: (2.0, 65.0),
};
const LUM_DEF_II: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "II", visual_radius_multiplier: 1.7, generation_scales: (1.6, 24.0),
};
const LUM_DEF_III: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "III", visual_radius_multiplier: 1.45, generation_scales: (1.35, 10.0),
};
const LUM_DEF_IV: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "IV", visual_radius_multiplier: 1.2, generation_scales: (1.15, 3.2),
};
const LUM_DEF_V: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "V", visual_radius_multiplier: 1.0, generation_scales: (1.0, 1.0),
};
const LUM_DEF_VA: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "Va", visual_radius_multiplier: 1.08, generation_scales: (1.08, 1.6),
};
const LUM_DEF_VAB: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "Vab", visual_radius_multiplier: 1.03, generation_scales: (1.03, 1.25),
};
const LUM_DEF_VB: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "Vb", visual_radius_multiplier: 1.0, generation_scales: (1.0, 1.0),
};
const LUM_DEF_VZ: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "Vz", visual_radius_multiplier: 0.92, generation_scales: (0.94, 0.78),
};
const LUM_DEF_VI: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "VI", visual_radius_multiplier: 0.82, generation_scales: (0.82, 0.46),
};
const LUM_DEF_VII: LuminosityClassDefinition = LuminosityClassDefinition {
    code: "VII", visual_radius_multiplier: 0.65, generation_scales: (0.64, 0.08),
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StellarClassification {
    pub spectral: SpectralClass,
    pub subclass: u8,
    pub luminosity: LuminosityClass,
}

impl StellarClassification {
    pub fn new(spectral: SpectralClass, subclass: u8, luminosity: LuminosityClass) -> Self {
        Self {
            spectral,
            subclass: subclass.min(9),
            luminosity,
        }
    }

    pub fn notation(self) -> String {
        if matches!(self.spectral, SpectralClass::BH | SpectralClass::NS) {
            return self.spectral.code().to_owned();
        }

        format!(
            "{}{} {}",
            self.spectral.code(),
            self.subclass,
            self.luminosity.code()
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemSummary {
    pub id: SystemId,
    pub pos: [f32; 3],
    pub represented_systems: u32,
    pub primary_star: StellarClassification,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StarBody {
    pub class: StellarClassification,
    pub mass_solar: f32,
    pub luminosity_solar: f32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PlanetKind {
    EarthLikeWorld,
    Rocky,
    RockyIceWorld,
    Icy,
    WaterWorld,
    AmmoniaWorld,
    MetalRich,
    Metal,
    GasGiantClassI,
    GasGiantClassII,
    GasGiantClassIII,
    GasGiantClassIV,
    GasGiantClassV,
    HeliumRichGasGiant,
    HeliumGasGiant,
    GasGiantAmmoniaLife,
    GasGiantWaterLife,
    WaterGiant,
    GasGiant,
}

/// Complete, self-contained definition of a planet kind.
///
/// To add a new planet type, create a new `const PlanetKindDefinition`,
/// add the enum variant to [`PlanetKind`], and register it in
/// [`PlanetKind::definition`].  Label, gas-giant flag, and visual base
/// radius are derived automatically.
#[derive(Clone, Copy, Debug)]
pub struct PlanetKindDefinition {
    pub label: &'static str,
    pub is_gas_giant: bool,
    /// Base visual radius in UI pixels (before size/temperature scaling).
    pub base_visual_radius_px: f32,
    /// Cold-temperature RGB for visual color lerp.
    pub visual_color_cold: [u8; 3],
    /// Hot-temperature RGB for visual color lerp.
    pub visual_color_hot: [u8; 3],
}

const PLANET_DEF_EARTH_LIKE_WORLD: PlanetKindDefinition = PlanetKindDefinition {
    label: "Earth-like world",
    is_gas_giant: false,
    base_visual_radius_px: 2.55,
    visual_color_cold: [84, 172, 210],
    visual_color_hot: [98, 214, 132],
};
const PLANET_DEF_ROCKY: PlanetKindDefinition = PlanetKindDefinition {
    label: "Rocky body",
    is_gas_giant: false,
    base_visual_radius_px: 1.9,
    visual_color_cold: [124, 132, 145],
    visual_color_hot: [184, 104, 76],
};
const PLANET_DEF_ROCKY_ICE_WORLD: PlanetKindDefinition = PlanetKindDefinition {
    label: "Rocky ice world",
    is_gas_giant: false,
    base_visual_radius_px: 2.2,
    visual_color_cold: [160, 176, 190],
    visual_color_hot: [204, 220, 238],
};
const PLANET_DEF_ICY: PlanetKindDefinition = PlanetKindDefinition {
    label: "Icy body",
    is_gas_giant: false,
    base_visual_radius_px: 2.1,
    visual_color_cold: [184, 214, 255],
    visual_color_hot: [154, 188, 225],
};
const PLANET_DEF_WATER_WORLD: PlanetKindDefinition = PlanetKindDefinition {
    label: "Water world",
    is_gas_giant: false,
    base_visual_radius_px: 2.45,
    visual_color_cold: [66, 132, 198],
    visual_color_hot: [110, 184, 210],
};
const PLANET_DEF_AMMONIA_WORLD: PlanetKindDefinition = PlanetKindDefinition {
    label: "Ammonia world",
    is_gas_giant: false,
    base_visual_radius_px: 2.3,
    visual_color_cold: [174, 186, 144],
    visual_color_hot: [196, 160, 122],
};
const PLANET_DEF_METAL_RICH: PlanetKindDefinition = PlanetKindDefinition {
    label: "Metal-rich world",
    is_gas_giant: false,
    base_visual_radius_px: 2.0,
    visual_color_cold: [160, 136, 118],
    visual_color_hot: [224, 156, 96],
};
const PLANET_DEF_METAL: PlanetKindDefinition = PlanetKindDefinition {
    label: "Metal world",
    is_gas_giant: false,
    base_visual_radius_px: 1.8,
    visual_color_cold: [128, 134, 142],
    visual_color_hot: [236, 224, 204],
};
const PLANET_DEF_GAS_GIANT_CLASS_I: PlanetKindDefinition = PlanetKindDefinition {
    label: "Class I gas giant",
    is_gas_giant: true,
    base_visual_radius_px: 4.35,
    visual_color_cold: [198, 212, 158],
    visual_color_hot: [242, 228, 172],
};
const PLANET_DEF_GAS_GIANT_CLASS_II: PlanetKindDefinition = PlanetKindDefinition {
    label: "Class II gas giant",
    is_gas_giant: true,
    base_visual_radius_px: 4.1,
    visual_color_cold: [134, 176, 228],
    visual_color_hot: [212, 230, 244],
};
const PLANET_DEF_GAS_GIANT_CLASS_III: PlanetKindDefinition = PlanetKindDefinition {
    label: "Class III gas giant",
    is_gas_giant: true,
    base_visual_radius_px: 3.95,
    visual_color_cold: [112, 148, 210],
    visual_color_hot: [212, 150, 102],
};
const PLANET_DEF_GAS_GIANT_CLASS_IV: PlanetKindDefinition = PlanetKindDefinition {
    label: "Class IV gas giant",
    is_gas_giant: true,
    base_visual_radius_px: 4.0,
    visual_color_cold: [170, 126, 178],
    visual_color_hot: [242, 154, 102],
};
const PLANET_DEF_GAS_GIANT_CLASS_V: PlanetKindDefinition = PlanetKindDefinition {
    label: "Class V gas giant",
    is_gas_giant: true,
    base_visual_radius_px: 4.2,
    visual_color_cold: [212, 148, 102],
    visual_color_hot: [244, 206, 132],
};
const PLANET_DEF_HELIUM_RICH_GAS_GIANT: PlanetKindDefinition = PlanetKindDefinition {
    label: "Helium-rich gas giant",
    is_gas_giant: true,
    base_visual_radius_px: 4.5,
    visual_color_cold: [178, 168, 214],
    visual_color_hot: [206, 188, 236],
};
const PLANET_DEF_HELIUM_GAS_GIANT: PlanetKindDefinition = PlanetKindDefinition {
    label: "Helium gas giant",
    is_gas_giant: true,
    base_visual_radius_px: 4.8,
    visual_color_cold: [206, 204, 236],
    visual_color_hot: [232, 226, 250],
};
const PLANET_DEF_GAS_GIANT_AMMONIA_LIFE: PlanetKindDefinition = PlanetKindDefinition {
    label: "Gas giant with ammonia-based life",
    is_gas_giant: true,
    base_visual_radius_px: 4.2,
    visual_color_cold: [132, 184, 112],
    visual_color_hot: [188, 218, 124],
};
const PLANET_DEF_GAS_GIANT_WATER_LIFE: PlanetKindDefinition = PlanetKindDefinition {
    label: "Gas giant with water-based life",
    is_gas_giant: true,
    base_visual_radius_px: 4.25,
    visual_color_cold: [84, 160, 184],
    visual_color_hot: [126, 212, 196],
};
const PLANET_DEF_WATER_GIANT: PlanetKindDefinition = PlanetKindDefinition {
    label: "Water giant",
    is_gas_giant: true,
    base_visual_radius_px: 3.55,
    visual_color_cold: [66, 144, 214],
    visual_color_hot: [136, 202, 236],
};
const PLANET_DEF_GAS_GIANT: PlanetKindDefinition = PlanetKindDefinition {
    label: "Gas giant",
    is_gas_giant: true,
    base_visual_radius_px: 3.8,
    visual_color_cold: [130, 156, 222],
    visual_color_hot: [252, 168, 96],
};

impl PlanetKind {
    /// Returns the full static definition for this planet kind.
    pub fn definition(self) -> &'static PlanetKindDefinition {
        match self {
            Self::EarthLikeWorld => &PLANET_DEF_EARTH_LIKE_WORLD,
            Self::Rocky => &PLANET_DEF_ROCKY,
            Self::RockyIceWorld => &PLANET_DEF_ROCKY_ICE_WORLD,
            Self::Icy => &PLANET_DEF_ICY,
            Self::WaterWorld => &PLANET_DEF_WATER_WORLD,
            Self::AmmoniaWorld => &PLANET_DEF_AMMONIA_WORLD,
            Self::MetalRich => &PLANET_DEF_METAL_RICH,
            Self::Metal => &PLANET_DEF_METAL,
            Self::GasGiantClassI => &PLANET_DEF_GAS_GIANT_CLASS_I,
            Self::GasGiantClassII => &PLANET_DEF_GAS_GIANT_CLASS_II,
            Self::GasGiantClassIII => &PLANET_DEF_GAS_GIANT_CLASS_III,
            Self::GasGiantClassIV => &PLANET_DEF_GAS_GIANT_CLASS_IV,
            Self::GasGiantClassV => &PLANET_DEF_GAS_GIANT_CLASS_V,
            Self::HeliumRichGasGiant => &PLANET_DEF_HELIUM_RICH_GAS_GIANT,
            Self::HeliumGasGiant => &PLANET_DEF_HELIUM_GAS_GIANT,
            Self::GasGiantAmmoniaLife => &PLANET_DEF_GAS_GIANT_AMMONIA_LIFE,
            Self::GasGiantWaterLife => &PLANET_DEF_GAS_GIANT_WATER_LIFE,
            Self::WaterGiant => &PLANET_DEF_WATER_GIANT,
            Self::GasGiant => &PLANET_DEF_GAS_GIANT,
        }
    }

    pub fn label(self) -> &'static str { self.definition().label }

    pub fn is_gas_giant(self) -> bool { self.definition().is_gas_giant }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ElementResourceInfo {
    pub atomic_number: u8,
    pub symbol: &'static str,
    pub name: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AtmosphereResourceInfo {
    pub formula: &'static str,
    pub name: &'static str,
}

pub fn composition_element_resource_catalog() -> &'static [ElementResourceInfo] {
    const CATALOG: [ElementResourceInfo; 32] = [
        ElementResourceInfo {
            atomic_number: 1,
            symbol: "H",
            name: "Hydrogen",
        },
        ElementResourceInfo {
            atomic_number: 2,
            symbol: "He",
            name: "Helium",
        },
        ElementResourceInfo {
            atomic_number: 3,
            symbol: "Li",
            name: "Lithium",
        },
        ElementResourceInfo {
            atomic_number: 5,
            symbol: "B",
            name: "Boron",
        },
        ElementResourceInfo {
            atomic_number: 6,
            symbol: "C",
            name: "Carbon",
        },
        ElementResourceInfo {
            atomic_number: 7,
            symbol: "N",
            name: "Nitrogen",
        },
        ElementResourceInfo {
            atomic_number: 8,
            symbol: "O",
            name: "Oxygen",
        },
        ElementResourceInfo {
            atomic_number: 9,
            symbol: "F",
            name: "Fluorine",
        },
        ElementResourceInfo {
            atomic_number: 10,
            symbol: "Ne",
            name: "Neon",
        },
        ElementResourceInfo {
            atomic_number: 11,
            symbol: "Na",
            name: "Sodium",
        },
        ElementResourceInfo {
            atomic_number: 12,
            symbol: "Mg",
            name: "Magnesium",
        },
        ElementResourceInfo {
            atomic_number: 13,
            symbol: "Al",
            name: "Aluminum",
        },
        ElementResourceInfo {
            atomic_number: 14,
            symbol: "Si",
            name: "Silicon",
        },
        ElementResourceInfo {
            atomic_number: 15,
            symbol: "P",
            name: "Phosphorus",
        },
        ElementResourceInfo {
            atomic_number: 16,
            symbol: "S",
            name: "Sulfur",
        },
        ElementResourceInfo {
            atomic_number: 17,
            symbol: "Cl",
            name: "Chlorine",
        },
        ElementResourceInfo {
            atomic_number: 18,
            symbol: "Ar",
            name: "Argon",
        },
        ElementResourceInfo {
            atomic_number: 19,
            symbol: "K",
            name: "Potassium",
        },
        ElementResourceInfo {
            atomic_number: 20,
            symbol: "Ca",
            name: "Calcium",
        },
        ElementResourceInfo {
            atomic_number: 22,
            symbol: "Ti",
            name: "Titanium",
        },
        ElementResourceInfo {
            atomic_number: 23,
            symbol: "V",
            name: "Vanadium",
        },
        ElementResourceInfo {
            atomic_number: 24,
            symbol: "Cr",
            name: "Chromium",
        },
        ElementResourceInfo {
            atomic_number: 25,
            symbol: "Mn",
            name: "Manganese",
        },
        ElementResourceInfo {
            atomic_number: 26,
            symbol: "Fe",
            name: "Iron",
        },
        ElementResourceInfo {
            atomic_number: 27,
            symbol: "Co",
            name: "Cobalt",
        },
        ElementResourceInfo {
            atomic_number: 28,
            symbol: "Ni",
            name: "Nickel",
        },
        ElementResourceInfo {
            atomic_number: 29,
            symbol: "Cu",
            name: "Copper",
        },
        ElementResourceInfo {
            atomic_number: 30,
            symbol: "Zn",
            name: "Zinc",
        },
        ElementResourceInfo {
            atomic_number: 42,
            symbol: "Mo",
            name: "Molybdenum",
        },
        ElementResourceInfo {
            atomic_number: 74,
            symbol: "W",
            name: "Tungsten",
        },
        ElementResourceInfo {
            atomic_number: 77,
            symbol: "Ir",
            name: "Iridium",
        },
        ElementResourceInfo {
            atomic_number: 78,
            symbol: "Pt",
            name: "Platinum",
        },
    ];

    &CATALOG
}

pub fn atmosphere_resource_catalog() -> &'static [AtmosphereResourceInfo] {
    const CATALOG: [AtmosphereResourceInfo; 15] = [
        AtmosphereResourceInfo {
            formula: "H2",
            name: "Hydrogen",
        },
        AtmosphereResourceInfo {
            formula: "He",
            name: "Helium",
        },
        AtmosphereResourceInfo {
            formula: "N2",
            name: "Nitrogen",
        },
        AtmosphereResourceInfo {
            formula: "O2",
            name: "Oxygen",
        },
        AtmosphereResourceInfo {
            formula: "CO2",
            name: "Carbon dioxide",
        },
        AtmosphereResourceInfo {
            formula: "CO",
            name: "Carbon monoxide",
        },
        AtmosphereResourceInfo {
            formula: "CH4",
            name: "Methane",
        },
        AtmosphereResourceInfo {
            formula: "NH3",
            name: "Ammonia",
        },
        AtmosphereResourceInfo {
            formula: "H2O",
            name: "Water vapor",
        },
        AtmosphereResourceInfo {
            formula: "SO2",
            name: "Sulfur dioxide",
        },
        AtmosphereResourceInfo {
            formula: "H2S",
            name: "Hydrogen sulfide",
        },
        AtmosphereResourceInfo {
            formula: "Ne",
            name: "Neon",
        },
        AtmosphereResourceInfo {
            formula: "Ar",
            name: "Argon",
        },
        AtmosphereResourceInfo {
            formula: "Kr",
            name: "Krypton",
        },
        AtmosphereResourceInfo {
            formula: "Xe",
            name: "Xenon",
        },
    ];

    &CATALOG
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanetElementComponent {
    pub atomic_number: u8,
    pub symbol: String,
    pub name: String,
    pub percent: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanetAtmosphereComponent {
    pub formula: String,
    pub name: String,
    pub percent: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanetBody {
    pub kind: PlanetKind,
    // Orbit around host star in AU (for moons this is their host planet's stellar orbit).
    pub orbit_au: f32,
    // If present, this body is a moon orbiting another planet in `planets`.
    pub host_planet_index: Option<u8>,
    // Orbit around the host planet in AU (only set for moons).
    pub moon_orbit_au: Option<f32>,
    // Approximate physical size and mass for rendering/physics heuristics.
    pub radius_earth: f32,
    pub mass_earth: f32,
    pub temperature_k: f32,
    pub habitable: bool,
    #[serde(default)]
    pub composition: Vec<PlanetElementComponent>,
    #[serde(default)]
    pub atmosphere: Vec<PlanetAtmosphereComponent>,
    #[serde(default)]
    pub atmosphere_pressure_atm: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemDetail {
    pub id: SystemId,
    pub canonical_name: String,
    pub display_name: String,
    pub pos: [f32; 3],
    pub represented_systems: u32,
    pub stars: Vec<StarBody>,
    pub planets: Vec<PlanetBody>,
    pub explored: bool,
    pub favorite: bool,
    pub note: Option<String>,
}

impl SystemDetail {
    pub fn apply_delta(&mut self, delta: &SystemDelta) {
        if let Some(rename_to) = &delta.rename_to {
            let trimmed = rename_to.trim();
            if !trimmed.is_empty() {
                self.display_name = trimmed.to_owned();
            }
        }
        if let Some(explored) = delta.explored {
            self.explored = explored;
        }
        if let Some(favorite) = delta.favorite {
            self.favorite = favorite;
        }
        if let Some(note) = &delta.note {
            let trimmed = note.trim();
            self.note = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            };
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeneratorConfig {
    pub galaxy_seed: u64,
    pub target_system_count: u64,
    pub center: [f32; 3],
    pub playfield_radius: f32,
    pub sector_size: f32,
    pub z_min: f32,
    pub z_max: f32,
    pub arm_count: usize,
    pub arm_pitch_per_world_unit: f32,
    pub arm_width_radians: f32,
    pub arm_contrast: f32,
    pub bulge_radius: f32,
    pub radial_falloff_exp: f32,
    pub base_sector_density: f32,
    pub min_materialized_per_sector: usize,
    pub max_materialized_per_sector: usize,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            galaxy_seed: 0xED11_E5DA_7A5E_ED01,
            target_system_count: 400_000_000_000,
            center: [50_000.0, 50_000.0, 30_000.0],
            playfield_radius: 50_000.0,
            sector_size: 2_000.0,
            z_min: 20_000.0,
            z_max: 40_000.0,
            arm_count: 4,
            arm_pitch_per_world_unit: 0.00022,
            arm_width_radians: 0.28,
            arm_contrast: 0.78,
            bulge_radius: 9_500.0,
            radial_falloff_exp: 0.62,
            base_sector_density: 140.0,
            min_materialized_per_sector: 4_000,
            max_materialized_per_sector: 120_000,
        }
    }
}

pub struct GalaxyGenerator {
    cfg: GeneratorConfig,
    density_scale: f32,
    arm_phases: Vec<f32>,
    inv_arm_width_sq: f32,
}

impl GalaxyGenerator {
    pub fn new(mut cfg: GeneratorConfig) -> Self {
        cfg.arm_count = cfg.arm_count.max(1);
        cfg.playfield_radius = cfg.playfield_radius.max(1.0);
        cfg.sector_size = cfg.sector_size.max(1.0);
        cfg.arm_width_radians = cfg.arm_width_radians.clamp(0.05, PI);
        cfg.arm_contrast = cfg.arm_contrast.clamp(0.0, 1.0);
        cfg.radial_falloff_exp = cfg.radial_falloff_exp.clamp(0.05, 4.0);
        cfg.bulge_radius = cfg.bulge_radius.max(1.0);
        cfg.max_materialized_per_sector = cfg
            .max_materialized_per_sector
            .max(cfg.min_materialized_per_sector)
            .max(1);

        let baseline = Self::estimate_total_systems_for_scale(&cfg, 1.0).max(1);
        let density_scale = cfg.target_system_count.max(1) as f32 / baseline as f32;

        let arm_phases: Vec<f32> = (0..cfg.arm_count)
            .map(|arm| 2.0 * PI * arm as f32 / cfg.arm_count as f32)
            .collect();
        let width = cfg.arm_width_radians.max(0.01);
        let inv_arm_width_sq = 1.0 / (width * width);

        Self { cfg, density_scale, arm_phases, inv_arm_width_sq }
    }

    pub fn config(&self) -> &GeneratorConfig {
        &self.cfg
    }

    pub fn density_scale(&self) -> f32 {
        self.density_scale
    }

    pub fn estimate_total_systems(&self) -> u64 {
        Self::estimate_total_systems_for_scale(&self.cfg, self.density_scale)
    }

    pub fn sector_seed(&self, coord: SectorCoord) -> u64 {
        mix64(
            self.cfg.galaxy_seed
                ^ GENERATION_VERSION
                ^ SECTOR_DOMAIN_TAG
                ^ (coord.x as i64 as u64).wrapping_mul(SECTOR_HASH_X)
                ^ (coord.y as i64 as u64).wrapping_mul(SECTOR_HASH_Y),
        )
    }

    pub fn system_seed(&self, id: SystemId) -> u64 {
        let sector_seed = self.sector_seed(id.sector);
        mix64(
            sector_seed
                ^ GENERATION_VERSION
                ^ SYSTEM_DOMAIN_TAG
                ^ (id.local_index as u64).wrapping_mul(SYSTEM_HASH_I),
        )
    }

    fn sector_bounds(&self, coord: SectorCoord) -> (f32, f32, f32, f32) {
        let x0 = self.cfg.center[0] + coord.x as f32 * self.cfg.sector_size;
        let y0 = self.cfg.center[1] + coord.y as f32 * self.cfg.sector_size;
        let x1 = x0 + self.cfg.sector_size;
        let y1 = y0 + self.cfg.sector_size;
        (x0, x1, y0, y1)
    }

    fn spawn_cell_seed(&self, coord: SpawnCellCoord) -> u64 {
        mix64(
            self.cfg.galaxy_seed
                ^ GENERATION_VERSION
                ^ SPAWN_CELL_DOMAIN_TAG
                ^ (coord.x as i64 as u64).wrapping_mul(SECTOR_HASH_X)
                ^ (coord.y as i64 as u64).wrapping_mul(SECTOR_HASH_Y),
        )
    }

    fn spawn_cell_bounds(&self, coord: SpawnCellCoord) -> (f32, f32, f32, f32) {
        let x0 = coord.x as f32 * SPAWN_CELL_SIZE;
        let y0 = coord.y as f32 * SPAWN_CELL_SIZE;
        let x1 = x0 + SPAWN_CELL_SIZE;
        let y1 = y0 + SPAWN_CELL_SIZE;
        (x0, x1, y0, y1)
    }

    fn radial_from_center(&self, x: f32, y: f32) -> f32 {
        let dx = x - self.cfg.center[0];
        let dy = y - self.cfg.center[1];
        (dx * dx + dy * dy).sqrt()
    }

    fn arm_signal(&self, theta: f32, radial: f32) -> f32 {
        let mut strongest: f32 = 0.0;
        for &arm_phase in &self.arm_phases {
            let expected_theta = arm_phase + radial * self.cfg.arm_pitch_per_world_unit;
            let delta = wrap_angle_radians(theta - expected_theta);
            let gaussian = (-0.5 * delta * delta * self.inv_arm_width_sq).exp();
            strongest = strongest.max(gaussian);
        }
        strongest.clamp(0.0, 1.0)
    }

    fn position_density_weight(&self, x: f32, y: f32) -> f32 {
        let dx = x - self.cfg.center[0];
        let dy = y - self.cfg.center[1];
        let radial = self.radial_from_center(x, y);
        if radial > self.cfg.playfield_radius {
            return 0.0;
        }

        let radial01 = (radial / self.cfg.playfield_radius).clamp(0.0, 1.0);
        let disk_term = (1.0 - radial01).powf(self.cfg.radial_falloff_exp);
        let theta = dy.atan2(dx);
        let arm = self.arm_signal(theta, radial).powf(ARM_DENSITY_SHARPNESS);
        let arm_term = (1.0 - self.cfg.arm_contrast) + self.cfg.arm_contrast * arm;

        let bulge_sigma = (self.cfg.bulge_radius * 0.55).max(1.0);
        let bulge = (-0.5 * (radial / bulge_sigma).powi(2)).exp();

        (disk_term * arm_term + 0.28 * bulge).clamp(0.0, 1.0)
    }

    fn sector_density_factor(&self, x: f32, y: f32) -> f32 {
        let dx = x - self.cfg.center[0];
        let dy = y - self.cfg.center[1];
        let radial = self.radial_from_center(x, y);
        if radial > self.cfg.playfield_radius {
            return 0.0;
        }

        let radial01 = (radial / self.cfg.playfield_radius).clamp(0.0, 1.0);
        let disk_term = (1.0 - radial01).powf(self.cfg.radial_falloff_exp);
        let theta = dy.atan2(dx);
        let arm_term = self.arm_signal(theta, radial).powf(ARM_DENSITY_SHARPNESS);
        let bulge_term = (-0.5 * (radial / self.cfg.bulge_radius.max(1.0)).powi(2)).exp();

        (disk_term * ((1.0 - self.cfg.arm_contrast) + self.cfg.arm_contrast * arm_term)
            + 0.24 * bulge_term)
            .max(0.0)
    }

    fn best_sector_position_sample(
        &self,
        x0: f32,
        x1: f32,
        y0: f32,
        y1: f32,
    ) -> (f32, f32, f32) {
        let samples_per_axis = POSITION_FALLBACK_SAMPLES_PER_AXIS as f32;
        let step_x = (x1 - x0) / samples_per_axis;
        let step_y = (y1 - y0) / samples_per_axis;

        let mut best_x = x0 + step_x * 0.5;
        let mut best_y = y0 + step_y * 0.5;
        let mut best_keep_probability = -1.0f32;

        for sample_x in 0..POSITION_FALLBACK_SAMPLES_PER_AXIS {
            let x = x0 + (sample_x as f32 + 0.5) * step_x;
            for sample_y in 0..POSITION_FALLBACK_SAMPLES_PER_AXIS {
                let y = y0 + (sample_y as f32 + 0.5) * step_y;
                let keep_probability = self.position_density_weight(x, y);
                if keep_probability > best_keep_probability {
                    best_keep_probability = keep_probability;
                    best_x = x;
                    best_y = y;
                }
            }
        }

        (best_x, best_y, best_keep_probability)
    }

    fn weighted_subcell_bounds(
        &self,
        x0: f32,
        x1: f32,
        y0: f32,
        y1: f32,
    ) -> Vec<(f32, f32, f32, f32, f32)> {
        let subcells_per_axis = POSITION_SUBCELLS_PER_AXIS as f32;
        let step_x = (x1 - x0) / subcells_per_axis;
        let step_y = (y1 - y0) / subcells_per_axis;
        let mut subcells = Vec::with_capacity(POSITION_SUBCELLS_PER_AXIS * POSITION_SUBCELLS_PER_AXIS);

        for cell_x in 0..POSITION_SUBCELLS_PER_AXIS {
            let cell_min_x = x0 + cell_x as f32 * step_x;
            let cell_max_x = cell_min_x + step_x;

            for cell_y in 0..POSITION_SUBCELLS_PER_AXIS {
                let cell_min_y = y0 + cell_y as f32 * step_y;
                let cell_max_y = cell_min_y + step_y;
                let sample_x = (cell_min_x + cell_max_x) * 0.5;
                let sample_y = (cell_min_y + cell_max_y) * 0.5;
                let weight = self.position_density_weight(sample_x, sample_y).max(0.000_1);
                subcells.push((cell_min_x, cell_max_x, cell_min_y, cell_max_y, weight));
            }
        }

        subcells
    }

    fn sector_target_total_systems_with_scale(&self, coord: SectorCoord, scale: f32) -> u64 {
        let (x0, x1, y0, y1) = self.sector_bounds(coord);
        let sx = (x0 + x1) * 0.5;
        let sy = (y0 + y1) * 0.5;
        let radial = self.radial_from_center(sx, sy);
        let half_diag = (2.0_f32).sqrt() * self.cfg.sector_size * 0.5;
        if radial - half_diag > self.cfg.playfield_radius {
            return 0;
        }

        let samples_per_axis = SECTOR_DENSITY_SAMPLES_PER_AXIS as f32;
        let step_x = (x1 - x0) / samples_per_axis;
        let step_y = (y1 - y0) / samples_per_axis;
        let mut density_sum = 0.0f32;
        let mut rng = StdRng::seed_from_u64(self.sector_seed(coord) ^ 0x7B3D_54A1_C9E2_104F);

        for sample_x in 0..SECTOR_DENSITY_SAMPLES_PER_AXIS {
            for sample_y in 0..SECTOR_DENSITY_SAMPLES_PER_AXIS {
                let x = x0 + (sample_x as f32 + rng.r#gen::<f32>()) * step_x;
                let y = y0 + (sample_y as f32 + rng.r#gen::<f32>()) * step_y;
                density_sum += self.sector_density_factor(x, y);
            }
        }

        let density = self.cfg.base_sector_density
            * density_sum
            / (SECTOR_DENSITY_SAMPLES_PER_AXIS * SECTOR_DENSITY_SAMPLES_PER_AXIS) as f32;

        (density * scale).max(0.0).round() as u64
    }

    pub fn sector_target_total_systems(&self, coord: SectorCoord) -> u64 {
        self.sector_target_total_systems_with_scale(coord, self.density_scale)
    }

    fn estimate_total_systems_for_scale(cfg: &GeneratorConfig, scale: f32) -> u64 {
        let generator = Self {
            cfg: cfg.clone(),
            density_scale: 1.0,
            arm_phases: (0..cfg.arm_count)
                .map(|arm| 2.0 * PI * arm as f32 / cfg.arm_count as f32)
                .collect(),
            inv_arm_width_sq: {
                let w = cfg.arm_width_radians.max(0.01);
                1.0 / (w * w)
            },
        };
        let sector_radius = (cfg.playfield_radius / cfg.sector_size).ceil() as i32 + 1;
        let mut total = 0u64;

        for x in -sector_radius..=sector_radius {
            for y in -sector_radius..=sector_radius {
                total = total.saturating_add(
                    generator.sector_target_total_systems_with_scale(SectorCoord { x, y }, scale),
                );
            }
        }

        total
    }

    fn sample_spectral_class_from_roll(roll: f32) -> SpectralClass {
        // Weighted to keep broad plausibility while including rare classes.
        if roll < 0.0010 {
            SpectralClass::BH
        } else if roll < 0.0018 {
            SpectralClass::NS
        } else if roll < 0.0025 {
            SpectralClass::O
        } else if roll < 0.0115 {
            SpectralClass::B
        } else if roll < 0.0515 {
            SpectralClass::A
        } else if roll < 0.1315 {
            SpectralClass::F
        } else if roll < 0.2715 {
            SpectralClass::G
        } else if roll < 0.4715 {
            SpectralClass::K
        } else if roll < 0.8365 {
            SpectralClass::M
        } else if roll < 0.8915 {
            SpectralClass::L
        } else if roll < 0.9315 {
            SpectralClass::T
        } else if roll < 0.9515 {
            SpectralClass::Y
        } else if roll < 0.9705 {
            SpectralClass::C
        } else if roll < 0.9805 {
            SpectralClass::S
        } else if roll < 0.9825 {
            SpectralClass::W
        } else if roll < 0.9855 {
            SpectralClass::WN
        } else if roll < 0.9885 {
            SpectralClass::WC
        } else if roll < 0.9905 {
            SpectralClass::WO
        } else if roll < 0.9935 {
            SpectralClass::D
        } else if roll < 0.9975 {
            SpectralClass::DA
        } else if roll < 0.9995 {
            SpectralClass::DB
        } else {
            SpectralClass::DC
        }
    }

    fn sample_spectral_subclass_from_roll(roll: f32) -> u8 {
        ((roll * 10.0).floor() as u8).min(9)
    }

    fn sample_luminosity_class_for_spectral(
        spectral: SpectralClass,
        roll: f32,
    ) -> LuminosityClass {
        match spectral {
            SpectralClass::BH => {
                if roll < 0.95 {
                    LuminosityClass::VII
                } else {
                    LuminosityClass::VI
                }
            }
            SpectralClass::NS => {
                if roll < 0.94 {
                    LuminosityClass::VII
                } else if roll < 0.995 {
                    LuminosityClass::VI
                } else {
                    LuminosityClass::Vz
                }
            }
            SpectralClass::D | SpectralClass::DA | SpectralClass::DB | SpectralClass::DC => {
                if roll < 0.92 {
                    LuminosityClass::VII
                } else if roll < 0.97 {
                    LuminosityClass::VI
                } else {
                    LuminosityClass::Vz
                }
            }
            SpectralClass::W | SpectralClass::WN | SpectralClass::WC | SpectralClass::WO => {
                if roll < 0.18 {
                    LuminosityClass::Ia
                } else if roll < 0.30 {
                    LuminosityClass::I
                } else if roll < 0.46 {
                    LuminosityClass::Iab
                } else if roll < 0.63 {
                    LuminosityClass::Ib
                } else if roll < 0.78 {
                    LuminosityClass::II
                } else {
                    LuminosityClass::III
                }
            }
            SpectralClass::C | SpectralClass::S => {
                if roll < 0.08 {
                    LuminosityClass::II
                } else if roll < 0.70 {
                    LuminosityClass::III
                } else if roll < 0.90 {
                    LuminosityClass::IV
                } else {
                    LuminosityClass::V
                }
            }
            SpectralClass::L | SpectralClass::T | SpectralClass::Y => {
                if roll < 0.60 {
                    LuminosityClass::VI
                } else if roll < 0.85 {
                    LuminosityClass::Vz
                } else if roll < 0.95 {
                    LuminosityClass::Vb
                } else {
                    LuminosityClass::V
                }
            }
            SpectralClass::O
            | SpectralClass::B
            | SpectralClass::A
            | SpectralClass::F
            | SpectralClass::G
            | SpectralClass::K
            | SpectralClass::M => {
                if roll < 0.01 {
                    LuminosityClass::Ia
                } else if roll < 0.02 {
                    LuminosityClass::I
                } else if roll < 0.05 {
                    LuminosityClass::Iab
                } else if roll < 0.09 {
                    LuminosityClass::Ib
                } else if roll < 0.16 {
                    LuminosityClass::II
                } else if roll < 0.28 {
                    LuminosityClass::III
                } else if roll < 0.42 {
                    LuminosityClass::IV
                } else if roll < 0.58 {
                    LuminosityClass::Va
                } else if roll < 0.72 {
                    LuminosityClass::Vab
                } else if roll < 0.90 {
                    LuminosityClass::Vb
                } else if roll < 0.97 {
                    LuminosityClass::V
                } else if roll < 0.995 {
                    LuminosityClass::Vz
                } else {
                    LuminosityClass::VI
                }
            }
        }
    }

    fn luminosity_class_scales(class: LuminosityClass) -> (f32, f32) {
        class.definition().generation_scales
    }

    fn sample_stellar_class(rng: &mut StdRng) -> StellarClassification {
        let spectral = Self::sample_spectral_class_from_roll(rng.r#gen::<f32>());
        if spectral == SpectralClass::BH {
            return StellarClassification::new(SpectralClass::BH, 0, LuminosityClass::VII);
        }
        if spectral == SpectralClass::NS {
            return StellarClassification::new(
                SpectralClass::NS,
                0,
                Self::sample_luminosity_class_for_spectral(spectral, rng.r#gen::<f32>()),
            );
        }

        let subclass = Self::sample_spectral_subclass_from_roll(rng.r#gen::<f32>());
        let luminosity =
            Self::sample_luminosity_class_for_spectral(spectral, rng.r#gen::<f32>());
        StellarClassification::new(spectral, subclass, luminosity)
    }

    fn sample_stellar_class_from_seed(seed: u64) -> StellarClassification {
        let spectral_roll = unit_f32_from_u64(mix64(seed ^ 0xA6B0_14D7_2FD1_8A43));
        let subclass_roll = unit_f32_from_u64(mix64(seed ^ 0x53B2_04E7_7C39_4F9A));
        let luminosity_roll = unit_f32_from_u64(mix64(seed ^ 0x9FA7_1CE5_DA03_B56C));

        let spectral = Self::sample_spectral_class_from_roll(spectral_roll);
        if spectral == SpectralClass::BH {
            return StellarClassification::new(SpectralClass::BH, 0, LuminosityClass::VII);
        }
        if spectral == SpectralClass::NS {
            return StellarClassification::new(
                SpectralClass::NS,
                0,
                Self::sample_luminosity_class_for_spectral(spectral, luminosity_roll),
            );
        }

        let subclass = Self::sample_spectral_subclass_from_roll(subclass_roll);
        let luminosity =
            Self::sample_luminosity_class_for_spectral(spectral, luminosity_roll);

        StellarClassification::new(spectral, subclass, luminosity)
    }

    fn sample_star_body(rng: &mut StdRng, class: StellarClassification) -> StarBody {
        if class.spectral == SpectralClass::BH {
            return StarBody {
                class,
                mass_solar: rng.gen_range(3.0..42.0),
                luminosity_solar: rng.gen_range(0.000_000_001..0.000_3),
            };
        }
        if class.spectral == SpectralClass::NS {
            return StarBody {
                class,
                mass_solar: rng.gen_range(1.1..2.3),
                luminosity_solar: rng.gen_range(0.000_000_1..0.05),
            };
        }

        let def = class.spectral.definition();
        let (mass_r, lum_r) = match (def.mass_range, def.luminosity_range) {
            (Some(m), Some(l)) => (m, l),
            _ => unreachable!("BH/NS are handled above"),
        };
        let (mut mass_min, mut mass_max) = mass_r;
        let (mut lum_min, mut lum_max) = lum_r;

        // Spectral subclass 0-9: 0 hottest, 9 coolest.
        let temp_bias = 1.0 - (class.subclass as f32 / 9.0);
        let mass_subclass_scale = 0.58 + 0.42 * temp_bias;
        let lum_subclass_scale = 0.38 + 0.62 * temp_bias;
        mass_min *= mass_subclass_scale;
        mass_max *= mass_subclass_scale;
        lum_min *= lum_subclass_scale;
        lum_max *= lum_subclass_scale;

        let (mass_scale, lum_scale) = Self::luminosity_class_scales(class.luminosity);
        mass_min *= mass_scale;
        mass_max *= mass_scale;
        lum_min *= lum_scale;
        lum_max *= lum_scale;

        if mass_max <= mass_min {
            mass_max = mass_min + (mass_min.abs() * 0.05).max(1e-6);
        }
        if lum_max <= lum_min {
            lum_max = lum_min + (lum_min.abs() * 0.05).max(1e-8);
        }

        StarBody {
            class,
            mass_solar: rng.gen_range(mass_min..mass_max),
            luminosity_solar: rng.gen_range(lum_min..lum_max),
        }
    }

    fn equilibrium_temperature_k(avg_luminosity: f32, orbit_au: f32) -> f32 {
        278.0 * avg_luminosity.powf(0.25) / orbit_au.max(0.03).sqrt()
    }

    fn element_metadata(symbol: &str) -> (u8, &'static str) {
        match symbol {
            "H" => (1, "Hydrogen"),
            "He" => (2, "Helium"),
            "Li" => (3, "Lithium"),
            "B" => (5, "Boron"),
            "C" => (6, "Carbon"),
            "N" => (7, "Nitrogen"),
            "O" => (8, "Oxygen"),
            "F" => (9, "Fluorine"),
            "Ne" => (10, "Neon"),
            "Na" => (11, "Sodium"),
            "Mg" => (12, "Magnesium"),
            "Al" => (13, "Aluminum"),
            "Si" => (14, "Silicon"),
            "P" => (15, "Phosphorus"),
            "S" => (16, "Sulfur"),
            "Cl" => (17, "Chlorine"),
            "Ar" => (18, "Argon"),
            "K" => (19, "Potassium"),
            "Ca" => (20, "Calcium"),
            "Ti" => (22, "Titanium"),
            "V" => (23, "Vanadium"),
            "Cr" => (24, "Chromium"),
            "Mn" => (25, "Manganese"),
            "Fe" => (26, "Iron"),
            "Co" => (27, "Cobalt"),
            "Ni" => (28, "Nickel"),
            "Cu" => (29, "Copper"),
            "Zn" => (30, "Zinc"),
            "Mo" => (42, "Molybdenum"),
            "W" => (74, "Tungsten"),
            "Ir" => (77, "Iridium"),
            "Pt" => (78, "Platinum"),
            _ => (0, "Unknown"),
        }
    }

    fn composition_profile(kind: PlanetKind) -> (&'static [(&'static str, f32)], &'static [&'static str]) {
        const EARTH_LIKE_MAJOR: [(&str, f32); 10] = [
            ("O", 31.0),
            ("Si", 25.0),
            ("Fe", 17.0),
            ("Mg", 15.0),
            ("Al", 4.0),
            ("Ca", 2.2),
            ("Na", 1.5),
            ("K", 1.0),
            ("S", 1.6),
            ("C", 1.7),
        ];
        const ROCKY_MAJOR: [(&str, f32); 10] = [
            ("O", 29.0),
            ("Si", 24.0),
            ("Mg", 17.0),
            ("Fe", 15.0),
            ("Al", 4.4),
            ("Ca", 2.8),
            ("Na", 1.7),
            ("K", 1.0),
            ("Ti", 1.4),
            ("Ni", 2.7),
        ];
        const ROCKY_ICE_MAJOR: [(&str, f32); 8] = [
            ("O", 34.0),
            ("Si", 15.0),
            ("Mg", 11.0),
            ("Fe", 9.0),
            ("H", 18.0),
            ("C", 6.0),
            ("N", 4.0),
            ("S", 3.0),
        ];
        const ICY_MAJOR: [(&str, f32); 7] = [
            ("H", 34.0),
            ("O", 31.0),
            ("C", 13.0),
            ("N", 12.0),
            ("Si", 4.0),
            ("Mg", 3.0),
            ("Fe", 3.0),
        ];
        const WATER_MAJOR: [(&str, f32); 8] = [
            ("O", 49.0),
            ("H", 23.0),
            ("Mg", 6.0),
            ("Si", 6.0),
            ("Na", 4.0),
            ("Cl", 3.0),
            ("C", 5.0),
            ("N", 4.0),
        ];
        const AMMONIA_MAJOR: [(&str, f32); 8] = [
            ("H", 31.0),
            ("N", 29.0),
            ("O", 13.0),
            ("C", 11.0),
            ("S", 6.0),
            ("Si", 4.0),
            ("Mg", 3.0),
            ("Fe", 3.0),
        ];
        const METAL_RICH_MAJOR: [(&str, f32); 9] = [
            ("Fe", 31.0),
            ("Ni", 15.0),
            ("Mg", 10.0),
            ("Si", 13.0),
            ("O", 11.0),
            ("Co", 5.0),
            ("Cr", 4.0),
            ("S", 6.0),
            ("Al", 5.0),
        ];
        const METAL_MAJOR: [(&str, f32); 8] = [
            ("Fe", 42.0),
            ("Ni", 22.0),
            ("Co", 9.0),
            ("Cr", 7.0),
            ("W", 5.0),
            ("Mo", 5.0),
            ("Ir", 5.0),
            ("Pt", 5.0),
        ];
        const CLASS_I_MAJOR: [(&str, f32); 5] =
            [("H", 70.0), ("He", 22.0), ("O", 3.0), ("C", 3.0), ("N", 2.0)];
        const CLASS_II_MAJOR: [(&str, f32); 6] = [
            ("H", 67.0),
            ("He", 20.0),
            ("O", 4.0),
            ("C", 3.0),
            ("N", 2.5),
            ("S", 3.5),
        ];
        const CLASS_III_MAJOR: [(&str, f32); 6] = [
            ("H", 62.0),
            ("He", 20.0),
            ("Na", 4.0),
            ("K", 2.2),
            ("C", 6.0),
            ("O", 5.8),
        ];
        const CLASS_IV_MAJOR: [(&str, f32); 7] = [
            ("H", 56.0),
            ("He", 17.0),
            ("Na", 4.0),
            ("K", 3.0),
            ("Fe", 7.0),
            ("Ti", 6.0),
            ("Si", 7.0),
        ];
        const CLASS_V_MAJOR: [(&str, f32); 7] = [
            ("H", 52.0),
            ("He", 16.0),
            ("Na", 4.0),
            ("K", 3.0),
            ("Fe", 9.0),
            ("Ti", 8.0),
            ("V", 8.0),
        ];
        const HELIUM_RICH_MAJOR: [(&str, f32); 4] =
            [("He", 66.0), ("H", 25.0), ("Ne", 5.0), ("Ar", 4.0)];
        const HELIUM_MAJOR: [(&str, f32); 4] =
            [("He", 79.0), ("H", 16.0), ("Ne", 3.0), ("Ar", 2.0)];
        const AMMONIA_LIFE_MAJOR: [(&str, f32); 7] = [
            ("H", 58.0),
            ("N", 17.0),
            ("O", 8.0),
            ("C", 8.0),
            ("S", 4.0),
            ("P", 3.0),
            ("He", 2.0),
        ];
        const WATER_LIFE_MAJOR: [(&str, f32); 7] = [
            ("H", 56.0),
            ("O", 18.0),
            ("C", 8.0),
            ("N", 7.0),
            ("S", 4.0),
            ("P", 3.0),
            ("He", 4.0),
        ];
        const WATER_GIANT_MAJOR: [(&str, f32); 7] = [
            ("O", 36.0),
            ("H", 33.0),
            ("He", 12.0),
            ("C", 7.0),
            ("N", 5.0),
            ("Na", 4.0),
            ("Mg", 3.0),
        ];
        const LEGACY_GAS_MAJOR: [(&str, f32); 5] =
            [("H", 69.0), ("He", 21.0), ("C", 4.0), ("O", 4.0), ("N", 2.0)];

        const TERRESTRIAL_TRACE: [&str; 12] =
            ["Ti", "Mn", "Cr", "P", "S", "Ni", "Ca", "Na", "K", "Al", "C", "Zn"];
        const ICY_TRACE: [&str; 11] =
            ["Ne", "Ar", "S", "Na", "Mg", "Si", "P", "Cl", "K", "Fe", "Ca"];
        const GIANT_TRACE: [&str; 11] =
            ["Ne", "Ar", "Na", "K", "S", "P", "C", "N", "O", "Si", "Ti"];
        const METAL_TRACE: [&str; 9] = ["Cu", "Zn", "Mn", "Mo", "W", "Ir", "Pt", "V", "Ti"];

        match kind {
            PlanetKind::EarthLikeWorld => (&EARTH_LIKE_MAJOR, &TERRESTRIAL_TRACE),
            PlanetKind::Rocky => (&ROCKY_MAJOR, &TERRESTRIAL_TRACE),
            PlanetKind::RockyIceWorld => (&ROCKY_ICE_MAJOR, &ICY_TRACE),
            PlanetKind::Icy => (&ICY_MAJOR, &ICY_TRACE),
            PlanetKind::WaterWorld => (&WATER_MAJOR, &ICY_TRACE),
            PlanetKind::AmmoniaWorld => (&AMMONIA_MAJOR, &ICY_TRACE),
            PlanetKind::MetalRich => (&METAL_RICH_MAJOR, &METAL_TRACE),
            PlanetKind::Metal => (&METAL_MAJOR, &METAL_TRACE),
            PlanetKind::GasGiantClassI => (&CLASS_I_MAJOR, &GIANT_TRACE),
            PlanetKind::GasGiantClassII => (&CLASS_II_MAJOR, &GIANT_TRACE),
            PlanetKind::GasGiantClassIII => (&CLASS_III_MAJOR, &GIANT_TRACE),
            PlanetKind::GasGiantClassIV => (&CLASS_IV_MAJOR, &GIANT_TRACE),
            PlanetKind::GasGiantClassV => (&CLASS_V_MAJOR, &GIANT_TRACE),
            PlanetKind::HeliumRichGasGiant => (&HELIUM_RICH_MAJOR, &GIANT_TRACE),
            PlanetKind::HeliumGasGiant => (&HELIUM_MAJOR, &GIANT_TRACE),
            PlanetKind::GasGiantAmmoniaLife => (&AMMONIA_LIFE_MAJOR, &GIANT_TRACE),
            PlanetKind::GasGiantWaterLife => (&WATER_LIFE_MAJOR, &GIANT_TRACE),
            PlanetKind::WaterGiant => (&WATER_GIANT_MAJOR, &GIANT_TRACE),
            PlanetKind::GasGiant => (&LEGACY_GAS_MAJOR, &GIANT_TRACE),
        }
    }

    fn adjusted_composition_weight(
        kind: PlanetKind,
        symbol: &str,
        base_weight: f32,
        temperature_k: f32,
        rng: &mut StdRng,
    ) -> f32 {
        let mut weight = base_weight * rng.gen_range(0.86..1.16);
        let hot_factor = ((temperature_k - 450.0) / 1_100.0).clamp(0.0, 1.0);
        let cold_factor = ((220.0 - temperature_k) / 180.0).clamp(0.0, 1.0);

        let refractory = matches!(symbol, "Fe" | "Ni" | "Mg" | "Si" | "Al" | "Ca" | "Ti");
        let volatile = matches!(symbol, "H" | "C" | "N" | "O" | "S" | "Cl");

        if !kind.is_gas_giant() {
            if volatile {
                weight *= 1.0 - 0.55 * hot_factor;
            }
            if refractory {
                weight *= 1.0 + 0.30 * hot_factor;
            }
        }

        if matches!(
            kind,
            PlanetKind::Icy
                | PlanetKind::RockyIceWorld
                | PlanetKind::WaterWorld
                | PlanetKind::AmmoniaWorld
                | PlanetKind::WaterGiant
                | PlanetKind::GasGiantAmmoniaLife
                | PlanetKind::GasGiantWaterLife
        ) {
            if volatile {
                weight *= 1.0 + 0.42 * cold_factor;
            }
        }

        if matches!(kind, PlanetKind::HeliumGasGiant | PlanetKind::HeliumRichGasGiant)
            && symbol == "He"
        {
            weight *= 1.0 + 0.25 * hot_factor;
        }

        weight.max(0.0001)
    }

    fn sample_planet_composition(
        kind: PlanetKind,
        temperature_k: f32,
        radius_earth: f32,
        mass_earth: f32,
        orbit_au: f32,
        host_planet_index: Option<u8>,
    ) -> Vec<PlanetElementComponent> {
        let mut seed = mix64(
            (kind as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ (temperature_k.to_bits() as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
                ^ (radius_earth.to_bits() as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93)
                ^ (mass_earth.to_bits() as u64).wrapping_mul(0xA24B_AED4_963E_E407)
                ^ (orbit_au.to_bits() as u64).wrapping_mul(0x94D0_49BB_1331_11EB),
        );
        if let Some(host_index) = host_planet_index {
            seed ^= (host_index as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            seed = mix64(seed);
        }

        let mut rng = StdRng::seed_from_u64(seed);
        let (major_components, trace_pool) = Self::composition_profile(kind);
        let mut weights: Vec<(&'static str, f32)> = Vec::with_capacity(major_components.len() + 4);

        for (symbol, base_weight) in major_components {
            let adjusted =
                Self::adjusted_composition_weight(kind, symbol, *base_weight, temperature_k, &mut rng);
            if let Some(entry) = weights.iter_mut().find(|(s, _)| *s == *symbol) {
                entry.1 += adjusted;
            } else {
                weights.push((*symbol, adjusted));
            }
        }

        let mut trace_candidates = trace_pool.to_vec();
        let trace_count = rng
            .gen_range(1..=4)
            .min(trace_candidates.len());
        for _ in 0..trace_count {
            if trace_candidates.is_empty() {
                break;
            }
            let idx = rng.gen_range(0..trace_candidates.len());
            let trace_symbol = trace_candidates.swap_remove(idx);
            let trace_weight = rng.gen_range(0.08..0.75);
            if let Some(entry) = weights.iter_mut().find(|(s, _)| *s == trace_symbol) {
                entry.1 += trace_weight;
            } else {
                weights.push((trace_symbol, trace_weight));
            }
        }

        let total_weight = weights.iter().map(|(_, w)| *w).sum::<f32>().max(0.001);
        let mut composition = weights
            .into_iter()
            .map(|(symbol, weight)| {
                let (atomic_number, name) = Self::element_metadata(symbol);
                PlanetElementComponent {
                    atomic_number,
                    symbol: symbol.to_owned(),
                    name: name.to_owned(),
                    percent: (weight / total_weight) * 100.0,
                }
            })
            .collect::<Vec<_>>();

        composition.sort_by(|a, b| {
            b.percent
                .partial_cmp(&a.percent)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut rounded_sum = 0.0;
        for entry in &mut composition {
            entry.percent = (entry.percent * 100.0).round() / 100.0;
            rounded_sum += entry.percent;
        }

        if let Some(first) = composition.first_mut() {
            let correction = 100.0 - rounded_sum;
            first.percent = (first.percent + correction).max(0.0);
            first.percent = (first.percent * 100.0).round() / 100.0;
        }

        composition.retain(|entry| entry.percent > 0.0 && entry.atomic_number > 0);
        composition
    }

    fn atmosphere_gas_name(formula: &str) -> &'static str {
        match formula {
            "H2" => "Hydrogen",
            "He" => "Helium",
            "N2" => "Nitrogen",
            "O2" => "Oxygen",
            "CO2" => "Carbon dioxide",
            "CO" => "Carbon monoxide",
            "CH4" => "Methane",
            "NH3" => "Ammonia",
            "H2O" => "Water vapor",
            "SO2" => "Sulfur dioxide",
            "H2S" => "Hydrogen sulfide",
            "Ne" => "Neon",
            "Ar" => "Argon",
            "Kr" => "Krypton",
            "Xe" => "Xenon",
            _ => "Unknown gas",
        }
    }

    fn atmosphere_profile(kind: PlanetKind) -> (&'static [(&'static str, f32)], &'static [&'static str]) {
        const EARTH_LIKE_GASES: [(&str, f32); 6] = [
            ("N2", 72.0),
            ("O2", 24.0),
            ("Ar", 1.4),
            ("CO2", 0.8),
            ("H2O", 1.6),
            ("Ne", 0.2),
        ];
        const ROCKY_GASES: [(&str, f32); 7] = [
            ("CO2", 56.0),
            ("N2", 24.0),
            ("SO2", 7.0),
            ("Ar", 4.0),
            ("O2", 2.0),
            ("H2O", 4.0),
            ("CO", 3.0),
        ];
        const ROCKY_ICE_GASES: [(&str, f32); 8] = [
            ("N2", 39.0),
            ("CH4", 17.0),
            ("CO2", 19.0),
            ("Ar", 8.0),
            ("H2", 5.0),
            ("Ne", 2.0),
            ("H2O", 8.0),
            ("NH3", 2.0),
        ];
        const ICY_GASES: [(&str, f32); 8] = [
            ("N2", 36.0),
            ("CH4", 25.0),
            ("CO2", 18.0),
            ("H2", 7.0),
            ("He", 5.0),
            ("Ne", 4.0),
            ("Ar", 3.0),
            ("NH3", 2.0),
        ];
        const WATER_WORLD_GASES: [(&str, f32); 8] = [
            ("N2", 54.0),
            ("H2O", 18.0),
            ("O2", 10.0),
            ("CO2", 8.0),
            ("Ar", 5.0),
            ("CH4", 3.0),
            ("Ne", 1.0),
            ("He", 1.0),
        ];
        const AMMONIA_WORLD_GASES: [(&str, f32); 7] = [
            ("H2", 46.0),
            ("N2", 22.0),
            ("NH3", 16.0),
            ("CH4", 8.0),
            ("He", 5.0),
            ("Ar", 2.0),
            ("CO2", 1.0),
        ];
        const METAL_RICH_GASES: [(&str, f32); 8] = [
            ("CO2", 50.0),
            ("SO2", 18.0),
            ("N2", 16.0),
            ("O2", 8.0),
            ("Ar", 4.0),
            ("CO", 2.0),
            ("Ne", 1.0),
            ("H2O", 1.0),
        ];
        const METAL_WORLD_GASES: [(&str, f32); 7] = [
            ("CO2", 34.0),
            ("SO2", 23.0),
            ("CO", 16.0),
            ("O2", 11.0),
            ("Ar", 9.0),
            ("Ne", 5.0),
            ("N2", 2.0),
        ];
        const CLASS_I_GASES: [(&str, f32); 6] = [
            ("H2", 82.0),
            ("He", 13.0),
            ("CH4", 2.5),
            ("NH3", 1.5),
            ("H2O", 0.7),
            ("Ne", 0.3),
        ];
        const CLASS_II_GASES: [(&str, f32); 7] = [
            ("H2", 78.0),
            ("He", 14.0),
            ("H2O", 3.0),
            ("CH4", 2.0),
            ("NH3", 1.0),
            ("CO2", 1.5),
            ("Ne", 0.5),
        ];
        const CLASS_III_GASES: [(&str, f32); 8] = [
            ("H2", 71.0),
            ("He", 16.0),
            ("CH4", 4.0),
            ("H2O", 3.0),
            ("NH3", 2.0),
            ("CO2", 2.0),
            ("H2S", 1.5),
            ("Ar", 0.5),
        ];
        const CLASS_IV_GASES: [(&str, f32); 8] = [
            ("H2", 62.0),
            ("He", 13.0),
            ("CO", 5.0),
            ("H2O", 4.0),
            ("SO2", 5.0),
            ("H2S", 4.0),
            ("CH4", 4.0),
            ("N2", 3.0),
        ];
        const CLASS_V_GASES: [(&str, f32); 8] = [
            ("H2", 56.0),
            ("He", 12.0),
            ("CO", 8.0),
            ("H2O", 7.0),
            ("SO2", 5.0),
            ("H2S", 5.0),
            ("CH4", 4.0),
            ("N2", 3.0),
        ];
        const HELIUM_RICH_GASES: [(&str, f32); 5] =
            [("He", 72.0), ("H2", 22.0), ("Ne", 3.0), ("Ar", 2.0), ("Kr", 1.0)];
        const HELIUM_GASES: [(&str, f32); 5] =
            [("He", 86.0), ("H2", 10.0), ("Ne", 2.0), ("Ar", 1.0), ("Kr", 1.0)];
        const AMMONIA_LIFE_GASES: [(&str, f32); 8] = [
            ("H2", 62.0),
            ("NH3", 14.0),
            ("N2", 9.0),
            ("CH4", 6.0),
            ("H2O", 4.0),
            ("He", 3.0),
            ("Ar", 1.0),
            ("Ne", 1.0),
        ];
        const WATER_LIFE_GASES: [(&str, f32); 8] = [
            ("H2", 58.0),
            ("H2O", 14.0),
            ("CH4", 8.0),
            ("N2", 7.0),
            ("CO2", 5.0),
            ("O2", 4.0),
            ("He", 2.0),
            ("Ar", 2.0),
        ];
        const WATER_GIANT_GASES: [(&str, f32); 8] = [
            ("H2", 49.0),
            ("H2O", 21.0),
            ("He", 17.0),
            ("CH4", 5.0),
            ("NH3", 4.0),
            ("Ne", 2.0),
            ("Ar", 1.0),
            ("CO2", 1.0),
        ];
        const LEGACY_GAS_GASES: [(&str, f32); 6] = [
            ("H2", 80.0),
            ("He", 15.0),
            ("CH4", 2.0),
            ("NH3", 1.5),
            ("H2O", 1.0),
            ("Ne", 0.5),
        ];

        const TERRAN_TRACE: [&str; 11] =
            ["CO2", "H2O", "SO2", "Ar", "Ne", "Kr", "Xe", "CH4", "NH3", "CO", "N2"];
        const GIANT_TRACE: [&str; 11] =
            ["He", "Ne", "Ar", "Kr", "Xe", "CH4", "NH3", "H2O", "H2S", "CO2", "CO"];
        const METAL_TRACE: [&str; 10] =
            ["SO2", "CO2", "CO", "O2", "N2", "Ar", "Ne", "Kr", "Xe", "H2S"];

        match kind {
            PlanetKind::EarthLikeWorld => (&EARTH_LIKE_GASES, &TERRAN_TRACE),
            PlanetKind::Rocky => (&ROCKY_GASES, &TERRAN_TRACE),
            PlanetKind::RockyIceWorld => (&ROCKY_ICE_GASES, &TERRAN_TRACE),
            PlanetKind::Icy => (&ICY_GASES, &TERRAN_TRACE),
            PlanetKind::WaterWorld => (&WATER_WORLD_GASES, &TERRAN_TRACE),
            PlanetKind::AmmoniaWorld => (&AMMONIA_WORLD_GASES, &TERRAN_TRACE),
            PlanetKind::MetalRich => (&METAL_RICH_GASES, &METAL_TRACE),
            PlanetKind::Metal => (&METAL_WORLD_GASES, &METAL_TRACE),
            PlanetKind::GasGiantClassI => (&CLASS_I_GASES, &GIANT_TRACE),
            PlanetKind::GasGiantClassII => (&CLASS_II_GASES, &GIANT_TRACE),
            PlanetKind::GasGiantClassIII => (&CLASS_III_GASES, &GIANT_TRACE),
            PlanetKind::GasGiantClassIV => (&CLASS_IV_GASES, &GIANT_TRACE),
            PlanetKind::GasGiantClassV => (&CLASS_V_GASES, &GIANT_TRACE),
            PlanetKind::HeliumRichGasGiant => (&HELIUM_RICH_GASES, &GIANT_TRACE),
            PlanetKind::HeliumGasGiant => (&HELIUM_GASES, &GIANT_TRACE),
            PlanetKind::GasGiantAmmoniaLife => (&AMMONIA_LIFE_GASES, &GIANT_TRACE),
            PlanetKind::GasGiantWaterLife => (&WATER_LIFE_GASES, &GIANT_TRACE),
            PlanetKind::WaterGiant => (&WATER_GIANT_GASES, &GIANT_TRACE),
            PlanetKind::GasGiant => (&LEGACY_GAS_GASES, &GIANT_TRACE),
        }
    }

    fn atmosphere_presence_probability(
        kind: PlanetKind,
        temperature_k: f32,
        radius_earth: f32,
        mass_earth: f32,
        is_moon: bool,
    ) -> f32 {
        if kind.is_gas_giant() {
            return 1.0;
        }

        let mut probability: f32 = match kind {
            PlanetKind::EarthLikeWorld => 0.98,
            PlanetKind::Rocky => 0.55,
            PlanetKind::RockyIceWorld => 0.70,
            PlanetKind::Icy => 0.62,
            PlanetKind::WaterWorld => 0.92,
            PlanetKind::AmmoniaWorld => 0.86,
            PlanetKind::MetalRich => 0.36,
            PlanetKind::Metal => 0.14,
            _ => 0.45,
        };

        let escape_proxy = (mass_earth / radius_earth.max(0.05)).clamp(0.0, 8.0);
        if escape_proxy < 0.45 {
            probability *= 0.45;
        } else if escape_proxy < 0.80 {
            probability *= 0.68;
        } else if escape_proxy > 1.6 {
            probability *= 1.15;
        }

        if temperature_k > 1_400.0 {
            probability *= 0.22;
        } else if temperature_k > 950.0 {
            probability *= 0.45;
        } else if temperature_k < 90.0 {
            probability *= 1.08;
        }

        if is_moon {
            probability *= 0.72;
        }

        probability.clamp(0.02_f32, 0.995_f32)
    }

    fn atmosphere_pressure_range_atm(kind: PlanetKind) -> (f32, f32) {
        match kind {
            PlanetKind::EarthLikeWorld => (0.65, 2.8),
            PlanetKind::Rocky => (0.03, 4.2),
            PlanetKind::RockyIceWorld => (0.06, 3.1),
            PlanetKind::Icy => (0.01, 1.4),
            PlanetKind::WaterWorld => (0.80, 9.5),
            PlanetKind::AmmoniaWorld => (0.45, 6.0),
            PlanetKind::MetalRich => (0.01, 1.9),
            PlanetKind::Metal => (0.0, 0.35),
            PlanetKind::GasGiantClassI => (28.0, 260.0),
            PlanetKind::GasGiantClassII => (24.0, 310.0),
            PlanetKind::GasGiantClassIII => (32.0, 380.0),
            PlanetKind::GasGiantClassIV => (40.0, 450.0),
            PlanetKind::GasGiantClassV => (46.0, 560.0),
            PlanetKind::HeliumRichGasGiant => (48.0, 640.0),
            PlanetKind::HeliumGasGiant => (52.0, 720.0),
            PlanetKind::GasGiantAmmoniaLife => (30.0, 320.0),
            PlanetKind::GasGiantWaterLife => (26.0, 300.0),
            PlanetKind::WaterGiant => (20.0, 230.0),
            PlanetKind::GasGiant => (24.0, 340.0),
        }
    }

    fn adjusted_atmosphere_weight(
        kind: PlanetKind,
        formula: &str,
        base_weight: f32,
        temperature_k: f32,
        rng: &mut StdRng,
    ) -> f32 {
        let mut weight = base_weight * rng.gen_range(0.84..1.18);
        let hot_factor = ((temperature_k - 500.0) / 1_400.0).clamp(0.0, 1.0);
        let cold_factor = ((210.0 - temperature_k) / 190.0).clamp(0.0, 1.0);

        let fragile = matches!(formula, "H2O" | "NH3" | "CH4");
        let refractory = matches!(formula, "CO2" | "CO" | "SO2");
        let noble = matches!(formula, "He" | "Ne" | "Ar" | "Kr" | "Xe");

        if !kind.is_gas_giant() {
            if fragile {
                weight *= 1.0 - 0.70 * hot_factor;
            }
            if refractory {
                weight *= 1.0 + 0.38 * hot_factor;
            }
        }

        if matches!(
            kind,
            PlanetKind::Icy
                | PlanetKind::RockyIceWorld
                | PlanetKind::WaterWorld
                | PlanetKind::AmmoniaWorld
                | PlanetKind::WaterGiant
                | PlanetKind::GasGiantAmmoniaLife
                | PlanetKind::GasGiantWaterLife
        ) {
            if fragile {
                weight *= 1.0 + 0.46 * cold_factor;
            }
        }

        if noble {
            weight *= 1.0 + rng.gen_range(0.00..0.18);
        }

        if matches!(kind, PlanetKind::HeliumGasGiant | PlanetKind::HeliumRichGasGiant)
            && formula == "He"
        {
            weight *= 1.0 + 0.24 * hot_factor;
        }

        weight.max(0.0001)
    }

    fn sample_planet_atmosphere(
        kind: PlanetKind,
        temperature_k: f32,
        radius_earth: f32,
        mass_earth: f32,
        orbit_au: f32,
        host_planet_index: Option<u8>,
    ) -> (Vec<PlanetAtmosphereComponent>, f32) {
        let mut seed = mix64(
            (kind as u64).wrapping_mul(0x8B7A_9D51_E13C_F127)
                ^ (temperature_k.to_bits() as u64).wrapping_mul(0xA6D1_7C21_BF58_476D)
                ^ (radius_earth.to_bits() as u64).wrapping_mul(0xC4CE_B9FE_1A85_EC53)
                ^ (mass_earth.to_bits() as u64).wrapping_mul(0x94D0_49BB_1331_11EB)
                ^ (orbit_au.to_bits() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        );
        if let Some(host_index) = host_planet_index {
            seed ^= (host_index as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93);
            seed = mix64(seed);
        }

        let mut rng = StdRng::seed_from_u64(seed);
        let probability = Self::atmosphere_presence_probability(
            kind,
            temperature_k,
            radius_earth,
            mass_earth,
            host_planet_index.is_some(),
        );
        if rng.r#gen::<f32>() > probability {
            return (Vec::new(), 0.0);
        }

        let (major_gases, trace_pool) = Self::atmosphere_profile(kind);
        let mut weights: Vec<(&'static str, f32)> = Vec::with_capacity(major_gases.len() + 5);
        for (formula, base_weight) in major_gases {
            let adjusted = Self::adjusted_atmosphere_weight(
                kind,
                formula,
                *base_weight,
                temperature_k,
                &mut rng,
            );
            if let Some(entry) = weights.iter_mut().find(|(s, _)| *s == *formula) {
                entry.1 += adjusted;
            } else {
                weights.push((*formula, adjusted));
            }
        }

        let mut trace_candidates = trace_pool.to_vec();
        let trace_count = rng.gen_range(1..=5).min(trace_candidates.len());
        for _ in 0..trace_count {
            if trace_candidates.is_empty() {
                break;
            }
            let idx = rng.gen_range(0..trace_candidates.len());
            let trace_formula = trace_candidates.swap_remove(idx);
            let trace_weight = rng.gen_range(0.03..0.55);
            if let Some(entry) = weights.iter_mut().find(|(s, _)| *s == trace_formula) {
                entry.1 += trace_weight;
            } else {
                weights.push((trace_formula, trace_weight));
            }
        }

        let total_weight = weights.iter().map(|(_, w)| *w).sum::<f32>().max(0.001);
        let mut gases = weights
            .into_iter()
            .map(|(formula, weight)| PlanetAtmosphereComponent {
                formula: formula.to_owned(),
                name: Self::atmosphere_gas_name(formula).to_owned(),
                percent: (weight / total_weight) * 100.0,
            })
            .collect::<Vec<_>>();

        gases.sort_by(|a, b| {
            b.percent
                .partial_cmp(&a.percent)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut rounded_sum = 0.0;
        for gas in &mut gases {
            gas.percent = (gas.percent * 100.0).round() / 100.0;
            rounded_sum += gas.percent;
        }
        if let Some(first) = gases.first_mut() {
            let correction = 100.0 - rounded_sum;
            first.percent = (first.percent + correction).max(0.0);
            first.percent = (first.percent * 100.0).round() / 100.0;
        }

        gases.retain(|gas| gas.percent > 0.0);
        if gases.is_empty() {
            return (Vec::new(), 0.0);
        }

        let (min_pressure, max_pressure) = Self::atmosphere_pressure_range_atm(kind);
        let mut pressure = rng.gen_range(min_pressure..=max_pressure);
        if !kind.is_gas_giant() {
            let escape_proxy = (mass_earth / radius_earth.max(0.05)).clamp(0.15, 8.0);
            let retention_factor = (escape_proxy / 1.25).clamp(0.22, 1.9);
            let thermal_loss = ((temperature_k - 900.0) / 1_100.0).clamp(0.0, 0.78);
            pressure *= retention_factor * (1.0 - thermal_loss);
        }
        if host_planet_index.is_some() {
            pressure *= 0.72;
        }

        pressure = (pressure * 100.0).round() / 100.0;
        if pressure < 0.01 {
            return (Vec::new(), 0.0);
        }

        (gases, pressure)
    }

    fn sample_planet_size_and_mass(
        rng: &mut StdRng,
        kind: PlanetKind,
        is_moon: bool,
    ) -> (f32, f32) {
        let (radius_min, radius_max, density_min, density_max) = match (kind, is_moon) {
            (PlanetKind::EarthLikeWorld, false) => (0.85, 1.35, 0.90, 1.25),
            (PlanetKind::EarthLikeWorld, true) => (0.45, 0.95, 0.88, 1.18),
            (PlanetKind::Rocky, false) => (0.35, 1.85, 0.75, 1.45),
            (PlanetKind::Rocky, true) => (0.08, 0.95, 0.70, 1.35),
            (PlanetKind::RockyIceWorld, false) => (0.32, 1.70, 0.42, 1.05),
            (PlanetKind::RockyIceWorld, true) => (0.06, 0.90, 0.30, 0.95),
            (PlanetKind::Icy, false) => (0.25, 1.55, 0.30, 0.82),
            (PlanetKind::Icy, true) => (0.06, 0.85, 0.22, 0.72),
            (PlanetKind::WaterWorld, false) => (0.65, 2.60, 0.82, 1.40),
            (PlanetKind::WaterWorld, true) => (0.12, 1.35, 0.70, 1.22),
            (PlanetKind::AmmoniaWorld, false) => (0.45, 2.10, 0.55, 1.18),
            (PlanetKind::AmmoniaWorld, true) => (0.10, 1.05, 0.42, 1.00),
            (PlanetKind::MetalRich, false) => (0.28, 1.65, 1.10, 2.20),
            (PlanetKind::MetalRich, true) => (0.05, 0.78, 1.00, 2.00),
            (PlanetKind::Metal, false) => (0.20, 1.20, 1.50, 3.25),
            (PlanetKind::Metal, true) => (0.04, 0.58, 1.35, 3.00),
            (PlanetKind::GasGiantClassI, false) => (6.2, 14.6, 0.04, 0.18),
            (PlanetKind::GasGiantClassI, true) => (0.35, 2.2, 0.08, 0.25),
            (PlanetKind::GasGiantClassII, false) => (5.0, 13.0, 0.05, 0.22),
            (PlanetKind::GasGiantClassII, true) => (0.35, 2.2, 0.08, 0.25),
            (PlanetKind::GasGiantClassIII, false) => (4.1, 11.8, 0.07, 0.28),
            (PlanetKind::GasGiantClassIII, true) => (0.35, 2.2, 0.08, 0.25),
            (PlanetKind::GasGiantClassIV, false) => (4.0, 11.2, 0.09, 0.32),
            (PlanetKind::GasGiantClassIV, true) => (0.35, 2.2, 0.08, 0.25),
            (PlanetKind::GasGiantClassV, false) => (4.2, 12.4, 0.09, 0.34),
            (PlanetKind::GasGiantClassV, true) => (0.35, 2.2, 0.08, 0.25),
            (PlanetKind::HeliumRichGasGiant, false) => (6.0, 15.2, 0.035, 0.17),
            (PlanetKind::HeliumRichGasGiant, true) => (0.35, 2.2, 0.08, 0.25),
            (PlanetKind::HeliumGasGiant, false) => (6.6, 16.2, 0.025, 0.14),
            (PlanetKind::HeliumGasGiant, true) => (0.35, 2.2, 0.08, 0.25),
            (PlanetKind::GasGiantAmmoniaLife, false) => (5.2, 13.2, 0.05, 0.20),
            (PlanetKind::GasGiantAmmoniaLife, true) => (0.35, 2.2, 0.08, 0.25),
            (PlanetKind::GasGiantWaterLife, false) => (4.7, 12.2, 0.06, 0.24),
            (PlanetKind::GasGiantWaterLife, true) => (0.35, 2.2, 0.08, 0.25),
            (PlanetKind::WaterGiant, false) => (2.8, 8.2, 0.18, 0.52),
            (PlanetKind::WaterGiant, true) => (0.30, 1.8, 0.16, 0.50),
            (PlanetKind::GasGiant, false) => (3.2, 11.8, 0.06, 0.24),
            (PlanetKind::GasGiant, true) => (0.35, 2.2, 0.08, 0.25),
        };

        let radius: f32 = rng.gen_range(radius_min..radius_max);
        let density_factor: f32 = rng.gen_range(density_min..density_max);
        let mut mass: f32 = radius.powi(3) * density_factor;

        // Gas giants are low-density but massive compared with terrestrial worlds.
        if kind.is_gas_giant() {
            mass *= match kind {
                PlanetKind::WaterGiant => 7.0,
                PlanetKind::HeliumRichGasGiant => 10.0,
                PlanetKind::HeliumGasGiant => 8.5,
                _ => 12.0,
            };
        }

        (radius.max(0.02), mass.max(0.01))
    }

    fn default_gas_giant_class_for_temperature(equilibrium_temp: f32) -> PlanetKind {
        if equilibrium_temp < 160.0 {
            PlanetKind::GasGiantClassI
        } else if equilibrium_temp < 260.0 {
            PlanetKind::GasGiantClassII
        } else if equilibrium_temp < 900.0 {
            PlanetKind::GasGiantClassIII
        } else if equilibrium_temp < 1_400.0 {
            PlanetKind::GasGiantClassIV
        } else {
            PlanetKind::GasGiantClassV
        }
    }

    fn classify_standard_gas_giant_kind(rng: &mut StdRng, equilibrium_temp: f32) -> PlanetKind {
        let roll = rng.r#gen::<f32>();

        if equilibrium_temp < 160.0 {
            if roll < 0.78 {
                PlanetKind::GasGiantClassI
            } else if roll < 0.96 {
                PlanetKind::GasGiantClassII
            } else {
                PlanetKind::GasGiantClassIII
            }
        } else if equilibrium_temp < 260.0 {
            if roll < 0.66 {
                PlanetKind::GasGiantClassII
            } else if roll < 0.88 {
                PlanetKind::GasGiantClassI
            } else if roll < 0.98 {
                PlanetKind::GasGiantClassIII
            } else {
                PlanetKind::GasGiantClassIV
            }
        } else if equilibrium_temp < 900.0 {
            if roll < 0.62 {
                PlanetKind::GasGiantClassIII
            } else if roll < 0.84 {
                PlanetKind::GasGiantClassII
            } else if roll < 0.94 {
                PlanetKind::GasGiantClassI
            } else if roll < 0.99 {
                PlanetKind::GasGiantClassIV
            } else {
                PlanetKind::GasGiantClassV
            }
        } else if equilibrium_temp < 1_400.0 {
            if roll < 0.42 {
                PlanetKind::GasGiantClassIV
            } else if roll < 0.75 {
                PlanetKind::GasGiantClassIII
            } else if roll < 0.87 {
                PlanetKind::GasGiantClassV
            } else if roll < 0.95 {
                PlanetKind::GasGiantClassII
            } else {
                PlanetKind::GasGiantClassI
            }
        } else if roll < 0.46 {
            PlanetKind::GasGiantClassV
        } else if roll < 0.76 {
            PlanetKind::GasGiantClassIV
        } else if roll < 0.90 {
            PlanetKind::GasGiantClassIII
        } else if roll < 0.97 {
            PlanetKind::GasGiantClassII
        } else {
            PlanetKind::GasGiantClassI
        }
    }

    fn classify_gas_giant_kind(rng: &mut StdRng, equilibrium_temp: f32) -> PlanetKind {
        let roll = rng.r#gen::<f32>();
        let mut threshold = 0.0;

        if equilibrium_temp >= 450.0 {
            threshold += 0.004;
            if roll < threshold {
                return PlanetKind::HeliumGasGiant;
            }
        }
        if equilibrium_temp >= 250.0 {
            threshold += 0.03;
            if roll < threshold {
                return PlanetKind::HeliumRichGasGiant;
            }
        }
        if equilibrium_temp >= 90.0 && equilibrium_temp <= 260.0 {
            threshold += 0.08;
            if roll < threshold {
                return PlanetKind::GasGiantAmmoniaLife;
            }
        }
        if equilibrium_temp >= 170.0 && equilibrium_temp <= 360.0 {
            threshold += 0.08;
            if roll < threshold {
                return PlanetKind::GasGiantWaterLife;
            }
        }
        if equilibrium_temp >= 120.0 && equilibrium_temp <= 700.0 {
            threshold += 0.05;
            if roll < threshold {
                return PlanetKind::WaterGiant;
            }
        }

        Self::classify_standard_gas_giant_kind(rng, equilibrium_temp)
    }

    fn enforce_thermal_kind_constraints(kind: PlanetKind, equilibrium_temp: f32) -> PlanetKind {
        if kind == PlanetKind::EarthLikeWorld {
            if equilibrium_temp >= 240.0 && equilibrium_temp <= 330.0 {
                return kind;
            }
            return PlanetKind::Rocky;
        }

        if kind == PlanetKind::WaterWorld {
            if equilibrium_temp < 180.0 {
                return PlanetKind::RockyIceWorld;
            }
            if equilibrium_temp <= 550.0 {
                return kind;
            }
            if equilibrium_temp >= 1_500.0 {
                return PlanetKind::Metal;
            }
            if equilibrium_temp >= 850.0 {
                return PlanetKind::MetalRich;
            }
            return PlanetKind::Rocky;
        }

        if kind == PlanetKind::AmmoniaWorld {
            if equilibrium_temp < 80.0 {
                return PlanetKind::Icy;
            }
            if equilibrium_temp <= 320.0 {
                return kind;
            }
            if equilibrium_temp >= 1_100.0 {
                return PlanetKind::Metal;
            }
            if equilibrium_temp >= 650.0 {
                return PlanetKind::MetalRich;
            }
            return PlanetKind::Rocky;
        }

        if kind.is_gas_giant() {
            let fallback = Self::default_gas_giant_class_for_temperature(equilibrium_temp);
            match kind {
                PlanetKind::GasGiantClassI if equilibrium_temp > 260.0 => return fallback,
                PlanetKind::GasGiantClassII
                    if equilibrium_temp < 120.0 || equilibrium_temp > 450.0 =>
                {
                    return fallback;
                }
                PlanetKind::GasGiantClassIII
                    if equilibrium_temp < 200.0 || equilibrium_temp > 1_300.0 =>
                {
                    return fallback;
                }
                PlanetKind::GasGiantClassIV
                    if equilibrium_temp < 750.0 || equilibrium_temp > 1_900.0 =>
                {
                    return fallback;
                }
                PlanetKind::GasGiantClassV if equilibrium_temp < 1_200.0 => return fallback,
                PlanetKind::HeliumRichGasGiant if equilibrium_temp < 220.0 => return fallback,
                PlanetKind::HeliumGasGiant if equilibrium_temp < 420.0 => return fallback,
                PlanetKind::GasGiantAmmoniaLife
                    if equilibrium_temp < 90.0 || equilibrium_temp > 260.0 =>
                {
                    return fallback;
                }
                PlanetKind::GasGiantWaterLife
                    if equilibrium_temp < 170.0 || equilibrium_temp > 360.0 =>
                {
                    return fallback;
                }
                PlanetKind::WaterGiant
                    if equilibrium_temp < 120.0 || equilibrium_temp > 900.0 =>
                {
                    return fallback;
                }
                PlanetKind::GasGiant => return fallback,
                _ => return kind,
            }
        }

        // Safety belt against implausible combinations from future probability tuning.
        if matches!(kind, PlanetKind::Icy | PlanetKind::RockyIceWorld)
            && equilibrium_temp >= 260.0
        {
            if equilibrium_temp >= 1_600.0 {
                PlanetKind::Metal
            } else if equilibrium_temp >= 850.0 {
                PlanetKind::MetalRich
            } else {
                PlanetKind::Rocky
            }
        } else {
            kind
        }
    }

    fn classify_primary_kind(rng: &mut StdRng, equilibrium_temp: f32) -> PlanetKind {
        let roll = rng.r#gen::<f32>();

        let kind = if equilibrium_temp < 140.0 {
            if roll < 0.52 {
                PlanetKind::RockyIceWorld
            } else if roll < 0.79 {
                PlanetKind::Icy
            } else if roll < 0.90 {
                PlanetKind::Rocky
            } else if roll < 0.95 {
                PlanetKind::MetalRich
            } else if roll < 0.97 {
                PlanetKind::AmmoniaWorld
            } else {
                Self::classify_gas_giant_kind(rng, equilibrium_temp)
            }
        } else if equilibrium_temp < 230.0 {
            if roll < 0.42 {
                PlanetKind::RockyIceWorld
            } else if roll < 0.56 {
                PlanetKind::Icy
            } else if roll < 0.78 {
                PlanetKind::Rocky
            } else if roll < 0.84 {
                PlanetKind::WaterWorld
            } else if roll < 0.87 {
                PlanetKind::AmmoniaWorld
            } else if roll < 0.93 {
                PlanetKind::MetalRich
            } else if roll < 0.96 {
                PlanetKind::Metal
            } else {
                Self::classify_gas_giant_kind(rng, equilibrium_temp)
            }
        } else if equilibrium_temp < 330.0 {
            if roll < 0.11 {
                PlanetKind::EarthLikeWorld
            } else if roll < 0.50 {
                PlanetKind::Rocky
            } else if roll < 0.58 {
                PlanetKind::RockyIceWorld
            } else if roll < 0.72 {
                PlanetKind::WaterWorld
            } else if roll < 0.75 {
                PlanetKind::AmmoniaWorld
            } else if roll < 0.86 {
                PlanetKind::MetalRich
            } else if roll < 0.93 {
                PlanetKind::Metal
            } else {
                Self::classify_gas_giant_kind(rng, equilibrium_temp)
            }
        } else if equilibrium_temp < 450.0 {
            if roll < 0.50 {
                PlanetKind::Rocky
            } else if roll < 0.62 {
                PlanetKind::WaterWorld
            } else if roll < 0.65 {
                PlanetKind::AmmoniaWorld
            } else if roll < 0.80 {
                PlanetKind::MetalRich
            } else if roll < 0.91 {
                PlanetKind::Metal
            } else {
                Self::classify_gas_giant_kind(rng, equilibrium_temp)
            }
        } else if equilibrium_temp < 900.0 {
            if roll < 0.36 {
                PlanetKind::Rocky
            } else if roll < 0.45 {
                PlanetKind::WaterWorld
            } else if roll < 0.47 {
                PlanetKind::AmmoniaWorld
            } else if roll < 0.73 {
                PlanetKind::MetalRich
            } else if roll < 0.88 {
                PlanetKind::Metal
            } else {
                Self::classify_gas_giant_kind(rng, equilibrium_temp)
            }
        } else if equilibrium_temp < 1_600.0 {
            if roll < 0.12 {
                PlanetKind::Rocky
            } else if roll < 0.36 {
                PlanetKind::MetalRich
            } else if roll < 0.78 {
                PlanetKind::Metal
            } else {
                Self::classify_gas_giant_kind(rng, equilibrium_temp)
            }
        } else {
            if roll < 0.10 {
                PlanetKind::Rocky
            } else if roll < 0.27 {
                PlanetKind::MetalRich
            } else if roll < 0.84 {
                PlanetKind::Metal
            } else {
                Self::classify_gas_giant_kind(rng, equilibrium_temp)
            }
        };

        Self::enforce_thermal_kind_constraints(kind, equilibrium_temp)
    }

    fn classify_moon_kind(rng: &mut StdRng, equilibrium_temp: f32) -> PlanetKind {
        let roll = rng.r#gen::<f32>();

        let kind = if equilibrium_temp < 130.0 {
            if roll < 0.56 {
                PlanetKind::RockyIceWorld
            } else if roll < 0.83 {
                PlanetKind::Icy
            } else if roll < 0.96 {
                PlanetKind::Rocky
            } else if roll < 0.985 {
                PlanetKind::WaterWorld
            } else {
                PlanetKind::AmmoniaWorld
            }
        } else if equilibrium_temp < 220.0 {
            if roll < 0.38 {
                PlanetKind::RockyIceWorld
            } else if roll < 0.49 {
                PlanetKind::Icy
            } else if roll < 0.82 {
                PlanetKind::Rocky
            } else if roll < 0.89 {
                PlanetKind::WaterWorld
            } else if roll < 0.92 {
                PlanetKind::AmmoniaWorld
            } else if roll < 0.97 {
                PlanetKind::MetalRich
            } else {
                PlanetKind::Metal
            }
        } else if equilibrium_temp < 650.0 {
            if roll < 0.55 {
                PlanetKind::Rocky
            } else if roll < 0.64 {
                PlanetKind::WaterWorld
            } else if roll < 0.67 {
                PlanetKind::AmmoniaWorld
            } else if roll < 0.85 {
                PlanetKind::MetalRich
            } else {
                PlanetKind::Metal
            }
        } else if equilibrium_temp < 1_200.0 {
            if roll < 0.17 {
                PlanetKind::Rocky
            } else if roll < 0.20 {
                PlanetKind::WaterWorld
            } else if roll < 0.22 {
                PlanetKind::AmmoniaWorld
            } else if roll < 0.57 {
                PlanetKind::MetalRich
            } else {
                PlanetKind::Metal
            }
        } else {
            if roll < 0.08 {
                PlanetKind::Rocky
            } else if roll < 0.09 {
                PlanetKind::WaterWorld
            } else if roll < 0.10 {
                PlanetKind::AmmoniaWorld
            } else if roll < 0.40 {
                PlanetKind::MetalRich
            } else {
                PlanetKind::Metal
            }
        };

        Self::enforce_thermal_kind_constraints(kind, equilibrium_temp)
    }

    fn sample_planets(rng: &mut StdRng, stars: &[StarBody]) -> Vec<PlanetBody> {
        let avg_luminosity = if stars.is_empty() {
            1.0
        } else {
            stars.iter().map(|s| s.luminosity_solar).sum::<f32>() / stars.len() as f32
        }
        .max(0.0001);
        let total_star_mass_solar = stars
            .iter()
            .map(|s| s.mass_solar.max(0.02))
            .sum::<f32>()
            .max(0.08);

        let body_count = match rng.r#gen::<f32>() {
            roll if roll < 0.10 => 0,
            roll if roll < 0.30 => rng.gen_range(1..=3),
            roll if roll < 0.75 => rng.gen_range(4..=8),
            _ => rng.gen_range(9..=14),
        };

        if body_count == 0 {
            return Vec::new();
        }

        let mut primary_count =
            ((body_count as f32) * rng.gen_range(0.45..0.78)).round() as usize;
        primary_count = primary_count.clamp(1, body_count);

        let mut planets = Vec::with_capacity(body_count);
        let mut orbit: f32 = rng.gen_range(0.20..0.70);

        for _ in 0..primary_count {
            orbit += rng.gen_range(0.12..2.30);
            let equilibrium_temp = Self::equilibrium_temperature_k(avg_luminosity, orbit);
            let kind = Self::classify_primary_kind(rng, equilibrium_temp);

            let (radius_earth, mass_earth) =
                Self::sample_planet_size_and_mass(rng, kind, false);
            let habitable = kind == PlanetKind::EarthLikeWorld
                || (kind == PlanetKind::Rocky
                && equilibrium_temp >= 240.0
                && equilibrium_temp <= 330.0
                && radius_earth >= 0.5
                && radius_earth <= 1.8);
            let composition = Self::sample_planet_composition(
                kind,
                equilibrium_temp,
                radius_earth,
                mass_earth,
                orbit,
                None,
            );
            let (atmosphere, atmosphere_pressure_atm) = Self::sample_planet_atmosphere(
                kind,
                equilibrium_temp,
                radius_earth,
                mass_earth,
                orbit,
                None,
            );

            planets.push(PlanetBody {
                kind,
                orbit_au: orbit,
                host_planet_index: None,
                moon_orbit_au: None,
                radius_earth,
                mass_earth,
                temperature_k: equilibrium_temp,
                habitable,
                composition,
                atmosphere,
                atmosphere_pressure_atm,
            });
        }

        let mut remaining = body_count.saturating_sub(planets.len());
        while remaining > 0 {
            let host_candidates = planets
                .iter()
                .enumerate()
                .filter(|(_, p)| {
                    p.host_planet_index.is_none()
                        && (p.kind.is_gas_giant()
                            || p.radius_earth >= 1.2
                            || p.mass_earth >= 2.0)
                })
                .map(|(idx, _)| idx)
                .collect::<Vec<_>>();

            // If no suitable host exists (or by chance), create another primary body.
            let add_primary = host_candidates.is_empty() || rng.r#gen::<f32>() < 0.24;
            if add_primary {
                orbit += rng.gen_range(0.08..1.70);
                let equilibrium_temp = Self::equilibrium_temperature_k(avg_luminosity, orbit);
                let kind = Self::classify_primary_kind(rng, equilibrium_temp);

                let (radius_earth, mass_earth) =
                    Self::sample_planet_size_and_mass(rng, kind, false);
                let habitable = kind == PlanetKind::EarthLikeWorld
                    || (kind == PlanetKind::Rocky
                    && equilibrium_temp >= 240.0
                    && equilibrium_temp <= 330.0
                    && radius_earth >= 0.5
                    && radius_earth <= 1.8);
                let composition = Self::sample_planet_composition(
                    kind,
                    equilibrium_temp,
                    radius_earth,
                    mass_earth,
                    orbit,
                    None,
                );
                let (atmosphere, atmosphere_pressure_atm) = Self::sample_planet_atmosphere(
                    kind,
                    equilibrium_temp,
                    radius_earth,
                    mass_earth,
                    orbit,
                    None,
                );

                planets.push(PlanetBody {
                    kind,
                    orbit_au: orbit,
                    host_planet_index: None,
                    moon_orbit_au: None,
                    radius_earth,
                    mass_earth,
                    temperature_k: equilibrium_temp,
                    habitable,
                    composition,
                    atmosphere,
                    atmosphere_pressure_atm,
                });
                remaining -= 1;
                continue;
            }

            // Weighted host selection favors larger/more massive planets.
            let total_weight = host_candidates
                .iter()
                .map(|idx| {
                    let host = &planets[*idx];
                    host.radius_earth.powf(1.35) * host.mass_earth.powf(0.18).max(0.6)
                })
                .sum::<f32>()
                .max(0.0001);
            let mut pick = rng.r#gen::<f32>() * total_weight;
            let mut host_index = host_candidates[0];
            for idx in host_candidates {
                let host = &planets[idx];
                let weight = host.radius_earth.powf(1.35) * host.mass_earth.powf(0.18).max(0.6);
                if pick <= weight {
                    host_index = idx;
                    break;
                }
                pick -= weight;
            }

            let host = planets[host_index].clone();
            let host_orbit_au = host.orbit_au.max(0.03);
            let host_mass_solar = (host.mass_earth / 332_946.0).max(1e-7);
            let hill_radius_au =
                host_orbit_au * (host_mass_solar / (3.0 * total_star_mass_solar)).powf(1.0 / 3.0);
            let moon_orbit_min_au = (0.0009 * host.radius_earth).clamp(0.0004, 0.01);
            let moon_orbit_max_au = (hill_radius_au * 0.42).clamp(0.0015, 0.14);
            let moon_orbit_au = if moon_orbit_max_au > moon_orbit_min_au {
                rng.gen_range(moon_orbit_min_au..moon_orbit_max_au)
            } else {
                moon_orbit_max_au
            }
            .max(0.0004);

            let equilibrium_temp = Self::equilibrium_temperature_k(avg_luminosity, host_orbit_au);
            let kind = Self::classify_moon_kind(rng, equilibrium_temp);

            let (mut radius_earth, mut mass_earth) =
                Self::sample_planet_size_and_mass(rng, kind, true);
            radius_earth = radius_earth.min(host.radius_earth * 0.82).max(0.03);
            mass_earth = mass_earth.min(host.mass_earth * 0.45).max(0.005);

            let habitable = kind == PlanetKind::EarthLikeWorld
                || (kind == PlanetKind::Rocky
                && equilibrium_temp >= 240.0
                && equilibrium_temp <= 330.0
                && radius_earth >= 0.35
                && radius_earth <= 1.6);
            let composition = Self::sample_planet_composition(
                kind,
                equilibrium_temp,
                radius_earth,
                mass_earth,
                host_orbit_au,
                Some(host_index as u8),
            );
            let (atmosphere, atmosphere_pressure_atm) = Self::sample_planet_atmosphere(
                kind,
                equilibrium_temp,
                radius_earth,
                mass_earth,
                host_orbit_au,
                Some(host_index as u8),
            );

            planets.push(PlanetBody {
                kind,
                orbit_au: host_orbit_au,
                host_planet_index: Some(host_index as u8),
                moon_orbit_au: Some(moon_orbit_au),
                radius_earth,
                mass_earth,
                temperature_k: equilibrium_temp,
                habitable,
                composition,
                atmosphere,
                atmosphere_pressure_atm,
            });

            remaining -= 1;
        }

        planets
    }

    pub fn generate_sector(&self, coord: SectorCoord) -> Vec<SystemSummary> {
        let total_target = self.sector_target_total_systems(coord);
        if total_target == 0 {
            return Vec::new();
        }

        let (x0, x1, y0, y1) = self.sector_bounds(coord);
        let z_span = (self.cfg.z_max - self.cfg.z_min).max(1.0);
        let disk_sigma = (z_span * 0.08).max(1.0);
        let z_distribution = Normal::new(self.cfg.center[2], disk_sigma)
            .expect("z normal distribution parameters are valid");
        let sector_area = (self.cfg.sector_size * self.cfg.sector_size).max(1.0);
        let cell_x_min = (x0 / SPAWN_CELL_SIZE).floor() as i32;
        let cell_x_max = ((x1 - f32::EPSILON) / SPAWN_CELL_SIZE).floor() as i32;
        let cell_y_min = (y0 / SPAWN_CELL_SIZE).floor() as i32;
        let cell_y_max = ((y1 - f32::EPSILON) / SPAWN_CELL_SIZE).floor() as i32;

        let mut systems = Vec::new();

        for cell_x in cell_x_min..=cell_x_max {
            for cell_y in cell_y_min..=cell_y_max {
                let cell = SpawnCellCoord {
                    x: cell_x,
                    y: cell_y,
                };
                let (cell_x0, cell_x1, cell_y0, cell_y1) = self.spawn_cell_bounds(cell);
                if cell_x1 <= x0 || cell_x0 >= x1 || cell_y1 <= y0 || cell_y0 >= y1 {
                    continue;
                }

                let cell_center_x = (cell_x0 + cell_x1) * 0.5;
                let cell_center_y = (cell_y0 + cell_y1) * 0.5;
                let cell_density = self.sector_density_factor(cell_center_x, cell_center_y);
                if cell_density <= 0.0 {
                    continue;
                }

                let cell_area = (cell_x1 - cell_x0) * (cell_y1 - cell_y0);
                let cell_target = (self.cfg.base_sector_density
                    * self.density_scale
                    * cell_density
                    * (cell_area / sector_area))
                    .max(0.0)
                    .round() as u64;
                if cell_target == 0 {
                    continue;
                }

                let materialized = cell_target
                    .div_ceil(TARGET_REPRESENTED_SYSTEMS_PER_POINT)
                    .max(1) as usize;
                let represented_base = cell_target / materialized as u64;
                let represented_remainder = (cell_target % materialized as u64) as usize;
                let mut rng = StdRng::seed_from_u64(self.spawn_cell_seed(cell));

                for point_index in 0..materialized {
                    let mut sampled_x = cell_x0;
                    let mut sampled_y = cell_y0;
                    let mut best_keep_probability = -1.0f32;
                    let mut accepted = false;
                    for _ in 0..POSITION_REJECTION_TRIES {
                        let x = rng.gen_range(cell_x0..cell_x1);
                        let y = rng.gen_range(cell_y0..cell_y1);
                        let keep_probability = self.position_density_weight(x, y);

                        if keep_probability > best_keep_probability {
                            best_keep_probability = keep_probability;
                            sampled_x = x;
                            sampled_y = y;
                        }

                        if rng.r#gen::<f32>() <= keep_probability {
                            sampled_x = x;
                            sampled_y = y;
                            accepted = true;
                            break;
                        }
                    }

                    if !accepted && best_keep_probability <= 0.0 {
                        let (fallback_x, fallback_y, fallback_keep_probability) =
                            self.best_sector_position_sample(cell_x0, cell_x1, cell_y0, cell_y1);
                        sampled_x = fallback_x;
                        sampled_y = fallback_y;
                        best_keep_probability = fallback_keep_probability;
                    }

                    if !accepted && best_keep_probability <= 0.0 {
                        continue;
                    }

                    if sampled_x < x0 || sampled_x >= x1 || sampled_y < y0 || sampled_y >= y1 {
                        continue;
                    }

                    let mut z = z_distribution.sample(&mut rng);
                    if z < self.cfg.z_min {
                        z = self.cfg.z_min;
                    }
                    if z > self.cfg.z_max {
                        z = self.cfg.z_max;
                    }

                    let id = SystemId {
                        sector: coord,
                        local_index: systems.len() as u32,
                    };
                    let represented = represented_base
                        + usize::from(point_index < represented_remainder) as u64;
                    let star_seed = self.system_seed(id);

                    systems.push(SystemSummary {
                        id,
                        pos: [sampled_x, sampled_y, z],
                        represented_systems: represented.min(u32::MAX as u64) as u32,
                        primary_star: Self::sample_stellar_class_from_seed(star_seed),
                    });
                }
            }
        }

        systems
    }

    pub fn find_system_summary(&self, id: SystemId) -> Option<SystemSummary> {
        let sector = self.generate_sector(id.sector);
        sector.get(id.local_index as usize).cloned()
    }

    /// Search for the nearest system to `pos` within the given sector and its
    /// eight neighbours.  Returns the best match if one is found within
    /// `max_distance`.
    pub fn find_nearest_system_by_pos(
        &self,
        sector: SectorCoord,
        pos: [f32; 3],
        max_distance: f32,
    ) -> Option<SystemSummary> {
        let max_dist_sq = max_distance * max_distance;
        let mut best: Option<(f32, SystemSummary)> = None;

        for dx in -1i32..=1 {
            for dy in -1i32..=1 {
                let coord = SectorCoord {
                    x: sector.x + dx,
                    y: sector.y + dy,
                };
                let systems = self.generate_sector(coord);
                for sys in &systems {
                    let d = (sys.pos[0] - pos[0]).powi(2)
                        + (sys.pos[1] - pos[1]).powi(2)
                        + (sys.pos[2] - pos[2]).powi(2);
                    if d <= max_dist_sq && best.as_ref().map_or(true, |(bd, _)| d < *bd) {
                        best = Some((d, *sys));
                    }
                }
            }
        }

        best.map(|(_, sys)| sys)
    }

    pub fn generate_system_detail(&self, summary: &SystemSummary) -> SystemDetail {
        let seed = self.system_seed(summary.id);
        let mut rng = StdRng::seed_from_u64(seed);
        let is_black_hole_system = summary.primary_star.spectral == SpectralClass::BH;

        let star_count = if is_black_hole_system {
            1
        } else {
            match rng.r#gen::<f32>() {
                roll if roll < 0.73 => 1,
                roll if roll < 0.95 => 2,
                _ => 3,
            }
        };

        let mut stars = Vec::with_capacity(star_count);
        for idx in 0..star_count {
            let class = if idx == 0 {
                summary.primary_star
            } else {
                Self::sample_stellar_class(&mut rng)
            };
            stars.push(Self::sample_star_body(&mut rng, class));
        }

        let planets = if is_black_hole_system {
            Vec::new()
        } else {
            Self::sample_planets(&mut rng, &stars)
        };
        let canonical_name = format!(
            "SYS-{}-{}-{:05}",
            summary.id.sector.x, summary.id.sector.y, summary.id.local_index
        );

        SystemDetail {
            id: summary.id,
            canonical_name: canonical_name.clone(),
            display_name: canonical_name,
            pos: summary.pos,
            represented_systems: summary.represented_systems.max(1),
            stars,
            planets,
            explored: false,
            favorite: false,
            note: None,
        }
    }
}

pub struct SectorLruCache {
    capacity: usize,
    entries: HashMap<SectorCoord, Arc<Vec<SystemSummary>>>,
    access_gen: HashMap<SectorCoord, u64>,
    generation: u64,
    hits: u64,
    misses: u64,
}

impl SectorLruCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: HashMap::new(),
            access_gen: HashMap::new(),
            generation: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn touch(&mut self, coord: SectorCoord) {
        self.generation += 1;
        self.access_gen.insert(coord, self.generation);
    }

    fn evict_if_needed(&mut self) {
        while self.entries.len() > self.capacity {
            if let Some((&oldest_coord, _)) = self.access_gen.iter().min_by_key(|&(_, &g)| g) {
                self.entries.remove(&oldest_coord);
                self.access_gen.remove(&oldest_coord);
            } else {
                break;
            }
        }
    }

    pub fn get(&mut self, coord: SectorCoord) -> Option<Arc<Vec<SystemSummary>>> {
        let value = self.entries.get(&coord).cloned();
        if value.is_some() {
            self.hits = self.hits.saturating_add(1);
            self.touch(coord);
        } else {
            self.misses = self.misses.saturating_add(1);
        }
        value
    }

    pub fn contains(&self, coord: SectorCoord) -> bool {
        self.entries.contains_key(&coord)
    }

    pub fn insert(&mut self, coord: SectorCoord, systems: Vec<SystemSummary>) {
        self.entries.insert(coord, Arc::new(systems));
        self.touch(coord);
        self.evict_if_needed();
    }

    pub fn get_or_generate(
        &mut self,
        generator: &GalaxyGenerator,
        coord: SectorCoord,
    ) -> Arc<Vec<SystemSummary>> {
        if let Some(cached) = self.get(coord) {
            return cached;
        }

        let generated = generator.generate_sector(coord);
        let shared = Arc::new(generated);
        self.entries.insert(coord, Arc::clone(&shared));
        self.touch(coord);
        self.evict_if_needed();
        shared
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn hit_count(&self) -> u64 {
        self.hits
    }

    pub fn miss_count(&self) -> u64 {
        self.misses
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SystemDelta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rename_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explored: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub favorite: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl SystemDelta {
    pub fn is_noop(&self) -> bool {
        self.rename_to.as_deref().map(str::trim).unwrap_or_default().is_empty()
            && self.explored.is_none()
            && self.favorite.is_none()
            && self.note.as_deref().map(str::trim).unwrap_or_default().is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DeltaRecord {
    system: SystemId,
    delta: SystemDelta,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DeltaFile {
    version: u32,
    deltas: Vec<DeltaRecord>,
}

pub struct DeltaStore {
    deltas: HashMap<SystemId, SystemDelta>,
    dirty: bool,
}

impl Default for DeltaStore {
    fn default() -> Self {
        Self {
            deltas: HashMap::new(),
            dirty: false,
        }
    }
}

impl DeltaStore {
    pub fn load_json(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(err) => return Err(err),
        };

        let parsed: DeltaFile = serde_json::from_str(&content).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse delta store JSON: {err}"),
            )
        })?;

        let mut deltas = HashMap::with_capacity(parsed.deltas.len());
        for record in parsed.deltas {
            if !record.delta.is_noop() {
                deltas.insert(record.system, record.delta);
            }
        }

        Ok(Self {
            deltas,
            dirty: false,
        })
    }

    pub fn save_json(&mut self, path: impl AsRef<Path>) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }

        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut records = self
            .deltas
            .iter()
            .map(|(system, delta)| DeltaRecord {
                system: *system,
                delta: delta.clone(),
            })
            .collect::<Vec<_>>();
        records.sort_by_key(|record| record.system);

        let file = DeltaFile {
            version: 1,
            deltas: records,
        };

        let json = serde_json::to_string_pretty(&file).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to serialize delta store JSON: {err}"),
            )
        })?;

        fs::write(path, json)?;
        self.dirty = false;
        Ok(())
    }

    pub fn upsert(&mut self, system: SystemId, mut delta: SystemDelta) {
        if let Some(rename) = &delta.rename_to {
            let trimmed = rename.trim();
            delta.rename_to = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            };
        }
        if let Some(note) = &delta.note {
            let trimmed = note.trim();
            delta.note = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            };
        }

        if delta.is_noop() {
            self.remove(system);
            return;
        }

        self.deltas.insert(system, delta);
        self.dirty = true;
    }

    pub fn remove(&mut self, system: SystemId) {
        if self.deltas.remove(&system).is_some() {
            self.dirty = true;
        }
    }

    pub fn apply_to_detail(&self, detail: &mut SystemDetail) {
        if let Some(delta) = self.deltas.get(&detail.id) {
            detail.apply_delta(delta);
        }
    }

    pub fn get(&self, system: SystemId) -> Option<&SystemDelta> {
        self.deltas.get(&system)
    }

    pub fn len(&self) -> usize {
        self.deltas.len()
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Return all system IDs that have been marked as favorited.
    pub fn favorited_system_ids(&self) -> Vec<SystemId> {
        self.deltas
            .iter()
            .filter(|(_, delta)| delta.favorite == Some(true))
            .map(|(id, _)| *id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sector_generation_is_deterministic() {
        let generator = GalaxyGenerator::new(GeneratorConfig {
            target_system_count: 30_000_000,
            min_materialized_per_sector: 64,
            max_materialized_per_sector: 512,
            ..Default::default()
        });

        let coord = SectorCoord { x: 12, y: -9 };
        let a = generator.generate_sector(coord);
        let b = generator.generate_sector(coord);

        assert_eq!(a, b);
        assert!(!a.is_empty());
    }

    #[test]
    fn system_detail_is_deterministic() {
        let generator = GalaxyGenerator::new(GeneratorConfig {
            target_system_count: 20_000_000,
            min_materialized_per_sector: 48,
            max_materialized_per_sector: 256,
            ..Default::default()
        });

        let coord = SectorCoord { x: 3, y: 4 };
        let summary = generator
            .generate_sector(coord)
            .into_iter()
            .next()
            .expect("sector should contain at least one system");

        let a = generator.generate_system_detail(&summary);
        let b = generator.generate_system_detail(&summary);

        assert_eq!(a, b);
    }

    #[test]
    fn spectral_roll_includes_neutron_stars() {
        assert_eq!(
            GalaxyGenerator::sample_spectral_class_from_roll(0.0015),
            SpectralClass::NS
        );
    }

    #[test]
    fn neutron_star_notation_and_ranges() {
        let class = StellarClassification::new(SpectralClass::NS, 6, LuminosityClass::VII);
        assert_eq!(class.notation(), "NS");

        let mut rng = StdRng::seed_from_u64(0x44BE_A1EF_4D60_B1AE);
        for _ in 0..1024 {
            let body = GalaxyGenerator::sample_star_body(&mut rng, class);
            assert!(body.mass_solar >= 1.1);
            assert!(body.mass_solar < 2.3);
            assert!(body.luminosity_solar >= 0.000_000_1);
            assert!(body.luminosity_solar < 0.05);
        }
    }

    #[test]
    fn hot_bodies_are_never_icy() {
        let mut rng = StdRng::seed_from_u64(0x51C7_A5C2_28FB_107D);
        for _ in 0..4_096 {
            for equilibrium_temp in [260.0, 350.0, 1_000.0, 2_000.0] {
                let primary = GalaxyGenerator::classify_primary_kind(&mut rng, equilibrium_temp);
                let moon = GalaxyGenerator::classify_moon_kind(&mut rng, equilibrium_temp);
                assert_ne!(primary, PlanetKind::Icy);
                assert_ne!(primary, PlanetKind::RockyIceWorld);
                assert_ne!(moon, PlanetKind::Icy);
                assert_ne!(moon, PlanetKind::RockyIceWorld);
            }
        }
    }

    #[test]
    fn planet_kind_labels_cover_new_variants() {
        assert_eq!(PlanetKind::RockyIceWorld.label(), "Rocky ice world");
        assert_eq!(PlanetKind::WaterWorld.label(), "Water world");
        assert_eq!(PlanetKind::AmmoniaWorld.label(), "Ammonia world");
        assert_eq!(PlanetKind::GasGiantClassI.label(), "Class I gas giant");
        assert_eq!(PlanetKind::GasGiantClassV.label(), "Class V gas giant");
        assert_eq!(
            PlanetKind::GasGiantAmmoniaLife.label(),
            "Gas giant with ammonia-based life"
        );
        assert_eq!(
            PlanetKind::GasGiantWaterLife.label(),
            "Gas giant with water-based life"
        );
        assert_eq!(PlanetKind::WaterGiant.label(), "Water giant");
        assert_eq!(PlanetKind::MetalRich.label(), "Metal-rich world");
        assert_eq!(PlanetKind::Metal.label(), "Metal world");
    }

    #[test]
    fn generated_systems_do_not_emit_hot_icy_bodies() {
        let generator = GalaxyGenerator::new(GeneratorConfig {
            galaxy_seed: 0x7F31_66B0_3DE7_C4A9,
            target_system_count: 25_000_000,
            min_materialized_per_sector: 80,
            max_materialized_per_sector: 512,
            ..Default::default()
        });

        let coord = SectorCoord { x: 9, y: -3 };
        let summaries = generator.generate_sector(coord);
        assert!(!summaries.is_empty());

        for summary in summaries.iter().take(160) {
            let detail = generator.generate_system_detail(summary);
            for planet in detail.planets {
                if planet.temperature_k >= 260.0 {
                    assert_ne!(planet.kind, PlanetKind::Icy);
                    assert_ne!(planet.kind, PlanetKind::RockyIceWorld);
                }
            }
        }
    }

    #[test]
    fn generated_planets_have_normalized_compositions() {
        let generator = GalaxyGenerator::new(GeneratorConfig {
            galaxy_seed: 0x41A5_5B7D_92E4_1103,
            target_system_count: 40_000_000,
            min_materialized_per_sector: 96,
            max_materialized_per_sector: 512,
            ..Default::default()
        });

        let summaries = generator.generate_sector(SectorCoord { x: -8, y: 11 });
        assert!(!summaries.is_empty());

        let mut atmosphere_count = 0usize;

        for summary in summaries.iter().take(80) {
            let detail = generator.generate_system_detail(summary);
            for planet in &detail.planets {
                assert!(
                    !planet.composition.is_empty(),
                    "planet composition should not be empty"
                );

                let percent_sum = planet
                    .composition
                    .iter()
                    .map(|entry| entry.percent)
                    .sum::<f32>();

                assert!(
                    (percent_sum - 100.0).abs() <= 0.25,
                    "composition percentages should sum to ~100, got {percent_sum}"
                );

                for component in &planet.composition {
                    assert!(component.atomic_number > 0);
                    assert!(!component.symbol.is_empty());
                    assert!(!component.name.is_empty());
                    assert!(component.percent > 0.0);
                }

                if planet.atmosphere.is_empty() {
                    assert!(planet.atmosphere_pressure_atm <= 0.001);
                } else {
                    atmosphere_count += 1;
                    let atmosphere_sum = planet
                        .atmosphere
                        .iter()
                        .map(|gas| gas.percent)
                        .sum::<f32>();
                    assert!(
                        (atmosphere_sum - 100.0).abs() <= 0.35,
                        "atmosphere percentages should sum to ~100, got {atmosphere_sum}"
                    );
                    assert!(planet.atmosphere_pressure_atm > 0.0);
                    for gas in &planet.atmosphere {
                        assert!(!gas.formula.is_empty());
                        assert!(!gas.name.is_empty());
                        assert!(gas.percent > 0.0);
                    }
                }
            }
        }

        assert!(
            atmosphere_count > 0,
            "at least one sampled planet should retain an atmosphere"
        );
    }

    #[test]
    fn delta_store_round_trip() {
        let temp_path = std::env::temp_dir().join(format!(
            "galaxy_delta_store_{}.json",
            mix64(0xA24B_AED4_963E_E407 ^ 42)
        ));

        let system = SystemId {
            sector: SectorCoord { x: -2, y: 5 },
            local_index: 17,
        };

        let mut store = DeltaStore::default();
        store.upsert(
            system,
            SystemDelta {
                rename_to: Some("Sol Prime".to_owned()),
                explored: Some(true),
                favorite: Some(true),
                note: Some("High metallicity worlds".to_owned()),
            },
        );
        assert!(store.is_dirty());
        store
            .save_json(&temp_path)
            .expect("delta store should save to JSON");
        assert!(!store.is_dirty());

        let loaded = DeltaStore::load_json(&temp_path).expect("delta store should load");
        let loaded_delta = loaded
            .get(system)
            .expect("saved delta should exist after reload");

        assert_eq!(loaded_delta.rename_to.as_deref(), Some("Sol Prime"));
        assert_eq!(loaded_delta.explored, Some(true));
        assert_eq!(loaded_delta.favorite, Some(true));
        assert_eq!(loaded_delta.note.as_deref(), Some("High metallicity worlds"));

        let _ = fs::remove_file(temp_path);
    }
}
