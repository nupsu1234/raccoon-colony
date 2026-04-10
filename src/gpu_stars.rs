use bytemuck::{Pod, Zeroable};
use eframe::egui;
use eframe::egui_wgpu;
use eframe::egui_wgpu::wgpu;
use eframe::egui_wgpu::wgpu::util::DeviceExt;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

const INITIAL_INSTANCE_CAPACITY: usize = 128 * 1024;
const STALE_CHUNK_FRAMES: u64 = 180;
const CULL_WORKGROUP_SIZE: u32 = 256;
const TIMESTAMP_RING_SIZE: usize = 4;

const MAP_IDLE: u8 = 0;
const MAP_PENDING: u8 = 1;
const MAP_READY: u8 = 2;
const MAP_ERROR: u8 = 3;

#[derive(Clone, Copy, Debug, Default)]
pub struct GpuTimingSnapshot {
    pub timestamp_supported: bool,
    pub render_timestamp_supported: bool,
    pub cull_ms: Option<f32>,
    pub render_ms: Option<f32>,
    pub total_ms: Option<f32>,
}

#[derive(Clone, Debug, Default)]
pub struct GpuRuntimeSnapshot {
    pub renderer_initialized: bool,
    pub adapter_name: String,
    pub backend: String,
    pub device_type: String,
    pub driver: String,
    pub driver_info: String,
    pub timestamp_feature_enabled: bool,
    pub timestamp_inside_pass_feature_enabled: bool,
    pub likely_software_adapter: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GpuWorkSnapshot {
    pub prepared_frames: u64,
    pub resident_instances: u32,
    pub max_visible_count: u32,
    pub cull_dispatch_groups: u32,
    pub keep_prob: f32,
}

static GPU_TIMING_SNAPSHOT: OnceLock<Mutex<GpuTimingSnapshot>> = OnceLock::new();
static GPU_RUNTIME_SNAPSHOT: OnceLock<Mutex<GpuRuntimeSnapshot>> = OnceLock::new();
static GPU_WORK_SNAPSHOT: OnceLock<Mutex<GpuWorkSnapshot>> = OnceLock::new();

fn timing_snapshot_cell() -> &'static Mutex<GpuTimingSnapshot> {
    GPU_TIMING_SNAPSHOT.get_or_init(|| Mutex::new(GpuTimingSnapshot::default()))
}

fn runtime_snapshot_cell() -> &'static Mutex<GpuRuntimeSnapshot> {
    GPU_RUNTIME_SNAPSHOT.get_or_init(|| Mutex::new(GpuRuntimeSnapshot::default()))
}

fn work_snapshot_cell() -> &'static Mutex<GpuWorkSnapshot> {
    GPU_WORK_SNAPSHOT.get_or_init(|| Mutex::new(GpuWorkSnapshot::default()))
}

fn publish_timing_snapshot(snapshot: GpuTimingSnapshot) {
    if let Ok(mut guard) = timing_snapshot_cell().lock() {
        *guard = snapshot;
    }
}

fn publish_runtime_snapshot(snapshot: GpuRuntimeSnapshot) {
    if let Ok(mut guard) = runtime_snapshot_cell().lock() {
        *guard = snapshot;
    }
}

fn publish_work_snapshot(snapshot: GpuWorkSnapshot) {
    if let Ok(mut guard) = work_snapshot_cell().lock() {
        *guard = snapshot;
    }
}

pub fn latest_timing_snapshot() -> GpuTimingSnapshot {
    timing_snapshot_cell()
        .lock()
        .map(|guard| *guard)
        .unwrap_or_default()
}

pub fn latest_runtime_snapshot() -> GpuRuntimeSnapshot {
    runtime_snapshot_cell()
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default()
}

pub fn latest_work_snapshot() -> GpuWorkSnapshot {
    work_snapshot_cell()
        .lock()
        .map(|guard| *guard)
        .unwrap_or_default()
}

struct TimestampSlot {
    readback_buffer: wgpu::Buffer,
    map_status: Arc<AtomicU8>,
    map_pending: bool,
}

struct GpuTimestampState {
    query_set: wgpu::QuerySet,
    resolve_buffer: wgpu::Buffer,
    slots: Vec<TimestampSlot>,
    timestamp_period_ns: f32,
    queries_per_slot: u32,
    slot_bytes: u64,
    slot_stride_bytes: u64,
    render_timestamp_supported: bool,
    smoothed_cull_ms: Option<f32>,
    smoothed_render_ms: Option<f32>,
    smoothed_total_ms: Option<f32>,
}

impl GpuTimestampState {
    fn new(device: &wgpu::Device, queue: &wgpu::Queue) -> Option<Self> {
        let features = device.features();
        if !features.contains(wgpu::Features::TIMESTAMP_QUERY) {
            publish_timing_snapshot(GpuTimingSnapshot {
                timestamp_supported: false,
                render_timestamp_supported: false,
                cull_ms: None,
                render_ms: None,
                total_ms: None,
            });
            return None;
        }

        let render_timestamp_supported =
            features.contains(wgpu::Features::TIMESTAMP_QUERY_INSIDE_PASSES);
        let queries_per_slot = if render_timestamp_supported { 4 } else { 2 };
        let query_count = queries_per_slot * TIMESTAMP_RING_SIZE as u32;
        let slot_bytes = queries_per_slot as u64 * std::mem::size_of::<u64>() as u64;
        let resolve_alignment = wgpu::QUERY_RESOLVE_BUFFER_ALIGNMENT;
        let slot_stride_bytes = ((slot_bytes + resolve_alignment - 1) / resolve_alignment)
            * resolve_alignment;

        let query_set = device.create_query_set(&wgpu::QuerySetDescriptor {
            label: Some("galaxy_timing_query_set"),
            ty: wgpu::QueryType::Timestamp,
            count: query_count,
        });

        let resolve_buffer = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("galaxy_timing_resolve_buffer"),
            size: slot_stride_bytes * TIMESTAMP_RING_SIZE as u64,
            usage: wgpu::BufferUsages::QUERY_RESOLVE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });

        let mut slots = Vec::with_capacity(TIMESTAMP_RING_SIZE);
        for slot_idx in 0..TIMESTAMP_RING_SIZE {
            let readback_buffer = device.create_buffer(&wgpu::BufferDescriptor {
                label: Some(&format!("galaxy_timing_readback_slot_{slot_idx}")),
                size: slot_bytes,
                usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
                mapped_at_creation: false,
            });
            slots.push(TimestampSlot {
                readback_buffer,
                map_status: Arc::new(AtomicU8::new(MAP_IDLE)),
                map_pending: false,
            });
        }

        publish_timing_snapshot(GpuTimingSnapshot {
            timestamp_supported: true,
            render_timestamp_supported,
            cull_ms: None,
            render_ms: None,
            total_ms: None,
        });

        Some(Self {
            query_set,
            resolve_buffer,
            slots,
            timestamp_period_ns: queue.get_timestamp_period(),
            queries_per_slot,
            slot_bytes,
            slot_stride_bytes,
            render_timestamp_supported,
            smoothed_cull_ms: None,
            smoothed_render_ms: None,
            smoothed_total_ms: None,
        })
    }

    fn query_base_for_frame(&self, frame_index: u64) -> u32 {
        let slot = (frame_index as usize) % TIMESTAMP_RING_SIZE;
        slot as u32 * self.queries_per_slot
    }

    fn slot_index_for_frame(&self, frame_index: u64) -> usize {
        (frame_index as usize) % TIMESTAMP_RING_SIZE
    }

    fn write_compute_begin(&self, encoder: &mut wgpu::CommandEncoder, frame_index: u64) {
        let base = self.query_base_for_frame(frame_index);
        encoder.write_timestamp(&self.query_set, base);
    }

    fn write_compute_end(&self, encoder: &mut wgpu::CommandEncoder, frame_index: u64) {
        let base = self.query_base_for_frame(frame_index);
        encoder.write_timestamp(&self.query_set, base + 1);
    }

    fn write_render_begin<'a>(&self, render_pass: &mut wgpu::RenderPass<'a>, frame_index: u64) {
        if !self.render_timestamp_supported {
            return;
        }
        let base = self.query_base_for_frame(frame_index);
        render_pass.write_timestamp(&self.query_set, base + 2);
    }

    fn write_render_end<'a>(&self, render_pass: &mut wgpu::RenderPass<'a>, frame_index: u64) {
        if !self.render_timestamp_supported {
            return;
        }
        let base = self.query_base_for_frame(frame_index);
        render_pass.write_timestamp(&self.query_set, base + 3);
    }

    fn resolve_previous_frame(
        &mut self,
        encoder: &mut wgpu::CommandEncoder,
        frame_index: u64,
    ) {
        if frame_index <= 1 {
            return;
        }

        let previous_frame = frame_index - 1;
        let base = self.query_base_for_frame(previous_frame);
        let slot_index = self.slot_index_for_frame(previous_frame);
        let slot = &mut self.slots[slot_index];
        if slot.map_pending {
            return;
        }

        let resolve_offset = slot_index as u64 * self.slot_stride_bytes;
        encoder.resolve_query_set(
            &self.query_set,
            base..(base + self.queries_per_slot),
            &self.resolve_buffer,
            resolve_offset,
        );
        encoder.copy_buffer_to_buffer(
            &self.resolve_buffer,
            resolve_offset,
            &slot.readback_buffer,
            0,
            self.slot_bytes,
        );

        // Map/read back on the next frame so this slot is never mapped while
        // command buffers that write to it are being submitted.
        slot.map_pending = true;
    }

    fn poll_completed_maps(&mut self, device: &wgpu::Device) {
        // Non-blocking poll — never stall the CPU waiting for the GPU.
        device.poll(wgpu::Maintain::Poll);

        let mut completed = Vec::new();

        for slot in &mut self.slots {
            if !slot.map_pending {
                continue;
            }

            match slot.map_status.load(Ordering::Acquire) {
                MAP_IDLE => {
                    // The resolve+copy commands were submitted on a previous
                    // frame's command buffer.  Issue the async map request now.
                    let status = Arc::clone(&slot.map_status);
                    status.store(MAP_PENDING, Ordering::Release);
                    slot.readback_buffer
                        .slice(..)
                        .map_async(wgpu::MapMode::Read, move |result| {
                            if result.is_ok() {
                                status.store(MAP_READY, Ordering::Release);
                            } else {
                                status.store(MAP_ERROR, Ordering::Release);
                            }
                        });
                    // Do NOT poll(Wait) — the map completes asynchronously and
                    // we will pick up the result on a future frame.
                }
                MAP_READY => {
                    let mapped = slot.readback_buffer.slice(..).get_mapped_range();
                    let timestamps = mapped
                        .chunks_exact(std::mem::size_of::<u64>())
                        .map(|chunk| {
                            let mut bytes = [0u8; 8];
                            bytes.copy_from_slice(chunk);
                            u64::from_le_bytes(bytes)
                        })
                        .collect::<Vec<_>>();
                    drop(mapped);
                    slot.readback_buffer.unmap();
                    completed.push(timestamps);
                    slot.map_pending = false;
                    slot.map_status.store(MAP_IDLE, Ordering::Release);
                }
                MAP_ERROR => {
                    slot.readback_buffer.unmap();
                    slot.map_pending = false;
                    slot.map_status.store(MAP_IDLE, Ordering::Release);
                }
                _ => {
                    // MAP_PENDING — async map still in flight; check next frame.
                }
            }
        }

        for timestamps in completed {
            self.consume_timestamps(&timestamps);
        }
    }

    fn consume_timestamps(&mut self, timestamps: &[u64]) {
        if timestamps.len() < 2 {
            return;
        }

        let cull_ms = timestamps[1]
            .checked_sub(timestamps[0])
            .map(|ticks| ticks as f32 * self.timestamp_period_ns / 1_000_000.0);

        let render_ms = if self.render_timestamp_supported && timestamps.len() >= 4 {
            timestamps[3]
                .checked_sub(timestamps[2])
                .map(|ticks| ticks as f32 * self.timestamp_period_ns / 1_000_000.0)
        } else {
            None
        };

        let total_ms = if self.render_timestamp_supported && timestamps.len() >= 4 {
            timestamps[3]
                .checked_sub(timestamps[0])
                .map(|ticks| ticks as f32 * self.timestamp_period_ns / 1_000_000.0)
        } else {
            cull_ms
        };

        self.smoothed_cull_ms = cull_ms.map(|sample| smooth_option_ms(self.smoothed_cull_ms, sample));
        self.smoothed_render_ms = render_ms.map(|sample| smooth_option_ms(self.smoothed_render_ms, sample));
        self.smoothed_total_ms = total_ms.map(|sample| smooth_option_ms(self.smoothed_total_ms, sample));

        publish_timing_snapshot(GpuTimingSnapshot {
            timestamp_supported: true,
            render_timestamp_supported: self.render_timestamp_supported,
            cull_ms: self.smoothed_cull_ms,
            render_ms: self.smoothed_render_ms,
            total_ms: self.smoothed_total_ms,
        });
    }
}

fn smooth_option_ms(previous: Option<f32>, sample: f32) -> f32 {
    match previous {
        Some(previous) => previous * 0.8 + sample * 0.2,
        None => sample,
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StarPoint {
    pub pos: [f32; 3],
    pub represented_systems: u32,
}

#[derive(Debug)]
pub struct ChunkUpload {
    pub key: u64,
    pub points: Vec<StarPoint>,
}

#[derive(Clone, Copy, Debug)]
pub struct FrameUniformInput {
    pub center3d: [f32; 3],
    pub pan: egui::Vec2,
    pub zoom: f32,
    pub yaw: f32,
    pub pitch: f32,
    pub canvas_size: egui::Vec2,
    pub black_hole_local: egui::Vec2,
    pub black_hole_cull_radius: f32,
    pub star_draw_radius: f32,
    pub max_point_radius: f32,
    pub star_alpha: f32,
    pub star_alpha_min: f32,
    pub star_alpha_max: f32,
    pub star_color_rgb: [f32; 3],
    pub max_visible_count: u32,
    pub density_keep: f32,
    pub random_seed: u32,
}

#[derive(Clone, Copy, Debug)]
struct CullSettings {
    max_visible_count: u32,
    density_keep: f32,
    random_seed: u32,
}

impl From<FrameUniformInput> for CullSettings {
    fn from(input: FrameUniformInput) -> Self {
        Self {
            max_visible_count: input.max_visible_count,
            density_keep: input.density_keep,
            random_seed: input.random_seed,
        }
    }
}

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct QuadVertex {
    xy: [f32; 2],
}

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct StarInstance {
    pos_repr: [f32; 4],
}

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct StarUniform {
    center3d: [f32; 4],
    pan_zoom: [f32; 4],
    yaw_sin_cos: [f32; 4],
    canvas_size_center: [f32; 4],
    black_hole: [f32; 4],
    star_shape: [f32; 4],
    alpha_range: [f32; 4],
    star_color: [f32; 4],
}

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct CullParams {
    total_star_count: u32,
    max_visible_count: u32,
    random_seed: u32,
    _padding0: u32,
    keep_prob_pad: [f32; 4],
}

impl From<FrameUniformInput> for StarUniform {
    fn from(input: FrameUniformInput) -> Self {
        let (sin_yaw, cos_yaw) = input.yaw.sin_cos();
        let (sin_pitch, cos_pitch) = input.pitch.sin_cos();
        Self {
            center3d: [input.center3d[0], input.center3d[1], input.center3d[2], 0.0],
            pan_zoom: [input.pan.x, input.pan.y, input.zoom, 0.0],
            yaw_sin_cos: [sin_yaw, cos_yaw, sin_pitch, cos_pitch],
            canvas_size_center: [
                input.canvas_size.x.max(1.0),
                input.canvas_size.y.max(1.0),
                input.canvas_size.x * 0.5,
                input.canvas_size.y * 0.5,
            ],
            black_hole: [
                input.black_hole_local.x,
                input.black_hole_local.y,
                input.black_hole_cull_radius.max(0.0),
                0.0,
            ],
            star_shape: [
                input.star_draw_radius.max(0.01),
                input.max_point_radius.max(input.star_draw_radius),
                input.star_alpha,
                0.0,
            ],
            alpha_range: [input.star_alpha_min, input.star_alpha_max, 0.0, 0.0],
            star_color: [
                input.star_color_rgb[0],
                input.star_color_rgb[1],
                input.star_color_rgb[2],
                1.0,
            ],
        }
    }
}

struct StarRenderResources {
    render_pipeline: wgpu::RenderPipeline,
    cull_pipeline: wgpu::ComputePipeline,
    finalize_pipeline: wgpu::ComputePipeline,
    render_bind_group_layout: wgpu::BindGroupLayout,
    compute_bind_group_layout: wgpu::BindGroupLayout,
    render_bind_group: wgpu::BindGroup,
    compute_bind_group: wgpu::BindGroup,
    uniform_buffer: wgpu::Buffer,
    cull_params_buffer: wgpu::Buffer,
    quad_buffer: wgpu::Buffer,
    instance_buffer: wgpu::Buffer,
    visible_indices_buffer: wgpu::Buffer,
    visible_counter_buffer: wgpu::Buffer,
    draw_indirect_buffer: wgpu::Buffer,
    instance_capacity: usize,
    resident_instance_count: u32,
    resident_chunks: HashMap<u64, ResidentChunk>,
    chunk_cpu_cache: HashMap<u64, Vec<StarInstance>>,
    gpu_timestamps: Option<GpuTimestampState>,
    frame_index: u64,
    last_processed_generation: u64,
}

#[derive(Clone, Copy, Debug)]
struct ResidentChunk {
    offset: u32,
    count: u32,
    fingerprint: u64,
    last_seen_frame: u64,
}

impl StarRenderResources {
    fn new(device: &wgpu::Device, queue: &wgpu::Queue, target_format: wgpu::TextureFormat) -> Self {
        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("galaxy_star_shader"),
            source: wgpu::ShaderSource::Wgsl(include_str!("gpu_stars.wgsl").into()),
        });

        let gpu_timestamps = GpuTimestampState::new(device, queue);

        let uniform_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("galaxy_star_uniform_buffer"),
            contents: bytemuck::bytes_of(&StarUniform::zeroed()),
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        });

        let cull_params_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("galaxy_cull_params_buffer"),
            contents: bytemuck::bytes_of(&CullParams::zeroed()),
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        });

        let render_bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("galaxy_star_render_bind_group_layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::VERTEX_FRAGMENT,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Uniform,
                        has_dynamic_offset: false,
                        min_binding_size: std::num::NonZeroU64::new(std::mem::size_of::<StarUniform>() as u64),
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::VERTEX,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::VERTEX,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

        let compute_bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("galaxy_star_compute_bind_group_layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Uniform,
                        has_dynamic_offset: false,
                        min_binding_size: std::num::NonZeroU64::new(std::mem::size_of::<StarUniform>() as u64),
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 3,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 4,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 5,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Uniform,
                        has_dynamic_offset: false,
                        min_binding_size: std::num::NonZeroU64::new(std::mem::size_of::<CullParams>() as u64),
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 6,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

        let instance_capacity = INITIAL_INSTANCE_CAPACITY;
        let instance_buffer = Self::create_instance_buffer(device, instance_capacity);
        let visible_indices_buffer = Self::create_visible_indices_buffer(device, instance_capacity);
        let visible_counter_buffer = Self::create_visible_counter_buffer(device);
        let draw_indirect_buffer = Self::create_draw_indirect_buffer(device);

        let render_bind_group = Self::create_render_bind_group(
            device,
            &render_bind_group_layout,
            &uniform_buffer,
            &instance_buffer,
            &visible_indices_buffer,
        );

        let compute_bind_group = Self::create_compute_bind_group(
            device,
            &compute_bind_group_layout,
            &uniform_buffer,
            &instance_buffer,
            &visible_indices_buffer,
            &visible_counter_buffer,
            &draw_indirect_buffer,
            &cull_params_buffer,
        );

        let render_pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("galaxy_star_render_pipeline_layout"),
            bind_group_layouts: &[&render_bind_group_layout],
            push_constant_ranges: &[],
        });

        let compute_pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("galaxy_star_compute_pipeline_layout"),
            bind_group_layouts: &[&compute_bind_group_layout],
            push_constant_ranges: &[],
        });

        let render_pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("galaxy_star_pipeline"),
            layout: Some(&render_pipeline_layout),
            vertex: wgpu::VertexState {
                module: &shader,
                entry_point: "vs_main",
                buffers: &[wgpu::VertexBufferLayout {
                    array_stride: std::mem::size_of::<QuadVertex>() as u64,
                    step_mode: wgpu::VertexStepMode::Vertex,
                    attributes: &[wgpu::VertexAttribute {
                        format: wgpu::VertexFormat::Float32x2,
                        offset: 0,
                        shader_location: 0,
                    }],
                }],
            },
            fragment: Some(wgpu::FragmentState {
                module: &shader,
                entry_point: "fs_main",
                targets: &[Some(wgpu::ColorTargetState {
                    format: target_format,
                    blend: Some(wgpu::BlendState {
                        color: wgpu::BlendComponent {
                            src_factor: wgpu::BlendFactor::One,
                            dst_factor: wgpu::BlendFactor::OneMinusSrcAlpha,
                            operation: wgpu::BlendOperation::Add,
                        },
                        alpha: wgpu::BlendComponent {
                            src_factor: wgpu::BlendFactor::One,
                            dst_factor: wgpu::BlendFactor::OneMinusSrcAlpha,
                            operation: wgpu::BlendOperation::Add,
                        },
                    }),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
            }),
            primitive: wgpu::PrimitiveState {
                topology: wgpu::PrimitiveTopology::TriangleList,
                strip_index_format: None,
                front_face: wgpu::FrontFace::Ccw,
                cull_mode: None,
                unclipped_depth: false,
                polygon_mode: wgpu::PolygonMode::Fill,
                conservative: false,
            },
            depth_stencil: None,
            multisample: wgpu::MultisampleState {
                count: 1,
                mask: !0,
                alpha_to_coverage_enabled: false,
            },
            multiview: None,
        });

        let cull_pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("galaxy_star_cull_pipeline"),
            layout: Some(&compute_pipeline_layout),
            module: &shader,
            entry_point: "cs_cull",
        });

        let finalize_pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("galaxy_star_finalize_pipeline"),
            layout: Some(&compute_pipeline_layout),
            module: &shader,
            entry_point: "cs_finalize",
        });

        let quad_vertices = [
            QuadVertex { xy: [-1.0, -1.0] },
            QuadVertex { xy: [1.0, -1.0] },
            QuadVertex { xy: [1.0, 1.0] },
            QuadVertex { xy: [-1.0, -1.0] },
            QuadVertex { xy: [1.0, 1.0] },
            QuadVertex { xy: [-1.0, 1.0] },
        ];

        let quad_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("galaxy_star_quad_buffer"),
            contents: bytemuck::cast_slice(&quad_vertices),
            usage: wgpu::BufferUsages::VERTEX,
        });

        Self {
            render_pipeline,
            cull_pipeline,
            finalize_pipeline,
            render_bind_group_layout,
            compute_bind_group_layout,
            render_bind_group,
            compute_bind_group,
            uniform_buffer,
            cull_params_buffer,
            quad_buffer,
            instance_buffer,
            visible_indices_buffer,
            visible_counter_buffer,
            draw_indirect_buffer,
            instance_capacity,
            resident_instance_count: 0,
            resident_chunks: HashMap::new(),
            chunk_cpu_cache: HashMap::new(),
            gpu_timestamps,
            frame_index: 0,
            last_processed_generation: u64::MAX,
        }
    }

    fn create_instance_buffer(device: &wgpu::Device, instance_capacity: usize) -> wgpu::Buffer {
        device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("galaxy_star_instance_buffer"),
            size: (std::mem::size_of::<StarInstance>() * instance_capacity.max(1)) as u64,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        })
    }

    fn create_visible_indices_buffer(device: &wgpu::Device, instance_capacity: usize) -> wgpu::Buffer {
        device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("galaxy_visible_indices_buffer"),
            size: (std::mem::size_of::<u32>() * instance_capacity.max(1)) as u64,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        })
    }

    fn create_visible_counter_buffer(device: &wgpu::Device) -> wgpu::Buffer {
        device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("galaxy_visible_counter_buffer"),
            contents: bytemuck::cast_slice(&[0u32]),
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
        })
    }

    fn create_draw_indirect_buffer(device: &wgpu::Device) -> wgpu::Buffer {
        device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("galaxy_draw_indirect_buffer"),
            contents: bytemuck::cast_slice(&[6u32, 0, 0, 0]),
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::INDIRECT | wgpu::BufferUsages::COPY_DST,
        })
    }

    fn create_render_bind_group(
        device: &wgpu::Device,
        layout: &wgpu::BindGroupLayout,
        uniform_buffer: &wgpu::Buffer,
        instance_buffer: &wgpu::Buffer,
        visible_indices_buffer: &wgpu::Buffer,
    ) -> wgpu::BindGroup {
        device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("galaxy_star_render_bind_group"),
            layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: uniform_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: instance_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: visible_indices_buffer.as_entire_binding(),
                },
            ],
        })
    }

    fn create_compute_bind_group(
        device: &wgpu::Device,
        layout: &wgpu::BindGroupLayout,
        uniform_buffer: &wgpu::Buffer,
        instance_buffer: &wgpu::Buffer,
        visible_indices_buffer: &wgpu::Buffer,
        visible_counter_buffer: &wgpu::Buffer,
        draw_indirect_buffer: &wgpu::Buffer,
        cull_params_buffer: &wgpu::Buffer,
    ) -> wgpu::BindGroup {
        device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("galaxy_star_compute_bind_group"),
            layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: uniform_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: instance_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: visible_counter_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 4,
                    resource: draw_indirect_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 5,
                    resource: cull_params_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 6,
                    resource: visible_indices_buffer.as_entire_binding(),
                },
            ],
        })
    }

    fn recreate_storage_buffers(&mut self, device: &wgpu::Device, instance_capacity: usize) {
        self.instance_capacity = instance_capacity.max(INITIAL_INSTANCE_CAPACITY);
        self.instance_buffer = Self::create_instance_buffer(device, self.instance_capacity);
        self.visible_indices_buffer = Self::create_visible_indices_buffer(device, self.instance_capacity);
        self.render_bind_group = Self::create_render_bind_group(
            device,
            &self.render_bind_group_layout,
            &self.uniform_buffer,
            &self.instance_buffer,
            &self.visible_indices_buffer,
        );
        self.compute_bind_group = Self::create_compute_bind_group(
            device,
            &self.compute_bind_group_layout,
            &self.uniform_buffer,
            &self.instance_buffer,
            &self.visible_indices_buffer,
            &self.visible_counter_buffer,
            &self.draw_indirect_buffer,
            &self.cull_params_buffer,
        );
    }

    /// Fingerprint the raw upload data without allocating StarInstance storage.
    /// Allows skipping the StarInstance conversion for unchanged chunks.
    fn hash_upload_points(points: &[StarPoint]) -> u64 {
        let mut h = 0x71A6_C2F8_39D1_4E05u64 ^ points.len() as u64;
        for point in points {
            for &val in &point.pos {
                h ^= f32::to_bits(val) as u64;
                h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
                h ^= h >> 27;
            }
            h ^= point.represented_systems as u64;
            h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            h ^= h >> 27;
        }
        h
    }

    fn repack_live_chunks(&mut self, device: &wgpu::Device, queue: &wgpu::Queue) {
        let mut keys = self
            .resident_chunks
            .keys()
            .copied()
            .collect::<Vec<_>>();
        keys.sort_unstable();

        let total_instances = keys
            .iter()
            .filter_map(|key| self.chunk_cpu_cache.get(key))
            .map(Vec::len)
            .sum::<usize>();

        let required_capacity = total_instances.max(INITIAL_INSTANCE_CAPACITY).next_power_of_two();
        if required_capacity != self.instance_capacity {
            self.recreate_storage_buffers(device, required_capacity);
        }

        // Batch all chunk instances into one contiguous Vec and issue a single
        // write_buffer call instead of one per chunk.  This halves the wgpu
        // staging-buffer allocation overhead during LOD transitions.
        let mut packed = Vec::<StarInstance>::with_capacity(total_instances);
        for key in &keys {
            let Some(chunk_instances) = self.chunk_cpu_cache.get(key) else {
                continue;
            };
            let offset = packed.len() as u32;
            let count = chunk_instances.len() as u32;
            packed.extend_from_slice(chunk_instances);

            if let Some(resident) = self.resident_chunks.get_mut(key) {
                resident.offset = offset;
                resident.count = count;
            }
        }

        if !packed.is_empty() {
            queue.write_buffer(
                &self.instance_buffer,
                0,
                bytemuck::cast_slice(&packed),
            );
        }

        self.resident_instance_count = packed.len() as u32;
    }

    fn evict_stale_chunks(&mut self) -> bool {
        let stale_keys = self
            .resident_chunks
            .iter()
            .filter_map(|(key, chunk)| {
                if self.frame_index.saturating_sub(chunk.last_seen_frame) > STALE_CHUNK_FRAMES {
                    Some(*key)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if stale_keys.is_empty() {
            return false;
        }

        for key in stale_keys {
            self.resident_chunks.remove(&key);
            self.chunk_cpu_cache.remove(&key);
        }

        true
    }

    fn prepare(
        &mut self,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        egui_encoder: &mut wgpu::CommandEncoder,
        uniform: &StarUniform,
        chunks: &[ChunkUpload],
        cull_settings: CullSettings,
        uploads_generation: u64,
    ) {
        self.frame_index = self.frame_index.saturating_add(1);

        if let Some(timestamps) = self.gpu_timestamps.as_mut() {
            timestamps.poll_completed_maps(device);
            timestamps.resolve_previous_frame(egui_encoder, self.frame_index);
        }

        queue.write_buffer(&self.uniform_buffer, 0, bytemuck::bytes_of(uniform));

        if uploads_generation != self.last_processed_generation {
            // New chunk data: synchronize GPU residency.
            self.last_processed_generation = uploads_generation;

            let mut needs_repack = false;
            let stride = std::mem::size_of::<StarInstance>() as u64;
            let mut seen_keys = HashSet::with_capacity(chunks.len());

            for upload in chunks {
                seen_keys.insert(upload.key);

                if upload.points.is_empty() {
                    if self.resident_chunks.remove(&upload.key).is_some() {
                        self.chunk_cpu_cache.remove(&upload.key);
                        needs_repack = true;
                    }
                    continue;
                }

                // Fingerprint the raw upload data *before* allocating
                // StarInstance storage.  When data is unchanged (common for
                // stable-LOD view shifts) we skip the conversion entirely.
                let upload_fingerprint = Self::hash_upload_points(&upload.points);

                if let Some(resident) = self.resident_chunks.get_mut(&upload.key) {
                    resident.last_seen_frame = self.frame_index;

                    if resident.count == upload.points.len() as u32
                        && resident.fingerprint == upload_fingerprint
                    {
                        // Data unchanged — skip StarInstance conversion and
                        // GPU upload.
                        continue;
                    }
                }

                // Data changed or new chunk — convert to StarInstance.
                let instances: Vec<StarInstance> = upload
                    .points
                    .iter()
                    .map(|point| StarInstance {
                        pos_repr: [
                            point.pos[0],
                            point.pos[1],
                            point.pos[2],
                            point.represented_systems.max(1) as f32,
                        ],
                    })
                    .collect();

                if let Some(resident) = self.resident_chunks.get_mut(&upload.key) {
                    if resident.count == instances.len() as u32 {
                        // Same count, different data — update in-place.
                        let offset_bytes = resident.offset as u64 * stride;
                        queue.write_buffer(
                            &self.instance_buffer,
                            offset_bytes,
                            bytemuck::cast_slice(&instances),
                        );
                        resident.fingerprint = upload_fingerprint;
                        self.chunk_cpu_cache.insert(upload.key, instances);
                    } else {
                        resident.count = instances.len() as u32;
                        resident.fingerprint = upload_fingerprint;
                        self.chunk_cpu_cache.insert(upload.key, instances);
                        needs_repack = true;
                    }
                } else {
                    self.resident_chunks.insert(
                        upload.key,
                        ResidentChunk {
                            offset: 0,
                            count: instances.len() as u32,
                            fingerprint: upload_fingerprint,
                            last_seen_frame: self.frame_index,
                        },
                    );
                    self.chunk_cpu_cache.insert(upload.key, instances);
                    needs_repack = true;
                }
            }

            // Uploaded chunk list is authoritative for this generation.
            // Drop any resident chunks that are no longer present.
            let obsolete_keys = self
                .resident_chunks
                .keys()
                .copied()
                .filter(|key| !seen_keys.contains(key))
                .collect::<Vec<_>>();

            if !obsolete_keys.is_empty() {
                for key in obsolete_keys {
                    self.resident_chunks.remove(&key);
                    self.chunk_cpu_cache.remove(&key);
                }
                needs_repack = true;
            }

            if self.evict_stale_chunks() {
                needs_repack = true;
            }

            if needs_repack {
                self.repack_live_chunks(device, queue);
            }
        } else {
            // Chunks are stable: just keep all resident chunks alive so they
            // are not evicted by the stale-frame check.
            for resident in self.resident_chunks.values_mut() {
                resident.last_seen_frame = self.frame_index;
            }
        }

        let resident_count = self.resident_instance_count;
        let max_visible_count = if resident_count == 0 {
            0
        } else {
            cull_settings
                .max_visible_count
                .max(1)
                .min(self.instance_capacity as u32)
                .min(resident_count)
        };

        let keep_prob = if resident_count == 0 || max_visible_count == 0 {
            0.0
        } else {
            ((max_visible_count as f32 / resident_count as f32)
                * cull_settings.density_keep.clamp(0.0, 1.0))
                .clamp(0.0, 1.0)
        };

        let cull_dispatch_groups = if resident_count > 0 && max_visible_count > 0 && keep_prob > 0.0 {
            (resident_count + CULL_WORKGROUP_SIZE - 1) / CULL_WORKGROUP_SIZE
        } else {
            0
        };

        publish_work_snapshot(GpuWorkSnapshot {
            prepared_frames: self.frame_index,
            resident_instances: resident_count,
            max_visible_count,
            cull_dispatch_groups,
            keep_prob,
        });

        let cull_params = CullParams {
            total_star_count: resident_count,
            max_visible_count,
            random_seed: cull_settings.random_seed,
            _padding0: 0,
            keep_prob_pad: [keep_prob, 0.0, 0.0, 0.0],
        };
        queue.write_buffer(&self.cull_params_buffer, 0, bytemuck::bytes_of(&cull_params));
        queue.write_buffer(&self.visible_counter_buffer, 0, bytemuck::cast_slice(&[0u32]));

        if let Some(timestamps) = self.gpu_timestamps.as_ref() {
            timestamps.write_compute_begin(egui_encoder, self.frame_index);
        }

        let mut cull_pass = egui_encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("galaxy_star_cull_pass"),
            timestamp_writes: None,
        });
        cull_pass.set_bind_group(0, &self.compute_bind_group, &[]);

        if cull_dispatch_groups > 0 {
            cull_pass.set_pipeline(&self.cull_pipeline);
            cull_pass.dispatch_workgroups(cull_dispatch_groups, 1, 1);
        }

        cull_pass.set_pipeline(&self.finalize_pipeline);
        cull_pass.dispatch_workgroups(1, 1, 1);
        drop(cull_pass);

        if let Some(timestamps) = self.gpu_timestamps.as_ref() {
            timestamps.write_compute_end(egui_encoder, self.frame_index);
        }
    }

    fn paint<'a>(&'a self, render_pass: &mut wgpu::RenderPass<'a>) {
        if let Some(timestamps) = self.gpu_timestamps.as_ref() {
            timestamps.write_render_begin(render_pass, self.frame_index);
        }
        render_pass.set_pipeline(&self.render_pipeline);
        render_pass.set_bind_group(0, &self.render_bind_group, &[]);
        render_pass.set_vertex_buffer(0, self.quad_buffer.slice(..));
        render_pass.draw_indirect(&self.draw_indirect_buffer, 0);
        if let Some(timestamps) = self.gpu_timestamps.as_ref() {
            timestamps.write_render_end(render_pass, self.frame_index);
        }
    }
}

pub fn initialize(cc: &eframe::CreationContext<'_>) -> bool {
    let Some(render_state) = cc.wgpu_render_state.as_ref() else {
        publish_runtime_snapshot(GpuRuntimeSnapshot {
            renderer_initialized: false,
            adapter_name: "none".to_owned(),
            backend: "none".to_owned(),
            device_type: "none".to_owned(),
            driver: String::new(),
            driver_info: String::new(),
            timestamp_feature_enabled: false,
            timestamp_inside_pass_feature_enabled: false,
            likely_software_adapter: true,
        });
        return false;
    };

    let adapter_info = render_state.adapter.get_info();
    let adapter_name_lower = adapter_info.name.to_ascii_lowercase();
    let likely_software_adapter = matches!(adapter_info.device_type, wgpu::DeviceType::Cpu)
        || adapter_name_lower.contains("llvmpipe")
        || adapter_name_lower.contains("software");

    let enabled_features = render_state.device.features();
    publish_runtime_snapshot(GpuRuntimeSnapshot {
        renderer_initialized: true,
        adapter_name: adapter_info.name,
        backend: format!("{:?}", adapter_info.backend),
        device_type: format!("{:?}", adapter_info.device_type),
        driver: adapter_info.driver,
        driver_info: adapter_info.driver_info,
        timestamp_feature_enabled: enabled_features.contains(wgpu::Features::TIMESTAMP_QUERY),
        timestamp_inside_pass_feature_enabled: enabled_features
            .contains(wgpu::Features::TIMESTAMP_QUERY_INSIDE_PASSES),
        likely_software_adapter,
    });

    let mut renderer = render_state.renderer.write();
    if renderer.callback_resources.get::<StarRenderResources>().is_none() {
        renderer.callback_resources.insert(StarRenderResources::new(
            &render_state.device,
            &render_state.queue,
            render_state.target_format,
        ));
    }

    true
}

pub fn add_chunked_paint_callback(
    painter: &egui::Painter,
    rect: egui::Rect,
    chunks: std::sync::Arc<Vec<ChunkUpload>>,
    uploads_generation: u64,
    input: FrameUniformInput,
) {
    let callback = StarPaintCallback {
        uniform: input.into(),
        cull_settings: input.into(),
        chunks,
        uploads_generation,
    };

    painter.add(egui_wgpu::Callback::new_paint_callback(rect, callback));
}

struct StarPaintCallback {
    uniform: StarUniform,
    cull_settings: CullSettings,
    chunks: std::sync::Arc<Vec<ChunkUpload>>,
    uploads_generation: u64,
}

impl egui_wgpu::CallbackTrait for StarPaintCallback {
    fn prepare(
        &self,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        _screen_descriptor: &egui_wgpu::ScreenDescriptor,
        egui_encoder: &mut wgpu::CommandEncoder,
        callback_resources: &mut egui_wgpu::CallbackResources,
    ) -> Vec<wgpu::CommandBuffer> {
        if let Some(resources) = callback_resources.get_mut::<StarRenderResources>() {
            resources.prepare(
                device,
                queue,
                egui_encoder,
                &self.uniform,
                &self.chunks,
                self.cull_settings,
                self.uploads_generation,
            );
        }
        Vec::new()
    }

    fn paint<'a>(
        &'a self,
        _info: egui::PaintCallbackInfo,
        render_pass: &mut wgpu::RenderPass<'a>,
        callback_resources: &'a egui_wgpu::CallbackResources,
    ) {
        if let Some(resources) = callback_resources.get::<StarRenderResources>() {
            resources.paint(render_pass);
        }
    }
}
