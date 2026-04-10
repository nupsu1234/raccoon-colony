struct StarUniform {
    center3d: vec4<f32>,
    pan_zoom: vec4<f32>,
    yaw_sin_cos: vec4<f32>,
    canvas_size_center: vec4<f32>,
    black_hole: vec4<f32>,
    star_shape: vec4<f32>,
    alpha_range: vec4<f32>,
    star_color: vec4<f32>,
};

@group(0) @binding(0)
var<uniform> uniforms: StarUniform;

struct StarInstance {
    pos_repr: vec4<f32>,
};

@group(0) @binding(1)
var<storage, read> stars: array<StarInstance>;

@group(0) @binding(2)
var<storage, read> visible_indices: array<u32>;

@group(0) @binding(6)
var<storage, read_write> visible_indices_rw: array<u32>;

struct VisibleCounter {
    count: atomic<u32>,
};

@group(0) @binding(3)
var<storage, read_write> visible_counter: VisibleCounter;

@group(0) @binding(4)
var<storage, read_write> indirect_args: array<u32>;

struct CullParams {
    total_star_count: u32,
    max_visible_count: u32,
    random_seed: u32,
    _padding0: u32,
    keep_prob_pad: vec4<f32>,
};

@group(0) @binding(5)
var<uniform> cull_params: CullParams;

struct VsInput {
    @location(0) quad: vec2<f32>,
    @builtin(instance_index) instance_index: u32,
};

struct VsOutput {
    @builtin(position) clip_pos: vec4<f32>,
    @location(0) local_uv: vec2<f32>,
    @location(1) star_center_local: vec2<f32>,
    @location(2) star_alpha: f32,
};

const REPRESENTED_BOOST_LOG2_CAP: f32 = 8.0;
const REPRESENTED_RADIUS_BOOST_SCALE: f32 = 0.07;
const REPRESENTED_ALPHA_BOOST_SCALE: f32 = 0.02;

fn rotated_star_center_local(star_pos: vec3<f32>) -> vec2<f32> {
    let sy = uniforms.yaw_sin_cos.x;
    let cy = uniforms.yaw_sin_cos.y;
    let sp = uniforms.yaw_sin_cos.z;
    let cp = uniforms.yaw_sin_cos.w;

    var x = star_pos.x - uniforms.center3d.x;
    var y = star_pos.y - uniforms.center3d.y;
    var z = star_pos.z - uniforms.center3d.z;

    let xz = cy * x - sy * z;
    let zz = sy * x + cy * z;
    x = xz;
    z = zz;

    let yz = cp * y - sp * z;
    y = yz;

    return vec2<f32>(
        uniforms.canvas_size_center.z + (x + uniforms.pan_zoom.x) * uniforms.pan_zoom.z,
        uniforms.canvas_size_center.w + (y + uniforms.pan_zoom.y) * uniforms.pan_zoom.z,
    );
}

fn star_visuals(represented_systems: f32) -> vec2<f32> {
    let represented = max(represented_systems, 1.0);
    let chunk_boost = clamp(log2(represented), 0.0, REPRESENTED_BOOST_LOG2_CAP);
    let point_radius = clamp(
        uniforms.star_shape.x * (1.0 + chunk_boost * REPRESENTED_RADIUS_BOOST_SCALE),
        uniforms.star_shape.x,
        uniforms.star_shape.y,
    );
    let point_alpha = clamp(
        uniforms.star_shape.z * (1.0 + chunk_boost * REPRESENTED_ALPHA_BOOST_SCALE),
        uniforms.alpha_range.x,
        uniforms.alpha_range.y,
    ) / 255.0;
    return vec2<f32>(point_radius, point_alpha);
}

fn hash_u32(x: u32) -> u32 {
    var v = x;
    v = v ^ (v >> 16u);
    v = v * 0x7FEB352Du;
    v = v ^ (v >> 15u);
    v = v * 0x846CA68Bu;
    v = v ^ (v >> 16u);
    return v;
}

fn keep_by_density(star_index: u32) -> bool {
    let keep_prob = cull_params.keep_prob_pad.x;
    if keep_prob >= 0.9999 {
        return true;
    }
    if keep_prob <= 0.0 {
        return false;
    }
    let threshold = u32(keep_prob * 4294967295.0);
    let h = hash_u32(star_index ^ cull_params.random_seed);
    return h <= threshold;
}

@compute @workgroup_size(256)
fn cs_cull(@builtin(global_invocation_id) gid: vec3<u32>) {
    let star_index = gid.x;
    if star_index >= cull_params.total_star_count {
        return;
    }

    let star = stars[star_index];
    let star_pos = star.pos_repr.xyz;
    let represented = max(star.pos_repr.w, 1.0);

    let visuals = star_visuals(represented);
    let point_radius = visuals.x;
    let center_local = rotated_star_center_local(star_pos);
    let canvas_size = uniforms.canvas_size_center.xy;

    if center_local.x < -point_radius || center_local.x > canvas_size.x + point_radius {
        return;
    }
    if center_local.y < -point_radius || center_local.y > canvas_size.y + point_radius {
        return;
    }
    let black_hole_delta = center_local - uniforms.black_hole.xy;
    let black_hole_radius_sq = uniforms.black_hole.z * uniforms.black_hole.z;
    if dot(black_hole_delta, black_hole_delta) <= black_hole_radius_sq {
        return;
    }
    if !keep_by_density(star_index) {
        return;
    }

    let dst = atomicAdd(&visible_counter.count, 1u);
    if dst < cull_params.max_visible_count {
        visible_indices_rw[dst] = star_index;
    }
}

@compute @workgroup_size(1)
fn cs_finalize() {
    let count = min(atomicLoad(&visible_counter.count), cull_params.max_visible_count);
    indirect_args[0] = 6u;
    indirect_args[1] = count;
    indirect_args[2] = 0u;
    indirect_args[3] = 0u;
}

@vertex
fn vs_main(input: VsInput) -> VsOutput {
    var out: VsOutput;
    let star_index = visible_indices[input.instance_index];
    let star = stars[star_index];
    let star_pos = star.pos_repr.xyz;

    let visuals = star_visuals(star.pos_repr.w);
    let point_radius = visuals.x;
    let star_alpha = visuals.y;
    let center_local = rotated_star_center_local(star_pos);

    let point_local = center_local + input.quad * point_radius;
    let canvas_size = uniforms.canvas_size_center.xy;

    let ndc = vec2<f32>(
        point_local.x / canvas_size.x * 2.0 - 1.0,
        1.0 - point_local.y / canvas_size.y * 2.0,
    );

    out.clip_pos = vec4<f32>(ndc, 0.0, 1.0);
    out.local_uv = input.quad;
    out.star_center_local = center_local;
    out.star_alpha = star_alpha;
    return out;
}

@fragment
fn fs_main(input: VsOutput) -> @location(0) vec4<f32> {
    if dot(input.local_uv, input.local_uv) > 1.0 {
        discard;
    }

    let black_hole_delta = input.star_center_local - uniforms.black_hole.xy;
    let black_hole_radius_sq = uniforms.black_hole.z * uniforms.black_hole.z;
    if dot(black_hole_delta, black_hole_delta) <= black_hole_radius_sq {
        discard;
    }

    let radial = clamp(1.0 - dot(input.local_uv, input.local_uv), 0.0, 1.0);
    let alpha = input.star_alpha * (0.55 + 0.45 * radial);

    return vec4<f32>(uniforms.star_color.rgb, alpha);
}
